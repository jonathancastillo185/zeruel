//! Consultas de la pestaña Corridas.
//!
//! # Por qué no alcanzaba con lo que había
//!
//! La pestaña pedía **las 100 más recientes** y filtraba en JavaScript sobre
//! esas 100. Medido contra la instalación real, con 5.013 corridas guardadas:
//!
//! ```text
//!   Failed       0 visibles de   51 reales
//!   Cancelled    0 visibles de    6 reales
//!   Success     94 visibles de 4950 reales
//!   las 100 cubren 1,5 horas de los 5,3 dias que hay en la base
//! ```
//!
//! O sea que **filtrar por "fallo" devolvía vacío teniendo 51 fallos
//! guardados**, porque en la última hora y media no hubo ninguno. No es una
//! tabla incompleta: es una que contesta "no hay fallos" cuando los hay, que es
//! justo la pregunta que se le hace.
//!
//! Es el mismo error que este repo ya cometió dos veces —N-36, y el bug A de
//! `docs/PLAN-UI.md`— y por eso acá todo filtro baja al `WHERE`, la cuenta
//! total se pide aparte, y la paginación es de verdad.

use crate::store::Store;
use anyhow::Result;
use rusqlite::params_from_iter;
use serde::Serialize;

/// Qué corridas se piden.
#[derive(Clone, Debug, Default)]
pub struct Filtro {
    /// Texto libre: se busca en el nombre del DAG y en el parámetro.
    pub texto: Option<String>,
    pub dag: Option<String>,
    /// `Success`, `Failed`, `Running`, `Cancelled`.
    pub estado: Option<String>,
    pub desde: Option<i64>,
    pub hasta: Option<i64>,
    /// Sólo las que duraron más que esto (segundos).
    pub dur_min_secs: Option<i64>,
    /// Sólo las que pasaron de este pico de RAM (MB).
    pub ram_min_mb: Option<i64>,
    /// Sólo las que necesitaron más de un intento en alguna tarea.
    pub con_reintentos: bool,
    pub limite: usize,
    pub offset: usize,
    /// Columna por la que ordenar y dirección. Se valida contra una lista
    /// blanca: viene de la query string y va a parar a un `ORDER BY`.
    pub orden: Option<String>,
    pub desc: bool,
}

/// Una corrida, con lo que hace falta para decidir si vale la pena abrirla.
#[derive(Clone, Debug, Serialize)]
pub struct CorridaRow {
    pub id: String,
    pub dag: String,
    pub param: String,
    pub state: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub dur_secs: Option<i64>,
    /// Pico de RAM del run: el máximo de sus tareas.
    pub ram_peak_mb: i64,
    pub cpu_secs: f64,
    pub tareas: i64,
    pub tareas_ok: i64,
    pub tareas_fail: i64,
    /// Cuántas tareas necesitaron más de un intento.
    pub reintentadas: i64,
    /// Mediana de duración de ese DAG en el período reciente, para poder decir
    /// "ésta tardó el triple de lo normal" sin abrir nada.
    pub p50_dag_secs: Option<f64>,
}

/// El resultado, con el total para poder paginar de verdad.
#[derive(Clone, Debug, Serialize)]
pub struct Pagina {
    pub filas: Vec<CorridaRow>,
    /// Cuántas cumplen el filtro **en total**, no cuántas se devolvieron.
    pub total: i64,
    pub offset: usize,
    pub limite: usize,
}

/// Columnas por las que se puede ordenar.
///
/// Es una lista blanca a propósito: `orden` llega por la query string y se
/// interpola en un `ORDER BY`, donde no se pueden usar parámetros. Comparar
/// contra esta lista es lo que hace que no se pueda inyectar SQL por ahí.
const ORDENABLES: &[(&str, &str)] = &[
    ("started_at", "r.started_at"),
    ("dag", "r.dag"),
    ("param", "r.param"),
    ("state", "r.state"),
    ("dur_secs", "dur"),
    ("ram_peak_mb", "ram"),
    ("cpu_secs", "cpu"),
];

impl Filtro {
    fn where_y_args(&self) -> (String, Vec<Box<dyn rusqlite::ToSql>>) {
        let mut cond: Vec<String> = vec!["1=1".into()];
        let mut par: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(t) = self.texto.as_deref().filter(|t| !t.is_empty()) {
            // Se busca en los dos campos que se ven en la fila. `LIKE` con
            // comodines a los dos lados: la gente escribe un pedazo del nombre.
            cond.push("(r.dag LIKE ?  OR r.param LIKE ?)".into());
            let p = format!("%{t}%");
            par.push(Box::new(p.clone()));
            par.push(Box::new(p));
        }
        if let Some(d) = self.dag.as_deref().filter(|d| !d.is_empty()) {
            cond.push("r.dag = ?".into());
            par.push(Box::new(d.to_string()));
        }
        if let Some(e) = self.estado.as_deref().filter(|e| !e.is_empty()) {
            cond.push("r.state = ?".into());
            par.push(Box::new(e.to_string()));
        }
        if let Some(v) = self.desde {
            cond.push("r.started_at >= ?".into());
            par.push(Box::new(v));
        }
        if let Some(v) = self.hasta {
            cond.push("r.started_at < ?".into());
            par.push(Box::new(v));
        }
        if let Some(v) = self.dur_min_secs {
            // Una que sigue corriendo todavía no tiene duración: no puede
            // cumplir ni incumplir el filtro.
            cond.push("(r.finished_at IS NOT NULL AND r.finished_at - r.started_at >= ?)".into());
            par.push(Box::new(v));
        }
        if let Some(v) = self.ram_min_mb {
            cond.push(
                "COALESCE((SELECT MAX(t.ram_peak_mb) FROM task_runs t
                           WHERE t.run_id = r.id AND t.reused = 0), 0) >= ?"
                    .into(),
            );
            par.push(Box::new(v));
        }
        if self.con_reintentos {
            cond.push(
                "EXISTS (SELECT 1 FROM task_runs t
                         WHERE t.run_id = r.id AND t.reused = 0 AND t.intentos > 1)"
                    .into(),
            );
        }
        (cond.join(" AND "), par)
    }

    fn order_by(&self) -> String {
        let col = self
            .orden
            .as_deref()
            .and_then(|o| ORDENABLES.iter().find(|(k, _)| *k == o))
            .map(|(_, sql)| *sql)
            .unwrap_or("r.started_at");
        let dir = if self.desc { "DESC" } else { "ASC" };
        // El id como desempate: sin un orden total, dos filas con el mismo
        // valor pueden salir en distinto orden en cada consulta y la paginación
        // repetiría o saltearía filas.
        format!("{col} {dir} NULLS LAST, r.id ASC")
    }
}

impl Store {
    /// Una página de corridas que cumplen el filtro, con el total.
    pub fn corridas(&self, f: &Filtro) -> Result<Pagina> {
        let c = self.conn.lock().unwrap();
        let (w, par) = f.where_y_args();

        let total: i64 = {
            let sql = format!("SELECT COUNT(*) FROM runs r WHERE {w}");
            let mut stmt = c.prepare(&sql)?;
            stmt.query_row(params_from_iter(par.iter()), |r| r.get(0))?
        };

        let limite = f.limite.clamp(1, 500);
        let sql = format!(
            "WITH base AS (
                 SELECT r.id, r.dag, r.param, r.state, r.started_at, r.finished_at,
                        CASE WHEN r.finished_at IS NOT NULL
                             THEN r.finished_at - r.started_at END AS dur,
                        COALESCE((SELECT MAX(t.ram_peak_mb) FROM task_runs t
                                  WHERE t.run_id = r.id AND t.reused = 0), 0) AS ram,
                        COALESCE((SELECT SUM(t.cpu_secs) FROM task_runs t
                                  WHERE t.run_id = r.id AND t.reused = 0), 0) AS cpu
                 FROM runs r WHERE {w}
                 ORDER BY {ord}
                 LIMIT ? OFFSET ?
             )
             SELECT b.*,
                    (SELECT COUNT(*) FROM task_runs t WHERE t.run_id = b.id),
                    (SELECT COUNT(*) FROM task_runs t
                     WHERE t.run_id = b.id AND t.state = 'Success'),
                    (SELECT COUNT(*) FROM task_runs t
                     WHERE t.run_id = b.id AND t.state = 'Failed'),
                    (SELECT COUNT(*) FROM task_runs t
                     WHERE t.run_id = b.id AND t.reused = 0 AND t.intentos > 1)
             FROM base b",
            w = w,
            ord = f.order_by()
        );
        let mut par2 = par;
        par2.push(Box::new(limite as i64));
        par2.push(Box::new(f.offset as i64));

        let mut stmt = c.prepare(&sql)?;
        let filas: Vec<CorridaRow> = stmt
            .query_map(params_from_iter(par2.iter()), |r| {
                Ok(CorridaRow {
                    id: r.get(0)?,
                    dag: r.get(1)?,
                    param: r.get(2)?,
                    state: r.get(3)?,
                    started_at: r.get(4)?,
                    finished_at: r.get(5)?,
                    dur_secs: r.get(6)?,
                    ram_peak_mb: r.get(7)?,
                    cpu_secs: r.get(8)?,
                    tareas: r.get(9)?,
                    tareas_ok: r.get(10)?,
                    tareas_fail: r.get(11)?,
                    reintentadas: r.get(12)?,
                    p50_dag_secs: None,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);

        // La mediana del DAG se calcula UNA vez por DAG distinto de la página,
        // no una por fila: con 100 filas de 3 DAGs son 3 consultas y no 100.
        let mut filas = filas;
        let mut medianas: std::collections::HashMap<String, Option<f64>> =
            std::collections::HashMap::new();
        for f in &filas {
            if !medianas.contains_key(&f.dag) {
                let m = Self::p50_de(&c, &f.dag).ok().flatten();
                medianas.insert(f.dag.clone(), m);
            }
        }
        for f in &mut filas {
            f.p50_dag_secs = medianas.get(&f.dag).copied().flatten();
        }

        Ok(Pagina {
            filas,
            total,
            offset: f.offset,
            limite,
        })
    }

    /// Mediana de duración de las corridas exitosas de un DAG.
    ///
    /// Es la referencia contra la que se compara una corrida: el promedio no
    /// sirve porque un scraper que normalmente tarda 30 s y a veces 40 minutos
    /// tiene un promedio que no describe ninguno de los dos casos.
    fn p50_de(c: &rusqlite::Connection, dag: &str) -> Result<Option<f64>> {
        let v: Option<f64> = c
            .query_row(
                "SELECT MAX(dur) FROM (
                     SELECT finished_at - started_at AS dur,
                            PERCENT_RANK() OVER (ORDER BY finished_at - started_at) AS pr
                     FROM runs
                     WHERE dag = ?1 AND state = 'Success' AND finished_at IS NOT NULL
                 ) WHERE pr <= 0.5",
                [dag],
                |r| r.get(0),
            )
            .unwrap_or(None);
        Ok(v)
    }

    /// Los DAGs que aparecen en el historial, para el buscador del filtro.
    pub fn dags_con_corridas(&self) -> Result<Vec<String>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare("SELECT DISTINCT dag FROM runs ORDER BY dag")?;
        let v = stmt
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{RunState, Store, TaskStateRecord};
    use zeruel_sdk::TaskState;

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    fn base() -> Filtro {
        Filtro {
            limite: 50,
            desc: true,
            ..Default::default()
        }
    }

    /// **El bug que motivo el modulo.** Con mas corridas que el limite de la
    /// pagina, filtrar por estado tiene que mirar TODA la historia y no solo la
    /// primera pagina. Medido en produccion: la pestaña decia "no hay fallos"
    /// con 51 fallos guardados.
    #[test]
    fn filtrar_por_estado_mira_toda_la_historia_no_la_primera_pagina() {
        let s = store();
        // Un fallo viejo, y despues 150 exitos que lo tapan.
        let viejo = s.create_run("d", "el-fallo").unwrap();
        s.set_run_state(viejo, RunState::Failed).unwrap();
        for i in 0..150 {
            let r = s.create_run("d", &format!("ok-{i}")).unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }

        let f = Filtro {
            estado: Some("Failed".into()),
            ..base()
        };
        let p = s.corridas(&f).unwrap();
        assert_eq!(p.total, 1, "el fallo tiene que encontrarse");
        assert_eq!(p.filas.len(), 1);
        assert_eq!(p.filas[0].param, "el-fallo");
    }

    /// El total es el de TODAS las que cumplen, no el de la pagina: sin eso no
    /// se puede saber si hay una pagina siguiente.
    #[test]
    fn el_total_es_del_filtro_y_no_de_la_pagina() {
        let s = store();
        for i in 0..30 {
            let r = s.create_run("d", &format!("p{i}")).unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        let f = Filtro {
            limite: 10,
            ..base()
        };
        let p = s.corridas(&f).unwrap();
        assert_eq!(p.filas.len(), 10, "devuelve una pagina");
        assert_eq!(p.total, 30, "pero informa el total");
    }

    /// Paginar no puede repetir ni saltear filas. Con corridas creadas en el
    /// mismo segundo, ordenar solo por fecha deja el orden indefinido: por eso
    /// el `ORDER BY` lleva el id como desempate.
    #[test]
    fn paginar_no_repite_ni_saltea() {
        let s = store();
        for i in 0..25 {
            let r = s.create_run("d", &format!("p{i}")).unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        let mut vistos = std::collections::HashSet::new();
        for pag in 0..3 {
            let f = Filtro {
                limite: 10,
                offset: pag * 10,
                ..base()
            };
            for fila in s.corridas(&f).unwrap().filas {
                assert!(vistos.insert(fila.id.clone()), "id repetido entre paginas");
            }
        }
        assert_eq!(vistos.len(), 25, "no se saltea ninguna");
    }

    /// El orden viene de la query string y se interpola en el `ORDER BY`, donde
    /// no se pueden usar parametros. Una columna que no este en la lista blanca
    /// tiene que ignorarse, no ejecutarse.
    #[test]
    fn una_columna_de_orden_inventada_no_se_ejecuta() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        s.set_run_state(r, RunState::Success).unwrap();

        let f = Filtro {
            orden: Some("r.id; DROP TABLE runs; --".into()),
            ..base()
        };
        let p = s
            .corridas(&f)
            .expect("no puede fallar: cae al orden por defecto");
        assert_eq!(p.total, 1);
        // Y la tabla sigue estando.
        assert_eq!(s.corridas(&base()).unwrap().total, 1);
    }

    /// La fila trae lo que hace falta para decidir si vale la pena abrirla.
    #[test]
    fn la_fila_trae_ram_tareas_y_reintentos() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        s.set_task_state(TaskStateRecord::con_intentos(
            r,
            "pesada",
            &TaskState::Success,
            0,
            "ok",
            512,
            2.5,
            3,
        ))
        .unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r,
            "liviana",
            &TaskState::Success,
            0,
            "ok",
            10,
            0.5,
        ))
        .unwrap();
        s.set_run_state(r, RunState::Success).unwrap();

        let p = s.corridas(&base()).unwrap();
        let f = &p.filas[0];
        assert_eq!(
            f.ram_peak_mb, 512,
            "el pico del run es el maximo de sus tareas"
        );
        assert_eq!(f.tareas, 2);
        assert_eq!(f.tareas_ok, 2);
        assert_eq!(f.reintentadas, 1, "una necesito mas de un intento");
        assert!(f.cpu_secs >= 3.0, "el CPU se suma: {}", f.cpu_secs);
    }

    /// El filtro de RAM, que no existe en ninguna otra pestaña a nivel corrida.
    #[test]
    fn se_puede_buscar_la_corrida_que_se_comio_la_memoria() {
        let s = store();
        for (param, mb) in [("liviana", 20u64), ("pesada", 900)] {
            let r = s.create_run("d", param).unwrap();
            s.set_task_state(TaskStateRecord::medido(
                r,
                "t",
                &TaskState::Success,
                0,
                "ok",
                mb,
                0.0,
            ))
            .unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        let f = Filtro {
            ram_min_mb: Some(500),
            ..base()
        };
        let p = s.corridas(&f).unwrap();
        assert_eq!(p.total, 1);
        assert_eq!(p.filas[0].param, "pesada");
    }

    /// La busqueda de texto mira el DAG y el parametro, que son las dos cosas
    /// que se ven en la fila.
    #[test]
    fn el_texto_busca_en_el_dag_y_en_el_parametro() {
        let s = store();
        s.set_run_state(
            s.create_run("scraping_norte", "2026-01-01").unwrap(),
            RunState::Success,
        )
        .unwrap();
        s.set_run_state(
            s.create_run("comparador", "norte-especial").unwrap(),
            RunState::Success,
        )
        .unwrap();

        let f = Filtro {
            texto: Some("norte".into()),
            ..base()
        };
        assert_eq!(
            s.corridas(&f).unwrap().total,
            2,
            "encuentra por los dos lados"
        );
    }

    /// La mediana del DAG viaja en la fila para poder decir "esta tardo el
    /// triple de lo normal" sin abrir nada ni consultar otra pantalla.
    #[test]
    fn la_fila_trae_la_mediana_de_su_dag_para_comparar() {
        let s = store();
        let ahora = chrono::Utc::now().timestamp();
        for (i, dur) in [10i64, 12, 11, 300].iter().enumerate() {
            let r = s.create_run("d", &format!("p{i}")).unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
            s.conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE runs SET started_at = ?2, finished_at = ?3 WHERE id = ?1",
                    rusqlite::params![
                        r.to_string(),
                        ahora - 1000 + i as i64,
                        ahora - 1000 + i as i64 + dur
                    ],
                )
                .unwrap();
        }
        let p = s.corridas(&base()).unwrap();
        let m = p.filas[0].p50_dag_secs.expect("tiene que traer mediana");
        assert!(
            (10.0..=20.0).contains(&m),
            "la mediana no puede arrastrarla el caso de 300s: dio {m}"
        );
    }
}

/// Las ultimas corridas de un DAG, de la mas vieja a la mas nueva.
#[derive(Clone, Debug, Serialize, Default)]
pub struct Historial {
    /// Estados, en orden cronologico. El ultimo es el mas reciente.
    pub ultimas: Vec<String>,
    /// Estado de la corrida mas reciente.
    pub ultimo_estado: Option<String>,
    /// Cuantas corridas seguidas vienen fallando, contando desde la ultima
    /// hacia atras. 0 si la ultima salio bien.
    pub racha_fallos: i64,
}

/// Cuantas corridas entran en la tira del panel.
///
/// Doce es lo que se lee de un vistazo en una celda de tabla sin que las
/// marcas queden demasiado finas, y alcanza para distinguir "roto desde
/// siempre" de "se rompio recien".
pub const LARGO_TIRA: usize = 12;

impl Store {
    /// Las ultimas corridas de **cada** DAG, en una sola consulta.
    ///
    /// # Por que hace falta
    ///
    /// El panel mostraba contadores acumulados —"3 fallos de 52"— y eso no
    /// contesta la pregunta que se le hace: **¿esta roto AHORA?**. Medido
    /// contra la instalacion real, `scraping_sportline` mostraba 3 de 52, que
    /// se lee como sano, y su ultima corrida habia fallado media hora antes.
    /// Habia 7 DAGs asi y el panel no los distinguia de los que andaban bien.
    ///
    /// # Por que la tira y no solo el ultimo estado
    ///
    /// Un solo estado no separa tres situaciones que se atienden distinto: un
    /// DAG roto de forma sostenida, uno intermitente, y uno que acaba de
    /// recuperarse. La secuencia si.
    ///
    /// Va en **una** consulta con `ROW_NUMBER()` y no una por DAG: con 56 DAGs
    /// serian 56 consultas en cada refresco del panel, que corre cada pocos
    /// segundos.
    pub fn historial_por_dag(&self) -> Result<std::collections::HashMap<String, Historial>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT dag, state FROM (
                 SELECT dag, state, started_at,
                        ROW_NUMBER() OVER (PARTITION BY dag ORDER BY started_at DESC) AS n
                 FROM runs
             ) WHERE n <= ?1
             ORDER BY dag, started_at ASC",
        )?;
        let filas = stmt
            .query_map([LARGO_TIRA as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut out: std::collections::HashMap<String, Historial> =
            std::collections::HashMap::new();
        for (dag, estado) in filas {
            out.entry(dag).or_default().ultimas.push(estado);
        }
        for h in out.values_mut() {
            h.ultimo_estado = h.ultimas.last().cloned();
            // La racha se cuenta desde la ultima hacia atras y se corta en la
            // primera que no fallo. Una corrida en curso NO corta la racha ni
            // la suma: todavia no se sabe como va a terminar.
            h.racha_fallos = h
                .ultimas
                .iter()
                .rev()
                .skip_while(|e| *e == "Running")
                .take_while(|e| *e == "Failed")
                .count() as i64;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests_historial {
    use super::*;
    use crate::store::{RunState, Store};

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    fn correr(s: &Store, dag: &str, estados: &[RunState]) {
        for (i, e) in estados.iter().enumerate() {
            let r = s.create_run(dag, &format!("p{i}")).unwrap();
            s.set_run_state(r, *e).unwrap();
            // Separar en el tiempo: el orden de la tira sale de `started_at`, y
            // con todas en el mismo segundo el orden quedaria indefinido.
            s.conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE runs SET started_at = started_at + ?2 WHERE id = ?1",
                    rusqlite::params![r.to_string(), i as i64],
                )
                .unwrap();
        }
    }

    /// **El caso que motivo esto.** Un DAG con muchas corridas buenas y pocos
    /// fallos se ve sano en el contador, aunque la ULTIMA haya fallado. Medido
    /// en produccion: `scraping_sportline` mostraba "3 fallos de 52" con su
    /// ultima corrida fallada media hora antes.
    #[test]
    fn el_contador_dice_sano_pero_la_ultima_fallo() {
        let s = store();
        let mut estados = vec![RunState::Success; 50];
        estados.push(RunState::Failed);
        correr(&s, "sportline", &estados);

        let h = s.historial_por_dag().unwrap();
        let x = &h["sportline"];
        assert_eq!(
            x.ultimo_estado.as_deref(),
            Some("Failed"),
            "lo que el contador tapaba"
        );
        assert_eq!(x.racha_fallos, 1);
    }

    /// La tira distingue tres situaciones que se atienden distinto y que un
    /// solo estado confunde.
    #[test]
    fn la_tira_separa_roto_de_intermitente_de_recuperado() {
        use RunState::{Failed as F, Success as S};
        let s = store();
        correr(&s, "roto", &[S, F, F, F, F]);
        correr(&s, "intermitente", &[S, F, S, F, S]);
        correr(&s, "recuperado", &[F, F, F, S, S]);

        let h = s.historial_por_dag().unwrap();
        assert_eq!(h["roto"].racha_fallos, 4, "viene fallando seguido");
        assert_eq!(h["intermitente"].racha_fallos, 0, "la ultima salio bien");
        assert_eq!(h["recuperado"].racha_fallos, 0);
        // Y la secuencia esta, que es lo que los diferencia a simple vista.
        assert_eq!(
            h["intermitente"].ultimas,
            vec!["Success", "Failed", "Success", "Failed", "Success"]
        );
    }

    /// El orden es cronologico: la ultima corrida va al final de la tira. Al
    /// reves, la tira se leeria hacia atras y la marca mas a la derecha —donde
    /// la vista va a buscar "lo de ahora"— seria la mas vieja.
    #[test]
    fn la_tira_va_de_la_mas_vieja_a_la_mas_nueva() {
        let s = store();
        correr(&s, "d", &[RunState::Failed, RunState::Success]);
        let h = s.historial_por_dag().unwrap();
        assert_eq!(h["d"].ultimas, vec!["Failed", "Success"]);
        assert_eq!(h["d"].ultimo_estado.as_deref(), Some("Success"));
    }

    /// Una corrida en curso no corta la racha ni la suma: todavia no se sabe
    /// como va a terminar. Decir que "se recupero" porque hay una corriendo
    /// seria adelantarse.
    #[test]
    fn una_corrida_en_curso_no_decide_la_racha() {
        let s = store();
        correr(&s, "d", &[RunState::Failed, RunState::Failed]);
        let r = s.create_run("d", "ahora").unwrap(); // queda Running
        let _ = r;
        s.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE runs SET started_at = started_at + 99 WHERE param='ahora'",
                [],
            )
            .unwrap();

        let h = s.historial_por_dag().unwrap();
        assert_eq!(h["d"].ultimo_estado.as_deref(), Some("Running"));
        assert_eq!(
            h["d"].racha_fallos, 2,
            "los dos fallos previos siguen contando"
        );
    }

    /// Solo las ultimas N: la tira tiene que entrar en una celda.
    #[test]
    fn la_tira_se_corta_en_el_largo_declarado() {
        let s = store();
        correr(&s, "d", &[RunState::Success; LARGO_TIRA + 20]);
        let h = s.historial_por_dag().unwrap();
        assert_eq!(h["d"].ultimas.len(), LARGO_TIRA);
    }

    /// Un DAG sin corridas no aparece: el panel lo muestra igual, con sus
    /// contadores en cero, y una tira vacia no diria nada.
    #[test]
    fn un_dag_sin_corridas_no_tiene_historial() {
        let s = store();
        correr(&s, "d", &[RunState::Success]);
        let h = s.historial_por_dag().unwrap();
        assert!(!h.contains_key("otro"));
    }
}

/// Un mismo error repitiendose en varias corridas.
#[derive(Clone, Debug, Serialize)]
pub struct FalloSistemico {
    /// El error, recortado. Es lo que agrupa.
    pub error: String,
    /// Cuantas tareas fallaron asi.
    pub veces: i64,
    /// En cuantos DAGs distintos. Es lo que separa "un pipeline roto" de
    /// "algo del entorno se rompio".
    pub dags: i64,
    /// Algunos nombres, para poder ir a mirar.
    pub ejemplos: Vec<String>,
    pub desde: i64,
    pub hasta: i64,
}

impl Store {
    /// Errores que se estan repitiendo, agrupados por mensaje.
    ///
    /// # Por que
    ///
    /// Cuando algo del entorno se rompe —un interprete que no esta, un
    /// directorio que no existe, un permiso que falta— **todos** los pipelines
    /// que lo usan fallan con el mismo error. Reportados de a uno son 51 fallos
    /// sueltos y una sensacion de que "todo esta roto"; agrupados son **un**
    /// problema con un mensaje concreto.
    ///
    /// Paso exactamente asi: 51 tareas de 12 DAGs distintos fallaron con
    /// "No Python at '...'". Se veian como 51 fallos independientes y se
    /// perdieron horas buscando que tenian en comun. El dato estaba en la base
    /// desde el primer minuto.
    ///
    /// El corte por DAGs distintos es lo que importa: un error repetido en UN
    /// solo DAG es un pipeline con un bug, y eso ya se ve en su tira de
    /// corridas. Repetido en VARIOS es el entorno, y eso no lo muestra nada.
    ///
    /// Los mensajes se agrupan por sus primeros `LARGO_CLAVE` caracteres: el
    /// final suele traer rutas o tiempos que cambian entre corridas y romperian
    /// el agrupamiento.
    pub fn fallos_sistemicos(&self, desde: i64, min_dags: i64) -> Result<Vec<FalloSistemico>> {
        /// Cuanto del mensaje se usa para agrupar.
        const LARGO_CLAVE: usize = 80;

        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT substr(t.message, 1, ?2) AS clave,
                    COUNT(*),
                    COUNT(DISTINCT r.dag),
                    MIN(r.started_at),
                    MAX(r.started_at)
             FROM task_runs t JOIN runs r ON r.id = t.run_id
             WHERE r.started_at >= ?1 AND t.state = 'Failed'
               AND t.message IS NOT NULL AND t.message != ''
             GROUP BY clave
             HAVING COUNT(DISTINCT r.dag) >= ?3
             ORDER BY COUNT(*) DESC",
        )?;
        let filas = stmt
            .query_map(
                rusqlite::params![desde, LARGO_CLAVE as i64, min_dags],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);

        let mut out = Vec::new();
        for (clave, veces, dags, desde_t, hasta_t) in filas {
            let mut ej = c.prepare(
                "SELECT DISTINCT r.dag FROM task_runs t JOIN runs r ON r.id = t.run_id
                 WHERE r.started_at >= ?1 AND t.state = 'Failed'
                   AND substr(t.message, 1, ?2) = ?3
                 LIMIT 4",
            )?;
            let ejemplos = ej
                .query_map(rusqlite::params![desde, LARGO_CLAVE as i64, &clave], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            out.push(FalloSistemico {
                error: clave.trim().to_string(),
                veces,
                dags,
                ejemplos,
                desde: desde_t,
                hasta: hasta_t,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests_sistemicos {
    use crate::store::{RunState, Store, TaskStateRecord};
    use zeruel_sdk::TaskState;

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    fn fallar(s: &Store, dag: &str, msg: &str) {
        let r = s.create_run(dag, "p").unwrap();
        let st = TaskState::Failed { retries_left: 0 };
        s.set_task_state(TaskStateRecord::medido(r, "t", &st, 0, msg, 0, 0.0))
            .unwrap();
        s.set_run_state(r, RunState::Failed).unwrap();
    }

    fn ahora() -> i64 {
        chrono::Utc::now().timestamp() - 3600
    }

    /// **El caso real.** Cuando algo del entorno se rompe, todos los pipelines
    /// que lo usan fallan con el mismo error. Reportados de a uno son 51 fallos
    /// sueltos; agrupados son UN problema con un mensaje concreto.
    #[test]
    fn un_error_repetido_en_varios_dags_se_reporta_como_uno_solo() {
        let s = store();
        for dag in ["categorias", "comparador", "scraping_bari", "financiacion"] {
            fallar(&s, dag, "No Python at 'C:/.../Python312/python.exe'");
        }
        let v = s.fallos_sistemicos(ahora(), 3).unwrap();
        assert_eq!(v.len(), 1, "es UN problema, no cuatro: {v:?}");
        assert_eq!(v[0].veces, 4);
        assert_eq!(v[0].dags, 4);
        assert!(
            v[0].error.contains("No Python"),
            "y dice cual: {}",
            v[0].error
        );
        assert!(
            !v[0].ejemplos.is_empty(),
            "con DAGs de ejemplo para ir a mirar"
        );
    }

    /// Un error repetido en UN solo DAG no es sistemico: es un pipeline con un
    /// bug, y eso ya se ve en su tira de corridas. Reportarlo aca ahogaria la
    /// señal que si importa.
    #[test]
    fn un_error_de_un_solo_dag_no_es_sistemico() {
        let s = store();
        for _ in 0..10 {
            fallar(&s, "un_dag_roto", "mi bug de siempre");
        }
        assert!(
            s.fallos_sistemicos(ahora(), 3).unwrap().is_empty(),
            "diez fallos del mismo DAG son un pipeline roto, no el entorno"
        );
    }

    /// Errores distintos no se mezclan aunque pasen a la vez.
    #[test]
    fn errores_distintos_no_se_agrupan() {
        let s = store();
        for dag in ["a", "b", "c"] {
            fallar(&s, dag, "no encuentro python");
        }
        for dag in ["d", "e", "f"] {
            fallar(&s, dag, "se acabo el disco");
        }
        let v = s.fallos_sistemicos(ahora(), 3).unwrap();
        assert_eq!(v.len(), 2, "dos problemas distintos: {v:?}");
        // El mas frecuente primero: es por donde hay que empezar.
        assert!(v[0].veces >= v[1].veces);
    }

    /// Lo viejo no cuenta: el corte por fecha es lo que hace que un problema ya
    /// resuelto deje de aparecer solo.
    #[test]
    fn lo_de_antes_del_corte_no_aparece() {
        let s = store();
        for dag in ["a", "b", "c"] {
            fallar(&s, dag, "un error viejo");
        }
        s.conn
            .lock()
            .unwrap()
            .execute("UPDATE runs SET started_at = started_at - 86400", [])
            .unwrap();
        assert!(s.fallos_sistemicos(ahora(), 3).unwrap().is_empty());
    }

    /// Las tareas que salieron bien no entran, por mas que su mensaje se repita
    /// en todos los DAGs (que es justamente lo normal).
    #[test]
    fn los_exitos_no_cuentan_por_mas_que_se_repita_el_mensaje() {
        let s = store();
        for dag in ["a", "b", "c", "d"] {
            let r = s.create_run(dag, "p").unwrap();
            s.set_task_state(TaskStateRecord::medido(
                r,
                "t",
                &TaskState::Success,
                0,
                "python.exe termino en 1.2s",
                0,
                0.0,
            ))
            .unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        assert!(s.fallos_sistemicos(ahora(), 3).unwrap().is_empty());
    }
}

/// Diagnostico de una cola que crece mas rapido de lo que drena.
#[derive(Clone, Debug, Serialize)]
pub struct ColaAtrasada {
    /// Cuantos items esperan.
    pub en_cola: i64,
    /// Cuanto espera el mas viejo, en segundos.
    pub espera_max_seg: i64,
    /// Items encolados por hora **entre los que siguen esperando**. Subestima
    /// cuando el atraso es viejo, porque lo ya consumido se borro de la cola.
    pub entran_por_hora: f64,
    /// Corridas terminadas por hora, en la misma ventana.
    pub salen_por_hora: f64,
}

impl Store {
    /// ¿La cola se esta atrasando?
    ///
    /// # Por que hace falta
    ///
    /// Una cola que crece de forma sostenida es un sintoma que **no depende de
    /// por que crece**. Puede ser un scheduler mal configurado, un pipeline que
    /// se puso lento, una maquina con menos RAM o un cron nuevo demasiado
    /// frecuente: en todos los casos el numero a mirar es el mismo.
    ///
    /// Por eso esto avisa antes y mejor que cualquier chequeo de una causa
    /// puntual. En la instalacion real la cola llego a **1.400 items con 22
    /// horas de espera** antes de que alguien lo notara, y la causa —el
    /// scheduler corriendo con `paralelo=2` en vez de 6 porque no encontro su
    /// configuracion— no se parecia en nada a lo que se estuvo buscando.
    ///
    /// # Que dispara la alerta
    ///
    /// Las dos condiciones juntas, para no gritar por un pico normal:
    ///
    /// 1. Hay al menos `min_items` esperando.
    /// 2. El mas viejo espera mas que `espera_minima`.
    ///
    /// **No se usa la tasa de entrada como disparador**, aunque parezca lo
    /// natural: los items consumidos se BORRAN de la cola, asi que "cuantos
    /// entraron en la ultima hora" da cero justo cuando el atraso es viejo —
    /// que es cuando mas importa. Un test lo dejo en evidencia. Las tasas van
    /// igual en el reporte, porque el drenaje si se puede medir bien y es el
    /// numero que dice cuanto falta para ponerse al dia.
    ///
    /// Devuelve `None` cuando la cola esta al dia, que es lo normal.
    pub fn cola_atrasada(
        &self,
        ventana_seg: i64,
        espera_minima: i64,
        min_items: i64,
    ) -> Result<Option<ColaAtrasada>> {
        let c = self.conn.lock().unwrap();
        let ahora = Self::now();

        let (en_cola, mas_viejo): (i64, Option<i64>) = c.query_row(
            "SELECT COUNT(*), MIN(enqueued_at) FROM queue WHERE state = 'queued'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let espera_max_seg = mas_viejo.map(|t| ahora - t).unwrap_or(0);
        if en_cola < min_items.max(1) || espera_max_seg < espera_minima {
            return Ok(None);
        }

        let horas = (ventana_seg as f64 / 3600.0).max(0.001);
        let entraron: i64 = c.query_row(
            "SELECT COUNT(*) FROM queue WHERE enqueued_at >= ?1",
            rusqlite::params![ahora - ventana_seg],
            |r| r.get(0),
        )?;
        // Lo que SALE son corridas terminadas: es el drenaje real. Contar las
        // que arrancaron incluiria a las que todavia estan ocupando un lugar.
        let salieron: i64 = c.query_row(
            "SELECT COUNT(*) FROM runs
             WHERE finished_at IS NOT NULL AND finished_at >= ?1",
            rusqlite::params![ahora - ventana_seg],
            |r| r.get(0),
        )?;

        let entran_por_hora = entraron as f64 / horas;
        let salen_por_hora = salieron as f64 / horas;

        Ok(Some(ColaAtrasada {
            en_cola,
            espera_max_seg,
            entran_por_hora,
            salen_por_hora,
        }))
    }
}

#[cfg(test)]
mod tests_cola_atrasada {
    use crate::store::{RunState, Store};

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    fn envejecer_cola(s: &Store, seg: i64) {
        s.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE queue SET enqueued_at = enqueued_at - ?1",
                rusqlite::params![seg],
            )
            .unwrap();
    }

    /// **El caso real.** Entra mucho, sale poco, y el mas viejo lleva horas.
    #[test]
    fn una_cola_que_no_drena_se_detecta() {
        let s = store();
        for i in 0..40 {
            s.enqueue("d", &format!("p{i}"), 0).unwrap();
        }
        envejecer_cola(&s, 6 * 3600); // el mas viejo espera 6 h

        let a = s
            .cola_atrasada(3600, 3600, 5)
            .unwrap()
            .expect("tiene que avisar");
        assert_eq!(a.en_cola, 40);
        assert!(
            a.espera_max_seg >= 6 * 3600,
            "y dice cuanto espera el mas viejo"
        );
        // El drenaje es cero porque no termino ninguna: eso es exactamente lo
        // que hay que reportar, no un motivo para callarse.
        assert_eq!(a.salen_por_hora, 0.0);
    }

    /// Una cola al dia no avisa. Es lo normal y no puede generar ruido.
    #[test]
    fn una_cola_al_dia_no_avisa() {
        let s = store();
        assert!(s.cola_atrasada(3600, 3600, 5).unwrap().is_none(), "vacia");

        s.enqueue("d", "p", 0).unwrap();
        assert!(
            s.cola_atrasada(3600, 3600, 5).unwrap().is_none(),
            "un item recien encolado no es un atraso"
        );
    }

    /// Pocos items, aunque lleven rato, no son un atraso: puede ser un DAG
    /// lento y nada mas. El tamaño minimo es lo que separa eso de una cola que
    /// se desbordo.
    #[test]
    fn pocos_items_no_son_un_atraso() {
        let s = store();
        for i in 0..3 {
            s.enqueue("d", &format!("p{i}"), 0).unwrap();
        }
        envejecer_cola(&s, 6 * 3600);
        assert!(s.cola_atrasada(3600, 3600, 5).unwrap().is_none());
    }

    /// El reporte trae el drenaje medido, que es el numero que dice cuanto
    /// falta para ponerse al dia.
    #[test]
    fn el_reporte_trae_el_drenaje_medido() {
        let s = store();
        for i in 0..40 {
            s.enqueue("d", &format!("p{i}"), 0).unwrap();
        }
        envejecer_cola(&s, 6 * 3600);
        for i in 0..7 {
            let r = s.create_run("d", &format!("hecho{i}")).unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        let a = s.cola_atrasada(3600, 3600, 5).unwrap().unwrap();
        assert!(a.salen_por_hora >= 7.0, "salieron 7: {}", a.salen_por_hora);
    }

    /// Muchos items pero recien encolados tampoco: la espera es la que dice si
    /// hay un problema, no la cantidad.
    #[test]
    fn muchos_items_recien_encolados_no_son_un_atraso() {
        let s = store();
        for i in 0..500 {
            s.enqueue("d", &format!("p{i}"), 0).unwrap();
        }
        assert!(s.cola_atrasada(3600, 3600, 5).unwrap().is_none());
    }
}

impl Store {
    /// Mediana de duracion de cada DAG, en segundos, de una sola consulta.
    ///
    /// Se usa para estimar cuanto trabajo genera un horario. La mediana y no el
    /// promedio: un scraper que normalmente tarda 30 s y a veces 40 minutos
    /// tiene un promedio que no describe ninguno de los dos casos, y para
    /// planificar capacidad lo que importa es el caso tipico.
    pub fn medianas_por_dag(&self) -> Result<Vec<(String, f64)>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT dag, MAX(dur) FROM (
                 SELECT dag, finished_at - started_at AS dur,
                        PERCENT_RANK() OVER (PARTITION BY dag
                                             ORDER BY finished_at - started_at) AS pr
                 FROM runs
                 WHERE state = 'Success' AND finished_at IS NOT NULL
             ) WHERE pr <= 0.5 GROUP BY dag",
        )?;
        let v = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(v)
    }
}
