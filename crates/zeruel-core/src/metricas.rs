//! Consultas de la pestaña Métricas.
//!
//! El diseño anterior leía `rollups_monthly`, una tabla pre-agregada que la GC
//! llena con los meses **ya cerrados**. Eso tenía una consecuencia que nadie
//! quería: una instalación nueva no mostraba nada hasta que terminara el mes,
//! y en régimen tampoco mostraba nunca el mes en curso —que es el único que
//! alguien mira cuando algo se rompió.
//!
//! Acá se invierte: **las métricas se calculan sobre `runs` y `task_runs`**, y
//! `rollups_monthly` queda para lo que siempre debió ser, el archivo de lo que
//! la purga ya borró.
//!
//! Todo filtro baja al `WHERE`. Es el error que este repo ya cometió dos veces
//! (N-36, y el bug A de `docs/PLAN-UI.md`): traer 2.000 filas para mostrar 10
//! anda bien hasta que hay volumen, y falla justo cuando más se la necesita.

use crate::store::Store;
use anyhow::Result;
use rusqlite::params_from_iter;
use serde::Serialize;

/// Qué recorte de las métricas se pide.
///
/// `desde`/`hasta` son unix seconds, con `hasta` **exclusivo**: así dos rangos
/// contiguos no cuentan dos veces la corrida del límite.
#[derive(Clone, Debug)]
pub struct Filtro {
    pub desde: i64,
    pub hasta: i64,
    pub dag: Option<String>,
    /// Sólo corridas cuyo pico de RAM superó esto (MB).
    pub ram_min_mb: Option<i64>,
    /// Sólo corridas que duraron más que esto (segundos).
    pub dur_min_secs: Option<i64>,
    /// Sólo las que terminaron en fallo.
    pub solo_fallos: bool,
}

impl Filtro {
    /// Un rango sin más recortes.
    pub fn rango(desde: i64, hasta: i64) -> Self {
        Self {
            desde,
            hasta,
            dag: None,
            ram_min_mb: None,
            dur_min_secs: None,
            solo_fallos: false,
        }
    }

    /// El `WHERE` y sus parámetros, compartido por todas las consultas.
    ///
    /// Devuelve condiciones sobre `r` (alias de `runs`). La RAM se compara
    /// contra el pico del run —el máximo de sus tareas—, no contra cada tarea:
    /// "corridas que pasaron de 500 MB" es la pregunta que se hace.
    fn where_runs(&self) -> (String, Vec<Box<dyn rusqlite::ToSql>>) {
        let mut cond = vec!["r.started_at >= ?".to_string(), "r.started_at < ?".into()];
        let mut par: Vec<Box<dyn rusqlite::ToSql>> =
            vec![Box::new(self.desde), Box::new(self.hasta)];

        if let Some(d) = &self.dag {
            cond.push("r.dag = ?".into());
            par.push(Box::new(d.clone()));
        }
        if self.solo_fallos {
            cond.push("r.state = 'Failed'".into());
        }
        if let Some(s) = self.dur_min_secs {
            // Un run sin `finished_at` sigue corriendo: no tiene duración
            // todavía, así que no puede cumplir ni incumplir el filtro.
            cond.push("(r.finished_at IS NOT NULL AND r.finished_at - r.started_at >= ?)".into());
            par.push(Box::new(s));
        }
        if let Some(mb) = self.ram_min_mb {
            cond.push(
                "COALESCE((SELECT MAX(tr.ram_peak_mb) FROM task_runs tr
                           WHERE tr.run_id = r.id AND tr.reused = 0), 0) >= ?"
                    .into(),
            );
            par.push(Box::new(mb));
        }
        (cond.join(" AND "), par)
    }

    /// ¿El rango es largo como para agrupar por mes en vez de por día?
    ///
    /// El corte está en 70 días: dos meses se leen bien día por día, pero un
    /// año son 365 renglones que nadie mira.
    fn por_mes(&self) -> bool {
        self.hasta - self.desde > 70 * 86_400
    }

    fn formato_periodo(&self) -> &'static str {
        if self.por_mes() {
            "%Y-%m"
        } else {
            "%Y-%m-%d"
        }
    }
}

/// Una fila de la tabla de volumen: un período y un DAG.
#[derive(Clone, Debug, Serialize)]
pub struct VolumenRow {
    pub periodo: String,
    pub dag: String,
    pub runs: i64,
    pub ok: i64,
    pub fail: i64,
    pub wall_secs: f64,
    pub ram_peak_mb: i64,
    /// Viene de `rollups_monthly`: el detalle ya se purgó y esto es todo lo
    /// que queda. La UI lo marca para que nadie lo confunda con datos vivos.
    pub archivado: bool,
}

/// Salud de un DAG en el período: con qué frecuencia falla y cuánto tarda.
#[derive(Clone, Debug, Serialize)]
pub struct DagStat {
    pub dag: String,
    pub runs: i64,
    pub ok: i64,
    pub fail: i64,
    pub cancelled: i64,
    /// Fracción 0..1. Se calcula sobre las **terminadas**: contar las que
    /// siguen corriendo como "no fallaron todavía" diluye la tasa.
    pub tasa_fallo: f64,
    pub p50_secs: f64,
    pub p95_secs: f64,
    pub max_secs: f64,
    pub ram_peak_mb: i64,
    pub ultima: Option<i64>,
}

/// Una tarea **dentro de un DAG**.
///
/// Antes se agrupaba sólo por nombre de tarea, y como los 47 scrapers tienen
/// las mismas tres, `scrape` era una fila con 614 ejecuciones de 45 DAGs
/// distintos: mezclaba uno de 4.400 s con otro de 30 s y el promedio no
/// describía a ninguno.
#[derive(Clone, Debug, Serialize)]
pub struct TareaStat {
    pub dag: String,
    pub task: String,
    pub ejecuciones: i64,
    pub ok: i64,
    pub fail: i64,
    pub filas_prom: f64,
    pub cpu_prom: f64,
    pub ram_peak_mb: i64,
    pub p50_secs: f64,
    pub p95_secs: f64,
    /// Cuántas veces esta tarea necesitó más de un intento.
    pub con_reintentos: i64,
}

/// Un punto del gráfico de actividad.
#[derive(Clone, Debug, Serialize)]
pub struct Bucket {
    pub t: String,
    pub ok: i64,
    pub fail: i64,
    pub otras: i64,
}

impl Store {
    /// Volumen por período y DAG, calculado sobre `runs`.
    ///
    /// Si el rango llega más atrás que la retención (`runs` se purga al año),
    /// se completa con `rollups_monthly`, que sobrevive a la purga. Esas filas
    /// van marcadas con `archivado`.
    pub fn met_volumen(&self, f: &Filtro) -> Result<Vec<VolumenRow>> {
        let c = self.conn.lock().unwrap();
        let (w, par) = f.where_runs();
        let fmt = f.formato_periodo();

        let sql = format!(
            "SELECT strftime('{fmt}', r.started_at, 'unixepoch', 'localtime') AS periodo,
                    r.dag,
                    COUNT(*),
                    SUM(CASE WHEN r.state='Success' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN r.state='Failed' THEN 1 ELSE 0 END),
                    COALESCE(SUM(CASE WHEN r.finished_at IS NOT NULL
                                      THEN r.finished_at - r.started_at END), 0),
                    COALESCE(MAX((SELECT MAX(tr.ram_peak_mb) FROM task_runs tr
                                  WHERE tr.run_id = r.id AND tr.reused = 0)), 0)
             FROM runs r WHERE {w}
             GROUP BY periodo, r.dag
             ORDER BY periodo DESC, r.dag ASC"
        );
        let mut stmt = c.prepare(&sql)?;
        let vivos = stmt
            .query_map(params_from_iter(par.iter()), |r| {
                Ok(VolumenRow {
                    periodo: r.get(0)?,
                    dag: r.get(1)?,
                    runs: r.get(2)?,
                    ok: r.get(3)?,
                    fail: r.get(4)?,
                    wall_secs: r.get(5)?,
                    ram_peak_mb: r.get(6)?,
                    archivado: false,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        // Lo archivado sólo aporta si el rango pide más atrás de lo que `runs`
        // conserva. Si no, sería duplicar los mismos meses.
        let corte_retencion = Self::now() - (365 * 86_400);
        if f.desde >= corte_retencion {
            return Ok(vivos);
        }
        drop(stmt);

        let mut cond = vec!["mes < strftime('%Y-%m', ?, 'unixepoch', 'localtime')".to_string()];
        let mut par2: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(corte_retencion)];
        cond.push("mes >= strftime('%Y-%m', ?, 'unixepoch', 'localtime')".into());
        par2.push(Box::new(f.desde));
        if let Some(d) = &f.dag {
            cond.push("dag = ?".into());
            par2.push(Box::new(d.clone()));
        }
        let sql = format!(
            "SELECT mes, dag, runs, ok, fail, wall_secs, ram_peak_mb
             FROM rollups_monthly WHERE {} ORDER BY mes DESC, dag ASC",
            cond.join(" AND ")
        );
        let mut stmt = c.prepare(&sql)?;
        let viejos = stmt
            .query_map(params_from_iter(par2.iter()), |r| {
                Ok(VolumenRow {
                    periodo: r.get(0)?,
                    dag: r.get(1)?,
                    runs: r.get(2)?,
                    ok: r.get(3)?,
                    fail: r.get(4)?,
                    wall_secs: r.get(5)?,
                    ram_peak_mb: r.get(6)?,
                    archivado: true,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut todo = vivos;
        todo.extend(viejos);
        Ok(todo)
    }

    /// Salud por DAG, ordenado por tasa de fallo descendente.
    ///
    /// Es la tabla que contesta "¿qué arreglo primero?". Los percentiles salen
    /// con `PERCENT_RANK()` en SQL: traer las duraciones a memoria para
    /// ordenarlas en Rust sería el mismo error que este módulo evita.
    pub fn met_por_dag(&self, f: &Filtro) -> Result<Vec<DagStat>> {
        let c = self.conn.lock().unwrap();
        let (w, par) = f.where_runs();

        let sql = format!(
            "WITH filtradas AS (
                 SELECT r.id, r.dag, r.state, r.started_at, r.finished_at,
                        CASE WHEN r.finished_at IS NOT NULL
                             THEN r.finished_at - r.started_at END AS dur,
                        COALESCE((SELECT MAX(tr.ram_peak_mb) FROM task_runs tr
                                  WHERE tr.run_id = r.id AND tr.reused = 0), 0) AS ram
                 FROM runs r WHERE {w}
             ),
             pct AS (
                 SELECT dag, dur,
                        PERCENT_RANK() OVER (PARTITION BY dag ORDER BY dur) AS pr
                 FROM filtradas WHERE dur IS NOT NULL
             )
             SELECT f.dag,
                    COUNT(*),
                    SUM(CASE WHEN f.state='Success'   THEN 1 ELSE 0 END),
                    SUM(CASE WHEN f.state='Failed'    THEN 1 ELSE 0 END),
                    SUM(CASE WHEN f.state='Cancelled' THEN 1 ELSE 0 END),
                    COALESCE((SELECT MAX(p.dur) FROM pct p
                              WHERE p.dag=f.dag AND p.pr<=0.50), 0),
                    COALESCE((SELECT MAX(p.dur) FROM pct p
                              WHERE p.dag=f.dag AND p.pr<=0.95), 0),
                    COALESCE(MAX(f.dur), 0),
                    COALESCE(MAX(f.ram), 0),
                    MAX(f.started_at)
             FROM filtradas f GROUP BY f.dag"
        );
        let mut stmt = c.prepare(&sql)?;
        let mut filas = stmt
            .query_map(params_from_iter(par.iter()), |r| {
                let runs: i64 = r.get(1)?;
                let ok: i64 = r.get(2)?;
                let fail: i64 = r.get(3)?;
                let cancelled: i64 = r.get(4)?;
                // Sobre las terminadas: las que siguen corriendo todavía no
                // fallaron ni salieron bien, y contarlas diluye la tasa.
                let term = ok + fail + cancelled;
                Ok(DagStat {
                    dag: r.get(0)?,
                    runs,
                    ok,
                    fail,
                    cancelled,
                    tasa_fallo: if term > 0 {
                        fail as f64 / term as f64
                    } else {
                        0.0
                    },
                    p50_secs: r.get(5)?,
                    p95_secs: r.get(6)?,
                    max_secs: r.get(7)?,
                    ram_peak_mb: r.get(8)?,
                    ultima: r.get(9)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        // Lo roto primero; a igual tasa, el que más corridas tiene.
        filas.sort_by(|a, b| {
            b.tasa_fallo
                .partial_cmp(&a.tasa_fallo)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(b.runs.cmp(&a.runs))
                .then(a.dag.cmp(&b.dag))
        });
        Ok(filas)
    }

    /// Estadísticas por `(DAG, tarea)`, ordenadas por p95 descendente.
    pub fn met_por_tarea(&self, f: &Filtro) -> Result<Vec<TareaStat>> {
        let c = self.conn.lock().unwrap();
        let (w, par) = f.where_runs();

        let sql = format!(
            "WITH t AS (
                 SELECT r.dag, tr.task, tr.state, tr.rows, tr.cpu_secs,
                        tr.ram_peak_mb, tr.intentos,
                        CASE WHEN tr.finished_at IS NOT NULL
                             THEN tr.finished_at - tr.started_at END AS dur
                 FROM task_runs tr JOIN runs r ON r.id = tr.run_id
                 WHERE {w} AND tr.reused = 0
             ),
             pct AS (
                 SELECT dag, task, dur,
                        PERCENT_RANK() OVER (PARTITION BY dag, task ORDER BY dur) AS pr
                 FROM t WHERE dur IS NOT NULL
             )
             SELECT t.dag, t.task,
                    COUNT(*),
                    SUM(CASE WHEN t.state='Success' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN t.state='Failed'  THEN 1 ELSE 0 END),
                    COALESCE(AVG(t.rows), 0),
                    COALESCE(AVG(t.cpu_secs), 0),
                    COALESCE(MAX(t.ram_peak_mb), 0),
                    COALESCE((SELECT MAX(p.dur) FROM pct p
                              WHERE p.dag=t.dag AND p.task=t.task AND p.pr<=0.50), 0),
                    COALESCE((SELECT MAX(p.dur) FROM pct p
                              WHERE p.dag=t.dag AND p.task=t.task AND p.pr<=0.95), 0),
                    SUM(CASE WHEN t.intentos > 1 THEN 1 ELSE 0 END)
             FROM t GROUP BY t.dag, t.task"
        );
        let mut stmt = c.prepare(&sql)?;
        let mut filas = stmt
            .query_map(params_from_iter(par.iter()), |r| {
                Ok(TareaStat {
                    dag: r.get(0)?,
                    task: r.get(1)?,
                    ejecuciones: r.get(2)?,
                    ok: r.get(3)?,
                    fail: r.get(4)?,
                    filas_prom: r.get(5)?,
                    cpu_prom: r.get(6)?,
                    ram_peak_mb: r.get(7)?,
                    p50_secs: r.get(8)?,
                    p95_secs: r.get(9)?,
                    con_reintentos: r.get(10)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        filas.sort_by(|a, b| {
            b.p95_secs
                .partial_cmp(&a.p95_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.dag.cmp(&b.dag))
        });
        Ok(filas)
    }

    /// Actividad a lo largo del período, para el gráfico.
    ///
    /// El bucket es la hora para rangos de hasta dos días y el día para el
    /// resto: 30 días por hora son 720 barras que no entran en una pantalla.
    ///
    /// **Los períodos sin corridas van igual, en cero.** Devolver sólo los que
    /// tienen datos parece una optimización y es un error: el gráfico dibuja
    /// una barra por elemento, así que un dia con datos dentro de una ventana
    /// de siete se veia como *una barra ocupando toda la semana*. El eje de
    /// tiempo tiene que ser continuo o miente sobre cuando paso lo que paso.
    pub fn met_actividad(&self, f: &Filtro) -> Result<Vec<Bucket>> {
        let c = self.conn.lock().unwrap();
        let (w, par) = f.where_runs();
        let por_hora = f.hasta - f.desde <= 2 * 86_400;
        let fmt = if por_hora {
            "%Y-%m-%d %H:00"
        } else {
            "%Y-%m-%d"
        };

        let sql = format!(
            "SELECT strftime('{fmt}', r.started_at, 'unixepoch', 'localtime') AS t,
                    SUM(CASE WHEN r.state='Success' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN r.state='Failed'  THEN 1 ELSE 0 END),
                    SUM(CASE WHEN r.state NOT IN ('Success','Failed') THEN 1 ELSE 0 END)
             FROM runs r WHERE {w} GROUP BY t ORDER BY t ASC"
        );
        let mut stmt = c.prepare(&sql)?;
        let filas = stmt
            .query_map(params_from_iter(par.iter()), |r| {
                Ok(Bucket {
                    t: r.get(0)?,
                    ok: r.get(1)?,
                    fail: r.get(2)?,
                    otras: r.get(3)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let con_datos: std::collections::HashMap<String, Bucket> =
            filas.into_iter().map(|b| (b.t.clone(), b)).collect();
        Ok(rejilla(f.desde, f.hasta, por_hora)
            .into_iter()
            .map(|t| {
                con_datos.get(&t).cloned().unwrap_or(Bucket {
                    t,
                    ok: 0,
                    fail: 0,
                    otras: 0,
                })
            })
            .collect())
    }

    /// Los DAGs que existen en el período, para el buscador del filtro.
    pub fn met_dags(&self, f: &Filtro) -> Result<Vec<String>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT DISTINCT dag FROM runs
             WHERE started_at >= ?1 AND started_at < ?2 ORDER BY dag",
        )?;
        let v = stmt
            .query_map([f.desde, f.hasta], |r| r.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(v)
    }

    /// ¿Alguna tarea del período reportó filas?
    ///
    /// La columna `filas prom.` estaba en 0 en las 2.050 filas de la
    /// instalación real, porque las tareas que envuelven un proceso externo no
    /// reportan filas. Una columna que siempre dice 0 entrena a la gente a
    /// ignorar la tabla entera, así que la UI la muestra sólo si hay algo.
    pub fn met_hay_filas(&self, f: &Filtro) -> Result<bool> {
        let c = self.conn.lock().unwrap();
        let n: i64 = c.query_row(
            "SELECT COUNT(*) FROM task_runs tr JOIN runs r ON r.id = tr.run_id
             WHERE r.started_at >= ?1 AND r.started_at < ?2
               AND tr.reused = 0 AND tr.rows > 0",
            [f.desde, f.hasta],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }
}

/// Todos los períodos entre `desde` y `hasta`, tengan datos o no.
///
/// Las etiquetas se arman en hora **local** y con el mismo formato que usa
/// `strftime(..., 'localtime')` en la consulta, porque se cruzan por igualdad
/// de texto: si difieren, cada bucket con datos quedaría huérfano y el gráfico
/// se dibujaría todo en cero.
///
/// El avance es por fecha de calendario, no sumando 86.400 segundos: en los
/// cambios de horario un día dura 23 o 25 horas y el paso fijo se desalinea,
/// duplicando o salteando un día.
fn rejilla(desde: i64, hasta: i64, por_hora: bool) -> Vec<String> {
    use chrono::{Local, TimeZone, Timelike};

    let ini = match Local.timestamp_opt(desde, 0).single() {
        Some(t) => t,
        None => return Vec::new(),
    };
    let fin = match Local.timestamp_opt(hasta, 0).single() {
        Some(t) => t,
        None => return Vec::new(),
    };

    let mut out = Vec::new();
    if por_hora {
        let mut t = ini
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .unwrap_or(ini);
        // Un tope duro: sin esto, un rango absurdo llenaria la memoria.
        while t <= fin && out.len() < 2_000 {
            out.push(t.format("%Y-%m-%d %H:00").to_string());
            t += chrono::TimeDelta::hours(1);
        }
    } else {
        let mut d = ini.date_naive();
        let ultimo = fin.date_naive();
        while d <= ultimo && out.len() < 2_000 {
            out.push(d.format("%Y-%m-%d").to_string());
            d = match d.succ_opt() {
                Some(s) => s,
                None => break,
            };
        }
    }
    out
}

#[cfg(test)]
mod tests_metricas {
    use crate::metricas::Filtro;
    use crate::store::{RunState, Store, TaskStateRecord};
    use zeruel_sdk::TaskState;

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    fn ahora() -> i64 {
        chrono::Utc::now().timestamp()
    }

    fn rango_de_hoy() -> Filtro {
        Filtro::rango(ahora() - 3600, ahora() + 3600)
    }

    /// El bug que motivo todo: la tabla principal leia `rollups_monthly`, que
    /// la GC llena solo con meses YA CERRADOS. Una instalacion nueva no
    /// mostraba nada hasta que terminara el mes, y en regimen tampoco mostraba
    /// nunca el mes en curso, que es el unico que alguien mira cuando algo se
    /// rompio.
    #[test]
    fn una_corrida_de_hoy_aparece_sin_esperar_a_fin_de_mes() {
        let s = store();
        let r = s.create_run("ventas", "hoy").unwrap();
        s.set_run_state(r, RunState::Success).unwrap();

        let v = s.met_volumen(&rango_de_hoy()).unwrap();
        assert_eq!(v.len(), 1, "la corrida de hoy tiene que aparecer: {v:?}");
        assert_eq!(v[0].dag, "ventas");
        assert_eq!(v[0].runs, 1);
        assert!(!v[0].archivado);

        // Y la GC no hace falta para verla.
        assert_eq!(s.rollups().unwrap().len(), 0, "sin gc, rollups sigue vacia");
    }

    /// El filtro baja al SQL: pedir un rango no puede traer lo de afuera.
    #[test]
    fn el_rango_recorta_de_verdad() {
        let s = store();
        let viejo = s.create_run("a", "viejo").unwrap();
        s.set_run_state(viejo, RunState::Success).unwrap();
        s.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE runs SET started_at = started_at - ?1 WHERE id = ?2",
                rusqlite::params![10 * 86_400, viejo.to_string()],
            )
            .unwrap();
        let nuevo = s.create_run("a", "nuevo").unwrap();
        s.set_run_state(nuevo, RunState::Success).unwrap();

        assert_eq!(
            s.met_volumen(&rango_de_hoy()).unwrap()[0].runs,
            1,
            "solo la de hoy"
        );

        let mes = Filtro::rango(ahora() - 30 * 86_400, ahora() + 3600);
        let total: i64 = s.met_volumen(&mes).unwrap().iter().map(|v| v.runs).sum();
        assert_eq!(total, 2, "el rango largo trae las dos");
    }

    /// La tabla que contesta cual arreglar primero.
    #[test]
    fn la_tasa_de_fallo_ordena_lo_roto_primero() {
        let s = store();
        for (dag, estados) in [
            ("sano", vec![true, true, true]),
            ("roto", vec![false, false]),
            ("a_medias", vec![true, false]),
        ] {
            for ok in estados {
                let r = s.create_run(dag, "p").unwrap();
                let e = if ok {
                    RunState::Success
                } else {
                    RunState::Failed
                };
                s.set_run_state(r, e).unwrap();
            }
        }
        let d = s.met_por_dag(&rango_de_hoy()).unwrap();
        assert_eq!(d[0].dag, "roto", "lo peor primero: {d:?}");
        assert_eq!(d[0].tasa_fallo, 1.0);
        assert_eq!(d[1].dag, "a_medias");
        assert_eq!(d[1].tasa_fallo, 0.5);
        assert_eq!(d[2].tasa_fallo, 0.0);
    }

    /// Una corrida que sigue corriendo no baja la tasa de fallo: todavia no
    /// fallo ni salio bien, y contarla diluye el numero.
    #[test]
    fn lo_que_sigue_corriendo_no_diluye_la_tasa() {
        let s = store();
        let f1 = s.create_run("x", "a").unwrap();
        s.set_run_state(f1, RunState::Failed).unwrap();
        let _corriendo = s.create_run("x", "b").unwrap(); // sin cerrar

        let d = s.met_por_dag(&rango_de_hoy()).unwrap();
        assert_eq!(d[0].runs, 2, "cuenta las dos");
        assert_eq!(d[0].tasa_fallo, 1.0, "pero la tasa es sobre las terminadas");
    }

    /// Antes se agrupaba solo por nombre de tarea, y los 47 scrapers tienen las
    /// mismas tres: `scrape` era UNA fila con 614 ejecuciones de 45 DAGs, que
    /// mezclaba uno de 4.400s con otro de 30s.
    #[test]
    fn las_tareas_se_separan_por_dag() {
        let s = store();
        for dag in ["scraping_norte", "scraping_sur"] {
            let r = s.create_run(dag, "p").unwrap();
            s.set_task_state(TaskStateRecord::medido(
                r,
                "scrape",
                &TaskState::Success,
                0,
                "ok",
                10,
                1.0,
            ))
            .unwrap();
        }
        let t = s.met_por_tarea(&rango_de_hoy()).unwrap();
        assert_eq!(t.len(), 2, "una fila por (dag, tarea), no una sola: {t:?}");
        assert!(t.iter().all(|x| x.task == "scrape"));
        let mut dags: Vec<_> = t.iter().map(|x| x.dag.as_str()).collect();
        dags.sort();
        assert_eq!(dags, ["scraping_norte", "scraping_sur"]);
    }

    /// El filtro de RAM, comparando contra el pico del run.
    #[test]
    fn el_filtro_de_ram_recorta_por_el_pico_del_run() {
        let s = store();
        for (dag, mb) in [("liviano", 20u64), ("pesado", 800)] {
            let r = s.create_run(dag, "p").unwrap();
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
        let mut f = rango_de_hoy();
        f.ram_min_mb = Some(500);
        let d = s.met_por_dag(&f).unwrap();
        assert_eq!(d.len(), 1, "solo el pesado: {d:?}");
        assert_eq!(d[0].dag, "pesado");
    }

    /// Los reintentos consumidos no se registraban: `retries_left` solo se
    /// escribe en el registro final y ahi siempre vale 0, asi que una tarea que
    /// salio a la tercera se veia igual que una que salio a la primera.
    #[test]
    fn los_reintentos_consumidos_quedan_registrados() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        s.set_task_state(TaskStateRecord::con_intentos(
            r,
            "t",
            &TaskState::Success,
            0,
            "ok",
            10,
            0.0,
            3,
        ))
        .unwrap();
        let t = s.met_por_tarea(&rango_de_hoy()).unwrap();
        assert_eq!(t[0].con_reintentos, 1, "salio, pero al tercer intento");
    }

    /// La columna de filas estaba en 0 en las 2.050 filas reales, porque las
    /// tareas que envuelven un proceso externo no reportan filas.
    #[test]
    fn la_columna_de_filas_se_anuncia_solo_si_alguien_la_llena() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r,
            "t",
            &TaskState::Success,
            0,
            "ok",
            10,
            0.0,
        ))
        .unwrap();
        let f = rango_de_hoy();
        assert!(!s.met_hay_filas(&f).unwrap(), "nadie reporto filas");

        let r2 = s.create_run("d", "p2").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r2,
            "t",
            &TaskState::Success,
            1_500,
            "ok",
            10,
            0.0,
        ))
        .unwrap();
        assert!(s.met_hay_filas(&f).unwrap(), "ahora si");
    }

    /// El grafico dibuja una barra por elemento. Si solo se devolvieran los
    /// periodos CON datos, un dia con corridas dentro de una ventana de siete
    /// se veria como una barra ocupando la semana entera: el eje de tiempo
    /// mentiria sobre cuando paso lo que paso.
    #[test]
    fn los_periodos_vacios_van_igual_en_cero() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        s.set_run_state(r, RunState::Success).unwrap();

        let f = Filtro::rango(ahora() - 6 * 86_400, ahora() + 60);
        let a = s.met_actividad(&f).unwrap();
        assert_eq!(a.len(), 7, "siete dias, siete barras: {a:?}");
        let con_datos: Vec<_> = a.iter().filter(|b| b.ok + b.fail + b.otras > 0).collect();
        assert_eq!(con_datos.len(), 1, "solo uno tiene corridas");
        assert_eq!(con_datos[0].ok, 1);
        // Y el ultimo es hoy: el orden es cronologico.
        assert_eq!(a[6].ok, 1, "la corrida de hoy va al final: {a:?}");
    }

    /// Las etiquetas de la rejilla se cruzan por igualdad de texto contra las
    /// que arma SQLite. Si los formatos se separan, todos los buckets con datos
    /// quedan huerfanos y el grafico se dibuja entero en cero, sin ningun error.
    #[test]
    fn las_etiquetas_coinciden_con_las_de_sqlite() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        s.set_run_state(r, RunState::Success).unwrap();

        // Por hora (rango corto) y por dia (rango largo): los dos formatos.
        for (desde, _quien) in [(ahora() - 3600, "hora"), (ahora() - 5 * 86_400, "dia")] {
            let f = Filtro::rango(desde, ahora() + 60);
            let a = s.met_actividad(&f).unwrap();
            let total: i64 = a.iter().map(|b| b.ok).sum();
            assert_eq!(
                total, 1,
                "la corrida tiene que caer en algun bucket, no perderse: {a:?}"
            );
        }
    }

    /// Las tareas reutilizadas (durabilidad) no son mediciones de este run.
    #[test]
    fn las_reutilizadas_siguen_quedando_afuera() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        let mut rec = TaskStateRecord::medido(r, "t", &TaskState::Success, 0, "ok", 999, 5.0);
        rec.reused = true;
        s.set_task_state(rec).unwrap();
        let t = s.met_por_tarea(&rango_de_hoy()).unwrap();
        assert!(t.is_empty(), "una reutilizada no aporta metricas: {t:?}");
    }
}
