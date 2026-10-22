//! Chequeos de datos: una tarea que corta el DAG si los datos no son los
//! esperados, y **dice cual es el problema**.
//!
//! El fallo mas caro de un ETL no es el que revienta: es el que sigue andando
//! con datos malos. La API devolvio un `[]`, el `WHERE` no matcheo nada, una
//! columna vino toda en null — el pipeline corre feliz, escribe el resultado, y
//! alguien se entera tres dias despues mirando un tablero raro.
//!
//! Un [`Chequeo`] se pone como una tarea mas del grafo, antes o despues de la
//! transformacion:
//!
//! ```text
//!   bajar ──► chequear_entrada ──► limpiar ──► chequear_salida ──► cargar
//! ```
//!
//! Si algo no da, la tarea falla, el DAG se corta ahi y **no se carga nada**.
//!
//! ```no_run
//! use zeruel_data::{Chequeo, DataEnv};
//!
//! let env = DataEnv::new("data");
//!
//! // Lo que tiene que cumplir lo que entra
//! let entrada = Chequeo::new("chequear_entrada", env.clone(), "input/ventas.parquet")
//!     .filas_entre(1_000, 50_000_000)
//!     .columnas(["id", "monto", "fecha"])
//!     .sin_nulos(["id", "monto"])
//!     .unicos("id")
//!     .rango("monto", 0.0, 1_000_000.0);
//!
//! // Lo que tiene que cumplir lo que sale
//! let salida = Chequeo::new("chequear_salida", env, "output/rollups.parquet")
//!     .no_vacio()
//!     .sin_nulos(["total"]);
//! ```
//!
//! Todo se resuelve en **una sola pasada** sobre el archivo, y sobre Parquet
//! buena parte sale de los metadatos sin leer los datos.

use anyhow::Result;
use polars::prelude::*;
use zeruel_sdk::{Task, TaskContext, TaskId, TaskOutput};

use crate::tasks::{read_parquet, DataEnv};

/// Una condicion que los datos tienen que cumplir.
#[derive(Clone, Debug)]
enum Regla {
    FilasEntre(u64, u64),
    Columnas(Vec<String>),
    SinNulos(Vec<String>),
    Unicos(String),
    Rango(String, f64, f64),
}

/// Chequea que un archivo de datos cumpla lo que se espera de el.
///
/// Ver el modulo para el detalle.
pub struct Chequeo {
    id: TaskId,
    env: DataEnv,
    archivo: String,
    reglas: Vec<Regla>,
}

impl Chequeo {
    /// Un chequeo sobre `archivo` (relativo al [`DataEnv`], o una URL de nube).
    pub fn new(id: impl Into<String>, env: DataEnv, archivo: impl Into<String>) -> Self {
        Self {
            id: TaskId(id.into()),
            env,
            archivo: archivo.into(),
            reglas: Vec::new(),
        }
    }

    /// La cantidad de filas tiene que caer en este rango (inclusive).
    pub fn filas_entre(mut self, min: u64, max: u64) -> Self {
        self.reglas.push(Regla::FilasEntre(min, max));
        self
    }

    /// Tiene que haber al menos una fila.
    ///
    /// Es el chequeo que mas veces salva: cubre el caso de la API que contesta
    /// vacio y el del filtro que no matcheo nada.
    pub fn no_vacio(self) -> Self {
        self.filas_entre(1, u64::MAX)
    }

    /// Estas columnas tienen que existir.
    pub fn columnas<I, S>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.reglas
            .push(Regla::Columnas(cols.into_iter().map(Into::into).collect()));
        self
    }

    /// Estas columnas no pueden tener ningun null.
    pub fn sin_nulos<I, S>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.reglas
            .push(Regla::SinNulos(cols.into_iter().map(Into::into).collect()));
        self
    }

    /// Esta columna no puede tener valores repetidos (una clave).
    pub fn unicos(mut self, col: impl Into<String>) -> Self {
        self.reglas.push(Regla::Unicos(col.into()));
        self
    }

    /// Los valores de esta columna tienen que caer entre `min` y `max`.
    pub fn rango(mut self, col: impl Into<String>, min: f64, max: f64) -> Self {
        self.reglas.push(Regla::Rango(col.into(), min, max));
        self
    }
}

impl Task for Chequeo {
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    fn run(&self, _ctx: &TaskContext) -> Result<TaskOutput> {
        let lf = read_parquet(&self.env, &self.archivo)?;

        // Las columnas se chequean primero y aparte: si falta una, las demas
        // agregaciones fallarian con un error de polars que no dice nada util.
        let esquema = lf
            .clone()
            .collect_schema()
            .map_err(|e| anyhow::anyhow!("no puedo leer el esquema de {}: {e}", self.archivo))?;
        let existentes: Vec<String> = esquema.iter_names().map(|s| s.to_string()).collect();

        let mut faltan_todas: Vec<String> = Vec::new();
        for r in &self.reglas {
            let pedidas: &[String] = match r {
                Regla::Columnas(c) | Regla::SinNulos(c) => c,
                Regla::Unicos(c) => std::slice::from_ref(c),
                Regla::Rango(c, _, _) => std::slice::from_ref(c),
                Regla::FilasEntre(..) => &[],
            };
            for p in pedidas {
                if !existentes.iter().any(|e| e == p) && !faltan_todas.contains(p) {
                    faltan_todas.push(p.clone());
                }
            }
        }
        if !faltan_todas.is_empty() {
            anyhow::bail!(
                "a {} le faltan columnas: {}. Tiene: {}",
                self.archivo,
                faltan_todas.join(", "),
                existentes.join(", ")
            );
        }

        // Todas las agregaciones en UNA pasada. Sobre Parquet, el conteo y los
        // min/max salen de los metadatos sin leer los datos.
        let mut exprs = vec![len().alias("__filas")];
        for (i, r) in self.reglas.iter().enumerate() {
            match r {
                Regla::SinNulos(cols) => {
                    for (j, c) in cols.iter().enumerate() {
                        exprs.push(col(c.as_str()).null_count().alias(format!("__n{i}_{j}")));
                    }
                }
                Regla::Unicos(c) => {
                    exprs.push(col(c.as_str()).n_unique().alias(format!("__u{i}")));
                }
                Regla::Rango(c, _, _) => {
                    exprs.push(col(c.as_str()).min().alias(format!("__min{i}")));
                    exprs.push(col(c.as_str()).max().alias(format!("__max{i}")));
                }
                _ => {}
            }
        }

        let df = lf
            .select(exprs)
            .collect()
            .map_err(|e| anyhow::anyhow!("no puedo chequear {}: {e}", self.archivo))?;

        let num = |nombre: &str| -> f64 {
            df.column(nombre)
                .ok()
                .and_then(|c| c.get(0).ok())
                .and_then(|v| v.try_extract::<f64>().ok())
                .unwrap_or(f64::NAN)
        };
        let filas = num("__filas") as u64;

        // Se juntan TODAS las fallas antes de cortar: si hay tres problemas,
        // conviene enterarse de los tres de una, no de a uno por corrida.
        let mut fallas: Vec<String> = Vec::new();
        for (i, r) in self.reglas.iter().enumerate() {
            match r {
                Regla::FilasEntre(min, max) => {
                    if filas < *min || filas > *max {
                        fallas.push(if *max == u64::MAX {
                            format!("tiene {filas} filas y se esperaban al menos {min}")
                        } else {
                            format!("tiene {filas} filas y se esperaban entre {min} y {max}")
                        });
                    }
                }
                Regla::SinNulos(cols) => {
                    for (j, c) in cols.iter().enumerate() {
                        let n = num(&format!("__n{i}_{j}")) as u64;
                        if n > 0 {
                            let pct = 100.0 * n as f64 / filas.max(1) as f64;
                            fallas.push(format!(
                                "la columna '{c}' tiene {n} nulos ({pct:.1}% de {filas})"
                            ));
                        }
                    }
                }
                Regla::Unicos(c) => {
                    let u = num(&format!("__u{i}")) as u64;
                    if u < filas {
                        fallas.push(format!(
                            "la columna '{c}' deberia ser unica: {filas} filas pero {u} valores distintos ({} repetidos)",
                            filas - u
                        ));
                    }
                }
                Regla::Rango(c, min, max) => {
                    let (lo, hi) = (num(&format!("__min{i}")), num(&format!("__max{i}")));
                    if lo.is_finite() && lo < *min {
                        fallas.push(format!(
                            "'{c}' baja hasta {lo} y el minimo esperado es {min}"
                        ));
                    }
                    if hi.is_finite() && hi > *max {
                        fallas.push(format!(
                            "'{c}' llega hasta {hi} y el maximo esperado es {max}"
                        ));
                    }
                }
                Regla::Columnas(_) => {} // ya chequeado arriba
            }
        }

        if !fallas.is_empty() {
            anyhow::bail!(
                "{} no cumple lo esperado:\n  - {}",
                self.archivo,
                fallas.join("\n  - ")
            );
        }

        Ok(TaskOutput {
            rows: filas,
            message: format!("{filas} filas, {} chequeo(s) OK", self.reglas.len()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::write_parquet_rows;

    fn entorno() -> (tempfile::TempDir, DataEnv) {
        let t = tempfile::tempdir().unwrap();
        let e = DataEnv::new(t.path());
        (t, e)
    }

    fn ctx() -> TaskContext {
        TaskContext {
            run_id: uuid::Uuid::new_v4(),
            param: "p".into(),
            cancel: None,
        }
    }

    /// Un dataset chico y controlado: 4 filas, un null y un id repetido.
    fn escribir(env: &DataEnv, nombre: &str, df: DataFrame) {
        write_parquet_rows(df, &env.path(nombre)).unwrap();
    }

    fn df_sano() -> DataFrame {
        df!(
            "id" => [1i64, 2, 3, 4],
            "monto" => [10.0f64, 20.0, 30.0, 40.0],
        )
        .unwrap()
    }

    #[test]
    fn un_dataset_sano_pasa_todos_los_chequeos() {
        let (_t, env) = entorno();
        escribir(&env, "x.parquet", df_sano());
        let out = Chequeo::new("c", env, "x.parquet")
            .no_vacio()
            .columnas(["id", "monto"])
            .sin_nulos(["id", "monto"])
            .unicos("id")
            .rango("monto", 0.0, 100.0)
            .run(&ctx())
            .unwrap();
        assert_eq!(out.rows, 4);
        assert!(out.message.contains("OK"));
    }

    #[test]
    fn un_archivo_vacio_se_detecta() {
        let (_t, env) = entorno();
        escribir(
            &env,
            "v.parquet",
            df!("id" => Vec::<i64>::new(), "monto" => Vec::<f64>::new()).unwrap(),
        );
        let e = Chequeo::new("c", env, "v.parquet")
            .no_vacio()
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("0 filas"), "{e}");
        assert!(e.contains("al menos 1"), "{e}");
    }

    #[test]
    fn una_columna_que_falta_lo_dice_y_muestra_las_que_hay() {
        let (_t, env) = entorno();
        escribir(&env, "x.parquet", df_sano());
        let e = Chequeo::new("c", env, "x.parquet")
            .columnas(["id", "fecha_venta"])
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("fecha_venta"), "nombra la que falta: {e}");
        assert!(e.contains("monto"), "muestra las que si estan: {e}");
    }

    #[test]
    fn los_nulos_se_reportan_con_cantidad_y_porcentaje() {
        let (_t, env) = entorno();
        escribir(
            &env,
            "n.parquet",
            df!(
                "id" => [Some(1i64), None, Some(3), Some(4)],
                "monto" => [10.0f64, 20.0, 30.0, 40.0],
            )
            .unwrap(),
        );
        let e = Chequeo::new("c", env, "n.parquet")
            .sin_nulos(["id"])
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("'id'"), "{e}");
        assert!(e.contains("1 nulos"), "{e}");
        assert!(e.contains("25.0%"), "da el porcentaje: {e}");
    }

    #[test]
    fn un_id_repetido_rompe_la_unicidad() {
        let (_t, env) = entorno();
        escribir(
            &env,
            "d.parquet",
            df!("id" => [1i64, 2, 2, 4], "monto" => [1.0f64, 2.0, 3.0, 4.0]).unwrap(),
        );
        let e = Chequeo::new("c", env, "d.parquet")
            .unicos("id")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("'id'"), "{e}");
        assert!(e.contains("1 repetidos"), "{e}");
    }

    #[test]
    fn un_valor_fuera_de_rango_se_detecta_en_los_dos_extremos() {
        let (_t, env) = entorno();
        escribir(
            &env,
            "r.parquet",
            df!("id" => [1i64, 2], "monto" => [-5.0f64, 999.0]).unwrap(),
        );
        let e = Chequeo::new("c", env, "r.parquet")
            .rango("monto", 0.0, 100.0)
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("baja hasta -5"), "{e}");
        assert!(e.contains("llega hasta 999"), "{e}");
    }

    #[test]
    fn junta_todas_las_fallas_en_un_solo_mensaje() {
        // Enterarse de a un problema por corrida es lo que hace que arreglar
        // un pipeline lleve toda la tarde.
        let (_t, env) = entorno();
        escribir(
            &env,
            "m.parquet",
            df!(
                "id" => [Some(1i64), None, Some(1)],
                "monto" => [-1.0f64, 2.0, 3.0],
            )
            .unwrap(),
        );
        let e = Chequeo::new("c", env, "m.parquet")
            .filas_entre(10, 100)
            .sin_nulos(["id"])
            .unicos("id")
            .rango("monto", 0.0, 10.0)
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("3 filas"), "reporta el conteo: {e}");
        assert!(e.contains("nulos"), "reporta los nulos: {e}");
        assert!(e.contains("unica"), "reporta la unicidad: {e}");
        assert!(e.contains("baja hasta"), "reporta el rango: {e}");
        assert_eq!(
            e.matches("\n  - ").count(),
            4,
            "las cuatro, en una lista: {e}"
        );
    }

    #[test]
    fn un_archivo_que_no_existe_lo_dice_claro() {
        let (_t, env) = entorno();
        let e = Chequeo::new("c", env, "no-existe.parquet")
            .no_vacio()
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("no-existe.parquet"), "{e}");
    }
}

/// Chequeo contra la **historia del propio pipeline**, sin umbrales a mano.
///
/// Great Expectations, Soda y Deequ necesitan una base aparte para guardar
/// metricas y poder decir "esto se salio de lo normal". zeruel ya la tiene: cada
/// `task_runs` guarda las filas de cada tarea de cada run desde el dia uno.
///
/// Este chequeo compara lo de hoy contra el promedio historico de esa tarea.
/// El valor esperado no se escribe: **se aprende**. Un pipeline que suele traer
/// 9,8 M de filas y hoy trae 2 M no viola ninguna regla fija, pero si es una
/// caida del 80% respecto de su propia historia — y eso es casi siempre un
/// problema en el origen, no un dia flojo.
///
/// No chequea nada hasta tener [`ChequeoHistorico::minimo_corridas`] de
/// historia: con dos datos, un promedio no significa nada.
pub struct ChequeoHistorico {
    id: TaskId,
    store: std::sync::Arc<zeruel_core::Store>,
    tarea: String,
    desvio_pct: f64,
    minimo: i64,
}

impl ChequeoHistorico {
    /// Chequea el volumen de `tarea` contra su propia historia.
    pub fn new(
        id: impl Into<String>,
        store: std::sync::Arc<zeruel_core::Store>,
        tarea: impl Into<String>,
    ) -> Self {
        Self {
            id: TaskId(id.into()),
            store,
            tarea: tarea.into(),
            desvio_pct: 50.0,
            minimo: 5,
        }
    }

    /// Cuanto puede desviarse del promedio historico, en porcentaje.
    pub fn desvio_maximo_pct(mut self, pct: f64) -> Self {
        self.desvio_pct = pct;
        self
    }

    /// Cuantas corridas previas hacen falta para que el chequeo actue.
    pub fn minimo_corridas(mut self, n: i64) -> Self {
        self.minimo = n;
        self
    }
}

impl Task for ChequeoHistorico {
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    fn run(&self, ctx: &TaskContext) -> Result<TaskOutput> {
        let stats = self
            .store
            .task_stats()
            .map_err(|e| anyhow::anyhow!("no puedo leer el historico: {e}"))?;
        let Some(s) = stats.iter().find(|t| t.task == self.tarea) else {
            return Ok(TaskOutput {
                rows: 0,
                message: format!("sin historia de '{}' todavia: no chequeo nada", self.tarea),
            });
        };

        // Las filas de HOY salen del checkpoint que dejo la tarea en este run.
        let hoy = self
            .store
            .task_runs(ctx.run_id)
            .map_err(|e| anyhow::anyhow!("no puedo leer las tareas de este run: {e}"))?
            .into_iter()
            .find(|t| t.task == self.tarea)
            .map(|t| t.rows);

        let Some(hoy) = hoy else {
            anyhow::bail!(
                "la tarea '{}' no corrio en este run: no esta declarada como dependencia de este chequeo",
                self.tarea
            );
        };

        // El promedio ya incluye la corrida de hoy, asi que hace falta una mas
        // que el minimo para tener con que comparar de verdad.
        if s.ejecuciones <= self.minimo {
            return Ok(TaskOutput {
                rows: hoy,
                message: format!(
                    "{hoy} filas · solo {} corrida(s) de historia (hacen falta {}): no chequeo",
                    s.ejecuciones, self.minimo
                ),
            });
        }

        let prom = s.filas_prom;
        if prom <= 0.0 {
            return Ok(TaskOutput {
                rows: hoy,
                message: format!("{hoy} filas · el promedio historico es 0: no chequeo"),
            });
        }

        let desvio = 100.0 * (hoy as f64 - prom) / prom;
        if desvio.abs() > self.desvio_pct {
            anyhow::bail!(
                "'{}' trajo {hoy} filas y su promedio historico es {prom:.0} ({desvio:+.1}%, \
                 el maximo tolerado es mas/menos {:.0}%), sobre {} corridas anteriores. \
                 Un salto asi suele ser un problema en el origen, no un dia distinto.",
                self.tarea,
                self.desvio_pct,
                s.ejecuciones - 1
            );
        }

        Ok(TaskOutput {
            rows: hoy,
            message: format!(
                "{hoy} filas · {desvio:+.1}% vs el promedio de {} corridas: normal",
                s.ejecuciones - 1
            ),
        })
    }
}

#[cfg(test)]
mod tests_historico {
    use super::*;
    use std::sync::Arc;
    use zeruel_core::store::TaskStateRecord;
    use zeruel_sdk::TaskState;

    /// Deja `n` corridas historicas de `tarea` con `filas` filas cada una.
    fn historia(store: &Arc<zeruel_core::Store>, tarea: &str, filas: u64, n: usize) {
        for i in 0..n {
            let r = store.create_run("d", &format!("hist{i}")).unwrap();
            store
                .set_task_state(TaskStateRecord::medido(
                    r,
                    tarea,
                    &TaskState::Success,
                    filas,
                    "ok",
                    10,
                    1.0,
                ))
                .unwrap();
        }
    }

    /// Arma el run de hoy con `filas` y devuelve su contexto.
    fn hoy(store: &Arc<zeruel_core::Store>, tarea: &str, filas: u64) -> TaskContext {
        let r = store.create_run("d", "hoy").unwrap();
        store
            .set_task_state(TaskStateRecord::medido(
                r,
                tarea,
                &TaskState::Success,
                filas,
                "ok",
                10,
                1.0,
            ))
            .unwrap();
        TaskContext {
            run_id: r,
            param: "hoy".into(),
            cancel: None,
        }
    }

    #[test]
    fn un_volumen_normal_pasa() {
        let s = Arc::new(zeruel_core::Store::open_in_memory().unwrap());
        historia(&s, "validar", 1000, 10);
        let ctx = hoy(&s, "validar", 1050); // +5%
        let out = ChequeoHistorico::new("c", s, "validar")
            .desvio_maximo_pct(30.0)
            .run(&ctx)
            .unwrap();
        assert_eq!(out.rows, 1050);
        assert!(out.message.contains("normal"), "{}", out.message);
    }

    #[test]
    fn una_caida_fuerte_corta_el_dag_y_dice_cuanto() {
        let s = Arc::new(zeruel_core::Store::open_in_memory().unwrap());
        historia(&s, "validar", 1000, 10);
        let ctx = hoy(&s, "validar", 200); // -80%
        let e = ChequeoHistorico::new("c", s, "validar")
            .desvio_maximo_pct(30.0)
            .run(&ctx)
            .unwrap_err()
            .to_string();
        assert!(e.contains("200 filas"), "{e}");
        assert!(e.contains("origen"), "sugiere donde mirar: {e}");
    }

    #[test]
    fn un_pico_hacia_arriba_tambien_se_detecta() {
        // Duplicar los datos es tan sospechoso como perderlos: suele ser un
        // backfill que se corrio dos veces.
        let s = Arc::new(zeruel_core::Store::open_in_memory().unwrap());
        historia(&s, "validar", 1000, 10);
        let ctx = hoy(&s, "validar", 2000);
        assert!(ChequeoHistorico::new("c", s, "validar")
            .desvio_maximo_pct(30.0)
            .run(&ctx)
            .is_err());
    }

    #[test]
    fn sin_historia_suficiente_no_chequea_nada() {
        let s = Arc::new(zeruel_core::Store::open_in_memory().unwrap());
        historia(&s, "validar", 1000, 2);
        let ctx = hoy(&s, "validar", 10); // -99%, pero no hay con que comparar
        let out = ChequeoHistorico::new("c", s, "validar")
            .minimo_corridas(5)
            .run(&ctx)
            .unwrap();
        assert!(out.message.contains("no chequeo"), "{}", out.message);
    }

    #[test]
    fn una_tarea_que_no_corrio_en_este_run_lo_dice() {
        let s = Arc::new(zeruel_core::Store::open_in_memory().unwrap());
        historia(&s, "validar", 1000, 10);
        let ctx = hoy(&s, "otra_cosa", 100);
        let e = ChequeoHistorico::new("c", s, "validar")
            .run(&ctx)
            .unwrap_err()
            .to_string();
        assert!(e.contains("no corrio en este run"), "{e}");
        assert!(e.contains("dependencia"), "sugiere la causa: {e}");
    }
}
