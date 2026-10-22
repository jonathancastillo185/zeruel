//! Postgres como origen y como destino, **sin cargar la tabla en memoria**.
//!
//! Las dos tareas usan `COPY`, que es el camino de streaming de Postgres:
//!
//! ```text
//!   PostgresSource:  COPY (query) TO STDOUT   ──► archivo CSV ──► polars lo escanea lazy
//!   PostgresSink:    archivo CSV ──► COPY tabla FROM STDIN
//! ```
//!
//! Por que `COPY` y no un `SELECT` normal: un `SELECT` de 20 millones de filas
//! junta el resultado entero antes de devolverlo, y ahi se va el techo de RAM.
//! `COPY` va escupiendo bytes a medida que los produce, asi que el pico no
//! depende del tamaño de la tabla. Es tambien el camino mas rapido para cargar.
//!
//! **TLS**: esta pensado para un Postgres al que llegas sin TLS (local, en la
//! misma red o por tunel). Un Postgres administrado en la nube suele exigir TLS,
//! y eso necesita `postgres-native-tls` — no esta incluido todavia.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Instant;

use zeruel_sdk::{Task, TaskContext, TaskId, TaskOutput};

/// De donde sale la cadena de conexion.
///
/// Siempre del entorno: una cadena con usuario y clave escrita en el codigo
/// termina en el repositorio, en los logs y en los mensajes de error.
fn conexion(var: &str) -> anyhow::Result<String> {
    std::env::var(var).map_err(|_| {
        anyhow::anyhow!(
            "falta la variable de entorno {var} con la cadena de conexion \
             (ej. postgres://usuario:clave@host:5432/base)"
        )
    })
}

/// Reemplaza `{param}` por el parametro del run.
fn expandir(s: &str, param: &str) -> String {
    s.replace("{param}", param)
}

/// Lee de Postgres a un CSV, en streaming.
///
/// ```no_run
/// use zeruel_tasks::PostgresSource;
///
/// let t = PostgresSource::new(
///         "bajar_ventas",
///         "SELECT * FROM ventas WHERE dia = '{param}'",
///     )
///     .conexion_de_entorno("PG_URL")
///     .a_csv("data/ventas-{param}.csv");
/// ```
pub struct PostgresSource {
    id: TaskId,
    query: String,
    var_conexion: String,
    destino: PathBuf,
    min_filas: u64,
}

impl PostgresSource {
    /// Una tarea que corre `query` y escribe el resultado.
    pub fn new(id: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            id: TaskId(id.into()),
            query: query.into(),
            var_conexion: "PG_URL".into(),
            destino: PathBuf::from("salida.csv"),
            min_filas: 0,
        }
    }

    /// Variable de entorno con la cadena de conexion (por defecto `PG_URL`).
    pub fn conexion_de_entorno(mut self, var: impl Into<String>) -> Self {
        self.var_conexion = var.into();
        self
    }

    /// Archivo CSV donde escribir. Admite `{param}`.
    pub fn a_csv(mut self, p: impl Into<PathBuf>) -> Self {
        self.destino = p.into();
        self
    }

    /// Falla si la consulta trae menos filas que esto.
    ///
    /// Contra el fallo silencioso: la query corre bien pero el `WHERE` no
    /// matchea nada, y el pipeline sigue como si todo estuviera bien.
    pub fn minimo_filas(mut self, n: u64) -> Self {
        self.min_filas = n;
        self
    }
}

impl Task for PostgresSource {
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    fn run(&self, ctx: &TaskContext) -> anyhow::Result<TaskOutput> {
        let url = conexion(&self.var_conexion)?;
        let query = expandir(&self.query, &ctx.param);
        let destino = PathBuf::from(expandir(&self.destino.to_string_lossy(), &ctx.param));
        let t0 = Instant::now();

        let mut cliente = postgres::Client::connect(&url, postgres::NoTls).map_err(|e| {
            anyhow::anyhow!("no puedo conectar (variable {}): {e}", self.var_conexion)
        })?;

        if let Some(p) = destino.parent() {
            std::fs::create_dir_all(p).ok();
        }
        let mut salida = std::io::BufWriter::new(
            std::fs::File::create(&destino)
                .map_err(|e| anyhow::anyhow!("no puedo escribir {}: {e}", destino.display()))?,
        );

        // `COPY (...) TO STDOUT` va entregando el resultado a medida que lo
        // produce, en vez de juntarlo entero primero.
        let copia = format!("COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER)");
        let mut lector = cliente
            .copy_out(&copia)
            .map_err(|e| anyhow::anyhow!("la consulta fallo: {e}\n  query: {query}"))?;

        let mut buf = vec![0u8; 64 * 1024];
        let mut bytes = 0u64;
        loop {
            if ctx
                .cancel
                .as_ref()
                .is_some_and(|c| c.load(Ordering::SeqCst))
            {
                anyhow::bail!("cancelado tras traer {bytes} bytes");
            }
            let n = lector.read(&mut buf)?;
            if n == 0 {
                break;
            }
            salida.write_all(&buf[..n])?;
            bytes += n as u64;
        }
        salida.flush()?;
        drop(lector);

        let filas = contar_lineas(&destino)?.saturating_sub(1); // menos la cabecera
        if filas < self.min_filas {
            anyhow::bail!(
                "la consulta trajo {filas} filas y se esperaban al menos {}. \
                 Suele ser un WHERE que no matcheo nada.",
                self.min_filas
            );
        }

        Ok(TaskOutput {
            rows: filas,
            message: format!(
                "{filas} filas en {:.1}s -> {}",
                t0.elapsed().as_secs_f64(),
                destino.display()
            ),
        })
    }
}

/// Carga un CSV a una tabla de Postgres, en streaming.
///
/// ```no_run
/// use zeruel_tasks::PostgresSink;
///
/// let t = PostgresSink::new("cargar", "ventas_diarias")
///     .conexion_de_entorno("PG_URL")
///     .desde_csv("data/rollups-{param}.csv")
///     .borrar_antes("dia = '{param}'");
/// ```
pub struct PostgresSink {
    id: TaskId,
    tabla: String,
    var_conexion: String,
    origen: PathBuf,
    columnas: Vec<String>,
    borrar_where: Option<String>,
}

impl PostgresSink {
    /// Una tarea que carga a `tabla`.
    pub fn new(id: impl Into<String>, tabla: impl Into<String>) -> Self {
        Self {
            id: TaskId(id.into()),
            tabla: tabla.into(),
            var_conexion: "PG_URL".into(),
            origen: PathBuf::from("salida.csv"),
            columnas: Vec::new(),
            borrar_where: None,
        }
    }

    /// Variable de entorno con la cadena de conexion (por defecto `PG_URL`).
    pub fn conexion_de_entorno(mut self, var: impl Into<String>) -> Self {
        self.var_conexion = var.into();
        self
    }

    /// CSV de origen (con cabecera). Admite `{param}`.
    pub fn desde_csv(mut self, p: impl Into<PathBuf>) -> Self {
        self.origen = p.into();
        self
    }

    /// Columnas destino, si el CSV no trae todas o van en otro orden.
    pub fn columnas<I, S>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columnas = cols.into_iter().map(Into::into).collect();
        self
    }

    /// Borra las filas que matcheen antes de cargar, en la MISMA transaccion.
    ///
    /// Es lo que hace que re-correr un run sea seguro: sin esto, un backfill
    /// que se corre dos veces deja los datos duplicados. Admite `{param}`.
    pub fn borrar_antes(mut self, condicion: impl Into<String>) -> Self {
        self.borrar_where = Some(condicion.into());
        self
    }
}

impl Task for PostgresSink {
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    fn run(&self, ctx: &TaskContext) -> anyhow::Result<TaskOutput> {
        let url = conexion(&self.var_conexion)?;
        let origen = PathBuf::from(expandir(&self.origen.to_string_lossy(), &ctx.param));
        let t0 = Instant::now();

        if !origen.is_file() {
            anyhow::bail!("no existe {}: ¿corrio la tarea anterior?", origen.display());
        }

        let mut cliente = postgres::Client::connect(&url, postgres::NoTls).map_err(|e| {
            anyhow::anyhow!("no puedo conectar (variable {}): {e}", self.var_conexion)
        })?;

        // Borrado y carga en UNA transaccion: si la carga falla a mitad, no
        // queda la tabla vacia ni a medio llenar.
        let mut tx = cliente.transaction()?;

        let mut borradas = 0u64;
        if let Some(cond) = &self.borrar_where {
            let cond = expandir(cond, &ctx.param);
            let sql = format!("DELETE FROM {} WHERE {cond}", self.tabla);
            borradas = tx
                .execute(sql.as_str(), &[])
                .map_err(|e| anyhow::anyhow!("no pude borrar lo anterior: {e}\n  {sql}"))?;
        }

        let cols = if self.columnas.is_empty() {
            String::new()
        } else {
            format!(" ({})", self.columnas.join(", "))
        };
        let sql = format!(
            "COPY {}{cols} FROM STDIN WITH (FORMAT CSV, HEADER)",
            self.tabla
        );
        let mut escritor = tx
            .copy_in(sql.as_str())
            .map_err(|e| anyhow::anyhow!("no pude arrancar la carga: {e}\n  {sql}"))?;

        let mut archivo = std::io::BufReader::new(std::fs::File::open(&origen)?);
        let mut buf = vec![0u8; 64 * 1024];
        let mut bytes = 0u64;
        loop {
            if ctx
                .cancel
                .as_ref()
                .is_some_and(|c| c.load(Ordering::SeqCst))
            {
                anyhow::bail!("cancelado tras cargar {bytes} bytes (la transaccion se descarta)");
            }
            let n = archivo.read(&mut buf)?;
            if n == 0 {
                break;
            }
            escritor.write_all(&buf[..n])?;
            bytes += n as u64;
        }
        let filas = escritor
            .finish()
            .map_err(|e| anyhow::anyhow!("la carga fallo: {e}"))?;
        tx.commit()?;

        Ok(TaskOutput {
            rows: filas,
            message: format!(
                "{filas} filas a {} en {:.1}s{}",
                self.tabla,
                t0.elapsed().as_secs_f64(),
                if borradas > 0 {
                    format!(" ({borradas} borradas antes)")
                } else {
                    String::new()
                }
            ),
        })
    }
}

/// Cuenta lineas de un archivo sin cargarlo entero.
fn contar_lineas(p: &std::path::Path) -> anyhow::Result<u64> {
    use std::io::BufRead;
    let f = std::fs::File::open(p)?;
    Ok(std::io::BufReader::new(f).lines().count() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn ctx() -> TaskContext {
        TaskContext {
            run_id: uuid::Uuid::new_v4(),
            param: "2026-08-19".into(),
            cancel: Some(Arc::new(AtomicBool::new(false))),
        }
    }

    #[test]
    fn expande_el_param_en_query_y_destino() {
        assert_eq!(
            expandir("SELECT * FROM v WHERE dia = '{param}'", "2026-08-19"),
            "SELECT * FROM v WHERE dia = '2026-08-19'"
        );
        assert_eq!(
            expandir("data/v-{param}.csv", "2026-08-19"),
            "data/v-2026-08-19.csv"
        );
    }

    #[test]
    fn sin_la_variable_de_conexion_lo_dice_por_su_nombre() {
        std::env::remove_var("PG_URL_QUE_NO_EXISTE");
        let e = PostgresSource::new("t", "SELECT 1")
            .conexion_de_entorno("PG_URL_QUE_NO_EXISTE")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("PG_URL_QUE_NO_EXISTE"), "{e}");
        assert!(
            e.contains("postgres://"),
            "muestra el formato esperado: {e}"
        );
    }

    #[test]
    fn el_sink_avisa_si_falta_el_archivo_de_entrada() {
        std::env::set_var("PG_URL_TEST", "postgres://x:y@localhost:1/z");
        let e = PostgresSink::new("t", "tabla")
            .conexion_de_entorno("PG_URL_TEST")
            .desde_csv("no/existe/{param}.csv")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("no existe"), "{e}");
        assert!(e.contains("tarea anterior"), "sugiere la causa real: {e}");
        std::env::remove_var("PG_URL_TEST");
    }

    #[test]
    fn una_conexion_que_no_responde_no_cuelga_el_run() {
        // Puerto 1: nada escucha ahi. Tiene que fallar rapido y claro.
        std::env::set_var("PG_URL_MUERTA", "postgres://u:p@127.0.0.1:1/nada");
        let t0 = Instant::now();
        let e = PostgresSource::new("t", "SELECT 1")
            .conexion_de_entorno("PG_URL_MUERTA")
            .a_csv("x.csv")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("no puedo conectar"), "{e}");
        assert!(t0.elapsed().as_secs() < 30, "no se queda colgado");
        std::env::remove_var("PG_URL_MUERTA");
    }

    #[test]
    fn cuenta_lineas_sin_cargar_el_archivo() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("x.csv");
        std::fs::write(&p, "a,b\n1,2\n3,4\n").unwrap();
        assert_eq!(contar_lineas(&p).unwrap(), 3);
    }
}
