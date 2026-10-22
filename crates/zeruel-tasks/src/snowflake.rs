//! Snowflake, por el camino que Snowflake mismo recomienda para volumen.
//!
//! No hay driver oficial de Snowflake para Rust. Los caminos posibles son tres,
//! y este modulo toma el tercero a proposito:
//!
//! | Camino | Que tiene a favor | Por que no |
//! |---|---|---|
//! | ADBC | Devuelve Arrow nativo, cero copias hacia polars | Es una libreria nativa por FFI: rompe la promesa de un binario sin dependencias |
//! | Un crate no oficial | Simple de usar | Sin mantenimiento garantizado, y ninguno hace `COPY` bien |
//! | **Staging + `COPY INTO`, por la API REST** | Es lo que Snowflake recomienda para volumen; sin dependencias nativas | Necesita un bucket de por medio |
//!
//! El flujo para cargar:
//!
//! ```text
//!   parquet local ──► (subir a S3) ──► COPY INTO tabla FROM @stage
//!                                       └─ una sola llamada REST
//! ```
//!
//! Lo pesado —mover los datos— lo hace S3, que ya sabemos hacer con el feature
//! `cloud`. Snowflake solo recibe una orden. Asi el pico de RAM no depende del
//! tamaño de la carga, y no hay que compilar un driver.
//!
//! **Lo que este modulo NO hace todavia**: leer masivamente de Snowflake. Para
//! eso el camino bueno es ADBC, y trae la contra de arriba. Hoy alcanza para
//! ejecutar sentencias y para cargar, que es el 90% de un ETL que termina en un
//! data warehouse.

use std::time::{Duration, Instant};

use zeruel_sdk::{Task, TaskContext, TaskId, TaskOutput};

/// Reemplaza `{param}` por el parametro del run.
fn expandir(s: &str, param: &str) -> String {
    s.replace("{param}", param)
}

/// Ejecuta una sentencia en Snowflake por su API REST (SQL API v2).
///
/// Sirve para el `COPY INTO` que carga desde un stage, y para cualquier DDL o
/// DML del pipeline (crear una tabla temporal, hacer un `MERGE`, borrar una
/// particion antes de recargarla).
///
/// ```no_run
/// use zeruel_tasks::SnowflakeStatement;
///
/// let t = SnowflakeStatement::new(
///         "cargar",
///         "COPY INTO ventas FROM @mi_stage/ventas-{param}.parquet \
///          FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
///     )
///     .cuenta_de_entorno("SNOWFLAKE_ACCOUNT")
///     .token_de_entorno("SNOWFLAKE_TOKEN")
///     .warehouse("COMPUTE_WH")
///     .base("ANALITICA")
///     .esquema("PUBLIC");
/// ```
///
/// La autenticacion es por token (OAuth o key-pair ya canjeado). Nunca va en el
/// codigo: sale del entorno.
pub struct SnowflakeStatement {
    id: TaskId,
    sql: String,
    var_cuenta: String,
    var_token: String,
    warehouse: Option<String>,
    base: Option<String>,
    esquema: Option<String>,
    rol: Option<String>,
    timeout: Duration,
}

impl SnowflakeStatement {
    /// Una tarea que ejecuta `sql`.
    pub fn new(id: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            id: TaskId(id.into()),
            sql: sql.into(),
            var_cuenta: "SNOWFLAKE_ACCOUNT".into(),
            var_token: "SNOWFLAKE_TOKEN".into(),
            warehouse: None,
            base: None,
            esquema: None,
            rol: None,
            timeout: Duration::from_secs(900),
        }
    }

    /// Variable de entorno con el identificador de cuenta (ej. `ab12345.sa-east-1`).
    pub fn cuenta_de_entorno(mut self, var: impl Into<String>) -> Self {
        self.var_cuenta = var.into();
        self
    }

    /// Variable de entorno con el token de acceso.
    pub fn token_de_entorno(mut self, var: impl Into<String>) -> Self {
        self.var_token = var.into();
        self
    }

    /// Warehouse a usar.
    pub fn warehouse(mut self, w: impl Into<String>) -> Self {
        self.warehouse = Some(w.into());
        self
    }

    /// Base de datos.
    pub fn base(mut self, b: impl Into<String>) -> Self {
        self.base = Some(b.into());
        self
    }

    /// Esquema.
    pub fn esquema(mut self, e: impl Into<String>) -> Self {
        self.esquema = Some(e.into());
        self
    }

    /// Rol.
    pub fn rol(mut self, r: impl Into<String>) -> Self {
        self.rol = Some(r.into());
        self
    }

    /// Tope de tiempo (por defecto 15 minutos: un `COPY INTO` grande tarda).
    pub fn timeout_secs(mut self, s: u64) -> Self {
        self.timeout = Duration::from_secs(s);
        self
    }

    /// La URL de la SQL API para esta cuenta.
    fn url(cuenta: &str) -> String {
        format!("https://{cuenta}.snowflakecomputing.com/api/v2/statements")
    }
}

impl Task for SnowflakeStatement {
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    fn run(&self, ctx: &TaskContext) -> anyhow::Result<TaskOutput> {
        let cuenta = std::env::var(&self.var_cuenta).map_err(|_| {
            anyhow::anyhow!(
                "falta la variable de entorno {} con el identificador de cuenta \
                 (ej. ab12345.sa-east-1)",
                self.var_cuenta
            )
        })?;
        let token = std::env::var(&self.var_token).map_err(|_| {
            anyhow::anyhow!(
                "falta la variable de entorno {} con el token de acceso",
                self.var_token
            )
        })?;

        let sql = expandir(&self.sql, &ctx.param);
        let t0 = Instant::now();

        let mut cuerpo = serde_json::json!({
            "statement": sql,
            "timeout": self.timeout.as_secs(),
        });
        for (clave, valor) in [
            ("warehouse", &self.warehouse),
            ("database", &self.base),
            ("schema", &self.esquema),
            ("role", &self.rol),
        ] {
            if let Some(v) = valor {
                cuerpo[clave] = serde_json::Value::String(v.clone());
            }
        }

        let agente = ureq::Agent::config_builder()
            .timeout_global(Some(self.timeout + Duration::from_secs(30)))
            .build()
            .new_agent();

        let respuesta = agente
            .post(Self::url(&cuenta))
            .header("Authorization", &format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            // Snowflake lo pide para saber que tipo de token es.
            .header("X-Snowflake-Authorization-Token-Type", "OAUTH")
            .send(cuerpo.to_string());

        let mut respuesta = match respuesta {
            Ok(r) => r,
            Err(ureq::Error::StatusCode(code)) => anyhow::bail!(
                "Snowflake devolvio HTTP {code}. \
                 401/403 suele ser el token vencido; 400, la sentencia."
            ),
            Err(e) => anyhow::bail!("no puedo hablar con Snowflake: {e}"),
        };

        let texto = respuesta.body_mut().read_to_string()?;
        let json: serde_json::Value = serde_json::from_str(&texto)
            .map_err(|e| anyhow::anyhow!("Snowflake devolvio algo que no es JSON: {e}"))?;

        // Una sentencia asincronica devuelve 202 y un handle. No se espera en
        // el mismo request: se avisa, porque tragarselo dejaria al pipeline
        // creyendo que la carga termino cuando recien empezo.
        if let Some(handle) = json.get("statementHandle").and_then(|v| v.as_str()) {
            if json.get("data").is_none() && json.get("stats").is_none() {
                anyhow::bail!(
                    "Snowflake acepto la sentencia pero la esta ejecutando en segundo plano \
                     (handle {handle}). Subi el timeout para que termine dentro del request."
                );
            }
        }

        // Cuantas filas movio: lo que informa el `COPY INTO`.
        let filas = json
            .get("stats")
            .and_then(|s| s.get("numRowsInserted"))
            .and_then(|v| v.as_u64())
            .or_else(|| {
                json.get("resultSetMetaData")
                    .and_then(|m| m.get("numRows"))
                    .and_then(|v| v.as_u64())
            })
            .unwrap_or(0);

        Ok(TaskOutput {
            rows: filas,
            message: format!(
                "Snowflake ok en {:.1}s{}",
                t0.elapsed().as_secs_f64(),
                if filas > 0 {
                    format!(" ({filas} filas)")
                } else {
                    String::new()
                }
            ),
        })
    }
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
    fn arma_bien_la_url_de_la_cuenta() {
        assert_eq!(
            SnowflakeStatement::url("ab12345.sa-east-1"),
            "https://ab12345.sa-east-1.snowflakecomputing.com/api/v2/statements"
        );
    }

    #[test]
    fn expande_el_param_en_la_sentencia() {
        assert_eq!(
            expandir("COPY INTO v FROM @s/v-{param}.parquet", "2026-08-19"),
            "COPY INTO v FROM @s/v-2026-08-19.parquet"
        );
    }

    #[test]
    fn sin_cuenta_lo_dice_por_su_nombre() {
        std::env::remove_var("SNOW_CUENTA_NO_EXISTE");
        let e = SnowflakeStatement::new("t", "SELECT 1")
            .cuenta_de_entorno("SNOW_CUENTA_NO_EXISTE")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("SNOW_CUENTA_NO_EXISTE"), "{e}");
        assert!(e.contains("ab12345"), "muestra el formato esperado: {e}");
    }

    #[test]
    fn sin_token_lo_dice_por_su_nombre() {
        std::env::set_var("SNOW_CUENTA_T", "ab12345.sa-east-1");
        std::env::remove_var("SNOW_TOKEN_NO_EXISTE");
        let e = SnowflakeStatement::new("t", "SELECT 1")
            .cuenta_de_entorno("SNOW_CUENTA_T")
            .token_de_entorno("SNOW_TOKEN_NO_EXISTE")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("SNOW_TOKEN_NO_EXISTE"), "{e}");
        std::env::remove_var("SNOW_CUENTA_T");
    }
}
