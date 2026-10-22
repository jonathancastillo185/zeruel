//! Configuracion de `zeruel`, en un archivo TOML.
//!
//! Hasta ahora todo se pasaba por flags, y `"state"` y `"data"` estaban
//! hardcodeados como default **quince veces** en `main.rs`. Eso funciona para
//! probar algo a mano; no para tener el mismo binario corriendo en tu maquina y
//! en un servidor con rutas, puertos y conexiones distintas.
//!
//! Lo que va en el archivo es **lo que cambia entre entornos**. La logica y el
//! grafo siguen en Rust: acá no hay nada que se ejecute.
//!
//! ```toml
//! # zeruel.toml
//! [zeruel]
//! state     = "/var/lib/zeruel"
//! pipelines = "/opt/zeruel/pipelines"
//! paralelo  = 4          # runs a la vez
//! workers   = 4          # tareas a la vez DENTRO de cada run
//! ram_max   = "2G"       # techo de memoria por run
//!
//! [web]
//! bind  = "127.0.0.1"
//! port  = 8787
//! token = "${ZERUEL_TOKEN}"
//!
//! # Variables que reciben los pipelines al ser lanzados. Es donde viven las
//! # conexiones de los conectores (Postgres, Snowflake, S3).
//! [entorno]
//! PG_URL            = "${PG_URL}"
//! SNOWFLAKE_ACCOUNT = "ab12345.sa-east-1"
//! AWS_REGION        = "sa-east-1"
//! ```
//!
//! # Secretos
//!
//! `${VAR}` se reemplaza por la variable de entorno de ese nombre. Es lo que
//! permite versionar el `zeruel.toml` sin meter una clave en el repositorio: el
//! archivo dice *de donde* sale el secreto, no cual es.
//!
//! Si la variable no existe, **falla al arrancar y dice cual falta**. Dejarlo
//! pasar significaria descubrirlo a mitad del primer run, con la conexion
//! rechazada y un mensaje que no explica nada.
//!
//! # Precedencia
//!
//! Gana lo mas explicito: **flag de CLI > variable de entorno > archivo >
//! default**. Asi se puede fijar todo en el archivo y pisar una cosa puntual
//! para una corrida sin editar nada.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

/// El archivo que se busca si no se pasa `--config`.
pub const NOMBRE_DEFAULT: &str = "zeruel.toml";

/// La configuracion completa.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub zeruel: Nodo,
    #[serde(default)]
    pub web: Web,
    /// Variables de entorno que reciben los pipelines al ser lanzados.
    #[serde(default)]
    pub entorno: BTreeMap<String, String>,
    /// De qué archivo salió esto. No viene del TOML: lo pone `cargar`.
    #[serde(skip)]
    origen: Option<PathBuf>,
}

/// Seccion `[zeruel]`: el scheduler.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Nodo {
    pub state: Option<PathBuf>,
    pub pipelines: Option<PathBuf>,
    pub paralelo: Option<usize>,
    pub workers: Option<usize>,
    /// Techo de memoria por run: `2G`, `512M`, o un numero en MB.
    pub ram_max: Option<String>,
    pub tick_ms: Option<u64>,
    pub timeout_secs: Option<u64>,
    /// Al apagar, segundos de espera para los runs en curso antes de cortarlos.
    pub gracia_secs: Option<u64>,
    /// Dias que se guarda el detalle por tarea (lo que mas pesa en la base).
    pub retencion_detalle_dias: Option<i64>,
    /// Dias que se guarda el listado de corridas.
    pub retencion_runs_dias: Option<i64>,
}

/// Seccion `[web]`: la UI.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Web {
    pub bind: Option<String>,
    pub port: Option<u16>,
    /// Token que hay que presentar para usar la UI. Sin esto, no pide nada.
    pub token: Option<String>,
}

impl Config {
    /// Carga el archivo. Si `ruta` es `None`, busca `zeruel.toml` en el directorio
    /// actual; si tampoco esta, devuelve la configuracion vacia (todo default).
    pub fn cargar(ruta: Option<&Path>) -> Result<Self> {
        let (p, obligatorio) = match ruta {
            Some(p) => (p.to_path_buf(), true),
            None => (PathBuf::from(NOMBRE_DEFAULT), false),
        };

        if !p.is_file() {
            if obligatorio {
                anyhow::bail!("no existe el archivo de configuracion {}", p.display());
            }
            return Ok(Self::default());
        }

        let texto = std::fs::read_to_string(&p)
            .with_context(|| format!("no puedo leer {}", p.display()))?;
        let texto = expandir_entorno(&texto, &p)?;
        let mut cfg: Config = toml::from_str(&texto)
            .with_context(|| format!("{} tiene un error de formato", p.display()))?;
        tracing::info!("configuracion cargada de {}", p.display());
        cfg.origen = Some(p);
        Ok(cfg)
    }

    /// Busca el `zeruel.toml` de una instalación, sin depender del directorio
    /// actual.
    ///
    /// Un servicio arranca desde `System32`, así que "buscar en el directorio
    /// actual" no encuentra nada, nunca. Acá se mira **al lado del ejecutable**,
    /// que es como queda una instalación normal, y sólo después se cae al
    /// directorio actual —que es lo correcto cuando se corre a mano.
    pub fn buscar_junto_al_ejecutable() -> Option<PathBuf> {
        let junto = std::env::current_exe()
            .ok()
            .map(|e| e.with_file_name(NOMBRE_DEFAULT))
            .filter(|p| p.is_file());
        junto.or_else(|| {
            let actual = PathBuf::from(NOMBRE_DEFAULT);
            actual.is_file().then_some(actual)
        })
    }

    /// ¿De qué archivo salió esta configuración? `None` = de fábrica.
    ///
    /// Se guarda para poder decirlo: un proceso que arrancó con los valores por
    /// defecto porque no encontró su archivo tiene que poder señalarlo, y sin
    /// esto no hay forma de distinguirlo de uno bien configurado.
    pub fn origen(&self) -> Option<&Path> {
        self.origen.as_deref()
    }

    /// Exporta las variables de `[entorno]` al proceso actual.
    ///
    /// Los subprocesos de pipeline las heredan, que es como llegan las cadenas
    /// de conexion a los conectores sin que cada pipeline lea la config.
    pub fn aplicar_entorno(&self) {
        for (k, v) in &self.entorno {
            std::env::set_var(k, v);
        }
        if !self.entorno.is_empty() {
            // Los NOMBRES si, los valores no: una clave no va nunca a un log.
            tracing::info!(
                "variables de entorno para los pipelines: {}",
                self.entorno.keys().cloned().collect::<Vec<_>>().join(", ")
            );
        }
    }
}

/// Reemplaza `${VAR}` por su valor en el entorno.
///
/// Falla nombrando la variable que falta, en vez de dejar el `${VAR}` literal:
/// una cadena de conexion con un `${PG_URL}` sin expandir termina en un error
/// de conexion que no dice nada sobre la causa real.
fn expandir_entorno(texto: &str, origen: &Path) -> Result<String> {
    let mut out = String::with_capacity(texto.len());
    let mut resto = texto;

    while let Some(i) = resto.find("${") {
        out.push_str(&resto[..i]);
        let desde = &resto[i + 2..];
        let Some(fin) = desde.find('}') else {
            anyhow::bail!(
                "{}: hay un `${{` sin cerrar. Se escribe `${{NOMBRE_DE_VARIABLE}}`",
                origen.display()
            );
        };
        let nombre = &desde[..fin];
        let valor = std::env::var(nombre).map_err(|_| {
            anyhow::anyhow!(
                "{} usa ${{{nombre}}} pero esa variable de entorno no esta definida. \
                 Definila antes de arrancar, o poné el valor literal en el archivo.",
                origen.display()
            )
        })?;
        out.push_str(&valor);
        resto = &desde[fin + 1..];
    }
    out.push_str(resto);
    Ok(out)
}

/// Elige el primer valor presente: flag de CLI, luego entorno, luego archivo,
/// luego el default.
///
/// `cli` viene como `Option` porque el flag tiene que poder estar ausente: si
/// clap le pone su default, no hay forma de saber si el usuario lo escribio o
/// no, y el archivo nunca ganaria.
pub fn elegir<T: Clone>(cli: Option<T>, entorno: Option<T>, archivo: Option<T>, def: T) -> T {
    cli.or(entorno).or(archivo).unwrap_or(def)
}

/// Lee una variable de entorno y la parsea.
pub fn de_entorno<T: std::str::FromStr>(var: &str) -> Option<T> {
    std::env::var(var).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn escribir(dir: &Path, contenido: &str) -> PathBuf {
        let p = dir.join("zeruel.toml");
        std::fs::write(&p, contenido).unwrap();
        p
    }

    #[test]
    fn sin_archivo_devuelve_todo_en_default() {
        let c = Config::cargar(None).unwrap();
        assert!(c.zeruel.state.is_none());
        assert!(c.entorno.is_empty());
    }

    #[test]
    fn un_archivo_pedido_que_no_existe_es_error() {
        let e = Config::cargar(Some(Path::new("no/existe/zeruel.toml")))
            .unwrap_err()
            .to_string();
        assert!(e.contains("no existe"), "{e}");
    }

    #[test]
    fn lee_las_secciones() {
        let t = tempfile::tempdir().unwrap();
        let p = escribir(
            t.path(),
            r#"
[zeruel]
state = "/var/lib/zeruel"
paralelo = 4
ram_max = "2G"

[web]
port = 9000

[entorno]
PG_URL = "postgres://x/y"
"#,
        );
        let c = Config::cargar(Some(&p)).unwrap();
        assert_eq!(c.zeruel.state.unwrap().to_string_lossy(), "/var/lib/zeruel");
        assert_eq!(c.zeruel.paralelo, Some(4));
        assert_eq!(c.zeruel.ram_max.as_deref(), Some("2G"));
        assert_eq!(c.web.port, Some(9000));
        assert_eq!(c.entorno.get("PG_URL").unwrap(), "postgres://x/y");
    }

    #[test]
    fn expande_las_variables_de_entorno() {
        std::env::set_var("ZERUEL_TEST_CLAVE", "secreta123");
        let t = tempfile::tempdir().unwrap();
        let p = escribir(
            t.path(),
            "[entorno]\nPG_URL = \"postgres://u:${ZERUEL_TEST_CLAVE}@host/db\"\n",
        );
        let c = Config::cargar(Some(&p)).unwrap();
        assert_eq!(
            c.entorno.get("PG_URL").unwrap(),
            "postgres://u:secreta123@host/db"
        );
        std::env::remove_var("ZERUEL_TEST_CLAVE");
    }

    #[test]
    fn una_variable_que_falta_falla_al_arrancar_y_la_nombra() {
        std::env::remove_var("ZERUEL_TEST_NO_EXISTE");
        let t = tempfile::tempdir().unwrap();
        let p = escribir(t.path(), "[entorno]\nX = \"${ZERUEL_TEST_NO_EXISTE}\"\n");
        let e = Config::cargar(Some(&p)).unwrap_err().to_string();
        assert!(e.contains("ZERUEL_TEST_NO_EXISTE"), "la nombra: {e}");
        assert!(e.contains("no esta definida"), "{e}");
    }

    #[test]
    fn un_dolar_sin_cerrar_lo_dice() {
        let t = tempfile::tempdir().unwrap();
        let p = escribir(t.path(), "[entorno]\nX = \"${SIN_CERRAR\"\n");
        let e = Config::cargar(Some(&p)).unwrap_err().to_string();
        assert!(e.contains("sin cerrar"), "{e}");
    }

    #[test]
    fn una_clave_mal_escrita_no_se_ignora_en_silencio() {
        // `deny_unknown_fields`: un `paralelos = 4` con una s de mas seria un
        // ajuste que el usuario cree que aplico y nunca aplico.
        let t = tempfile::tempdir().unwrap();
        let p = escribir(t.path(), "[zeruel]\nparalelos = 4\n");
        let e = Config::cargar(Some(&p)).unwrap_err().to_string();
        assert!(e.contains("error de formato"), "{e}");
    }

    #[test]
    fn la_precedencia_es_cli_entorno_archivo_default() {
        assert_eq!(elegir(Some(1), Some(2), Some(3), 4), 1, "manda la CLI");
        assert_eq!(elegir(None, Some(2), Some(3), 4), 2, "despues el entorno");
        assert_eq!(elegir(None, None, Some(3), 4), 3, "despues el archivo");
        assert_eq!(elegir(None, None, None, 4), 4, "por ultimo el default");
    }

    #[test]
    fn aplicar_entorno_exporta_las_variables() {
        let mut c = Config::default();
        c.entorno
            .insert("ZERUEL_TEST_APLICADA".into(), "valor".into());
        c.aplicar_entorno();
        assert_eq!(std::env::var("ZERUEL_TEST_APLICADA").unwrap(), "valor");
        std::env::remove_var("ZERUEL_TEST_APLICADA");
    }
}

#[cfg(test)]
mod tests_origen {
    use super::*;

    /// **El caso que costo dos dias.** Una configuracion que se cargo de un
    /// archivo tiene que poder decir de cual; una que salio de fabrica, que no
    /// hubo archivo.
    ///
    /// Sin esta distincion, un scheduler que arranco con `paralelo=2` porque no
    /// encontro su `zeruel.toml` se ve exactamente igual que uno donde el
    /// operador eligio 2. En produccion eso fue un tercio del rendimiento, sin
    /// techo de RAM y sin tope de tiempo, durante dos dias y sin una palabra.
    #[test]
    fn la_configuracion_dice_de_donde_salio() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("zeruel.toml");
        std::fs::write(&p, "[zeruel]\nparalelo = 6\n").unwrap();

        let cfg = Config::cargar(Some(&p)).unwrap();
        assert_eq!(cfg.zeruel.paralelo, Some(6));
        assert_eq!(cfg.origen(), Some(p.as_path()), "tiene que decir de cual");

        let vacia = Config::default();
        assert!(
            vacia.origen().is_none(),
            "una config de fabrica no puede aparentar tener archivo"
        );
    }

    /// Buscar el archivo NO puede depender del directorio actual: el de un
    /// servicio es System32, donde no hay ni va a haber ningun zeruel.toml.
    #[test]
    fn se_busca_al_lado_del_ejecutable_y_no_en_el_directorio_actual() {
        // El binario de test hace de "ejecutable instalado": se le pone un
        // zeruel.toml al lado y tiene que encontrarlo desde cualquier lado.
        let exe = std::env::current_exe().unwrap();
        let junto = exe.with_file_name(NOMBRE_DEFAULT);
        let habia = junto.exists();
        if !habia {
            std::fs::write(&junto, "[zeruel]\nparalelo = 9\n").unwrap();
        }

        let hallado = Config::buscar_junto_al_ejecutable();
        assert_eq!(
            hallado.as_deref(),
            Some(junto.as_path()),
            "tiene que mirar al lado del ejecutable"
        );
        let cfg = Config::cargar(hallado.as_deref()).unwrap();
        assert!(cfg.origen().is_some());

        if !habia {
            let _ = std::fs::remove_file(&junto);
        }
    }
}
