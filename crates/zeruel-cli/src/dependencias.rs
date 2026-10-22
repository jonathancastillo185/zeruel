//! Chequeo de las herramientas externas que van a usar los pipelines.
//!
//! # Por qué existe
//!
//! Un pipeline llama a programas que zeruel no conoce: un intérprete de Python,
//! `bun`, `dbt`, lo que sea. Si alguno no se puede ejecutar —no está, o la
//! cuenta del servicio no tiene permiso—, zeruel se entera **corrida por
//! corrida**, y cada fallo llega con el nombre del DAG, no con el del problema.
//!
//! Eso pasó en una instalación real y costó horas. La secuencia fue:
//!
//! 1. Falla un DAG. Se mira el DAG. No es el DAG.
//! 2. Se descubre que falta un permiso sobre `bun.exe`. Se concede.
//! 3. Vuelve a fallar, ahora por el Python del venv. Se concede.
//! 4. Vuelve a fallar: el venv es sólo un **lanzador** y delega en el Python
//!    base, al que nadie le dio permiso. Windows reporta esa falta de permiso
//!    como `No Python at '...'`, que se lee como "no está".
//!
//! Las dependencias se iban descubriendo **de a una, por error**. Ese bucle
//! sólo termina por suerte.
//!
//! # Qué hace
//!
//! Al arrancar, intenta ejecutar cada herramienta declarada y reporta **todas**
//! juntas. Es lo que convierte tres horas de búsqueda en un bloque de log.
//!
//! # Por qué las saca de `[entorno]`
//!
//! Porque ahí ya están, sin que nadie tenga que declararlas dos veces: la
//! sección `[entorno]` del `zeruel.toml` es donde el operador pone las rutas de
//! las herramientas, justamente para que los pipelines las encuentren. Toda
//! entrada cuyo valor sea un archivo que existe es candidata.
//!
//! # Por qué ejecutarlas y no sólo mirar si están
//!
//! Porque el caso que motivó esto **pasa el chequeo de existencia**: el
//! `python.exe` del venv estaba, era legible y tenía permiso. Lo que fallaba
//! era el eslabón siguiente, invisible desde afuera. La única forma de saber
//! que una herramienta sirve es usarla.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

/// Cómo le fue a una herramienta.
#[derive(Debug, Clone, PartialEq)]
pub enum Estado {
    /// Arrancó y terminó bien.
    Bien(String),
    /// Arrancó pero terminó con error. No siempre es un problema —hay programas
    /// que no entienden `--version`— pero el detalle se muestra igual, porque
    /// es exactamente donde apareció el fallo real que motivó este módulo.
    Dudoso { codigo: i32, detalle: String },
    /// Ni siquiera se pudo lanzar: no está, o no hay permiso.
    NoArranca(String),
}

/// El resultado para una variable de `[entorno]`.
#[derive(Debug, Clone)]
pub struct Chequeo {
    pub variable: String,
    pub programa: String,
    pub estado: Estado,
}

impl Chequeo {
    fn es_problema(&self) -> bool {
        !matches!(self.estado, Estado::Bien(_))
    }
}

/// Cuánto se espera a que la herramienta conteste.
///
/// Es una prueba de vida, no trabajo: si en cinco segundos no dijo su versión,
/// lo que importa —que se pudo lanzar— ya se sabe.
const TOPE: Duration = Duration::from_secs(5);

/// Las herramientas que se pueden probar, sacadas de `[entorno]`.
///
/// Se descartan las entradas cuyo valor no sea un archivo existente: la mayoría
/// de `[entorno]` son directorios, cadenas de conexión o claves, y probar una
/// clave no tendría sentido. También se descartan las que parezcan secretos,
/// para que su valor no llegue nunca a un log.
fn candidatas(entorno: &BTreeMap<String, String>) -> Vec<(String, String)> {
    entorno
        .iter()
        .filter(|(k, _)| {
            let k = k.to_uppercase();
            !k.contains("PASSWORD") && !k.contains("SECRET") && !k.contains("TOKEN")
        })
        .filter(|(_, v)| {
            let p = Path::new(v.as_str());
            p.is_file() && ejecutable(p)
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[cfg(windows)]
fn ejecutable(p: &Path) -> bool {
    // En Windows la extensión es lo que decide. Un `.toml` en `[entorno]` es
    // una ruta de datos, no una herramienta.
    matches!(
        p.extension()
            .and_then(|e| e.to_str())
            .map(str::to_lowercase)
            .as_deref(),
        Some("exe") | Some("bat") | Some("cmd") | Some("com")
    )
}

#[cfg(not(windows))]
fn ejecutable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p)
        .map(|m| m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

/// Prueba una herramienta ejecutándola.
fn probar(programa: &str) -> Estado {
    // `--version` es lo más parecido a un "no hagas nada y decime que estás".
    // Un programa que no lo entienda va a salir con error, y eso queda como
    // dudoso y no como fallo: lo que se estaba probando es que se pueda lanzar.
    let hijo = Command::new(programa)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();

    let mut hijo = match hijo {
        Ok(h) => h,
        Err(e) => return Estado::NoArranca(format!("{e}")),
    };

    // Sin tope, una herramienta que se queda esperando entrada colgaría el
    // arranque del scheduler, que es peor que no chequear nada.
    let t0 = std::time::Instant::now();
    loop {
        match hijo.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if t0.elapsed() > TOPE => {
                let _ = hijo.kill();
                let _ = hijo.wait();
                // Se pudo lanzar, que es lo que importa.
                return Estado::Bien("arranca (no contestó a --version)".into());
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(30)),
            Err(e) => return Estado::NoArranca(format!("{e}")),
        }
    }

    let salida = match hijo.wait_with_output() {
        Ok(s) => s,
        Err(e) => return Estado::NoArranca(format!("{e}")),
    };
    let texto = |b: &[u8]| {
        String::from_utf8_lossy(b)
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_string()
    };
    let codigo = salida.status.code().unwrap_or(-1);
    if salida.status.success() {
        let v = texto(&salida.stdout);
        Estado::Bien(if v.is_empty() { "arranca".into() } else { v })
    } else {
        let d = texto(&salida.stderr);
        Estado::Dudoso {
            codigo,
            detalle: if d.is_empty() {
                texto(&salida.stdout)
            } else {
                d
            },
        }
    }
}

/// Prueba todas las herramientas de `[entorno]`.
pub fn chequear(entorno: &BTreeMap<String, String>) -> Vec<Chequeo> {
    candidatas(entorno)
        .into_iter()
        .map(|(variable, programa)| {
            let estado = probar(&programa);
            Chequeo {
                variable,
                programa,
                estado,
            }
        })
        .collect()
}

/// Corre el chequeo y lo escribe en el log.
///
/// **No aborta el arranque.** Una herramienta rota tiene que gritar, no impedir
/// que el scheduler levante: los pipelines que no la usan tienen que seguir
/// corriendo, y un scheduler que no arranca es la falla más silenciosa de
/// todas.
pub fn chequear_y_reportar(entorno: &BTreeMap<String, String>) {
    let r = chequear(entorno);
    if r.is_empty() {
        return;
    }
    let problemas = r.iter().filter(|c| c.es_problema()).count();

    for c in &r {
        match &c.estado {
            Estado::Bien(v) => tracing::info!("  OK    {} = {} → {v}", c.variable, c.programa),
            Estado::Dudoso { codigo, detalle } => tracing::warn!(
                "  DUDA  {} = {} → salió con código {codigo}: {detalle}",
                c.variable,
                c.programa
            ),
            Estado::NoArranca(e) => tracing::error!(
                "  FALLA {} = {} → no se puede ejecutar: {e}",
                c.variable,
                c.programa
            ),
        }
    }
    if problemas > 0 {
        tracing::error!(
            "{problemas} de {} herramientas de [entorno] no responden como se espera. \
             Los pipelines que las usen van a fallar. Si esto corre como servicio, \
             lo más común es que su cuenta no tenga permiso sobre esa ruta.",
            r.len()
        );
    } else {
        tracing::info!("las {} herramientas de [entorno] responden bien", r.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entorno(pares: &[(&str, &str)]) -> BTreeMap<String, String> {
        pares
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Sólo se prueban las entradas que son un ejecutable de verdad. `[entorno]`
    /// tiene sobre todo directorios y cadenas de conexión.
    #[test]
    fn solo_se_prueban_los_ejecutables() {
        let exe = std::env::current_exe().unwrap();
        let e = entorno(&[
            ("UN_PROGRAMA", &exe.to_string_lossy()),
            ("UN_DIRECTORIO", &std::env::temp_dir().to_string_lossy()),
            ("UNA_CADENA", "postgres://localhost/db"),
            ("UNA_RUTA_QUE_NO_EXISTE", "C:/no-existe/x.exe"),
        ]);
        let c = candidatas(&e);
        assert_eq!(c.len(), 1, "solo el ejecutable: {c:?}");
        assert_eq!(c[0].0, "UN_PROGRAMA");
    }

    /// Un secreto no se prueba ni se nombra: su valor terminaría en un log.
    #[test]
    fn los_secretos_no_se_tocan() {
        let exe = std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .to_string();
        for clave in ["POSTGRES_PASSWORD", "API_TOKEN", "MI_SECRET"] {
            let e = entorno(&[(clave, &exe)]);
            assert!(
                candidatas(&e).is_empty(),
                "{clave} no puede entrar al chequeo"
            );
        }
    }

    /// **El caso que motivó el módulo**: una herramienta que no se puede
    /// ejecutar tiene que reportarse como tal, no pasar desapercibida.
    #[test]
    fn una_herramienta_que_no_esta_se_reporta() {
        assert!(matches!(
            probar("C:/no-existe-jamas/herramienta.exe"),
            Estado::NoArranca(_)
        ));
    }

    /// Y una que sí está tiene que dar por buena, sin importar si entiende
    /// `--version`: lo que se prueba es que se pueda lanzar.
    #[test]
    fn una_herramienta_que_esta_se_da_por_buena() {
        let exe = std::env::current_exe().unwrap();
        let r = probar(&exe.to_string_lossy());
        assert!(
            !matches!(r, Estado::NoArranca(_)),
            "el propio binario de test se puede lanzar: {r:?}"
        );
    }

    /// El chequeo no puede tardar para siempre si una herramienta se cuelga
    /// esperando entrada: colgaría el arranque del scheduler.
    #[test]
    fn el_chequeo_tiene_tope_de_tiempo() {
        let exe = std::env::current_exe().unwrap();
        let t0 = std::time::Instant::now();
        let _ = probar(&exe.to_string_lossy());
        assert!(
            t0.elapsed() < TOPE + Duration::from_secs(3),
            "tardó {:?}",
            t0.elapsed()
        );
    }
}
