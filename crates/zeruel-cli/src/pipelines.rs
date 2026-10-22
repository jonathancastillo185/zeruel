//! Descubrimiento y ejecucion de pipelines como **binarios independientes**.
//!
//! Un pipeline no es una entrada en un registro compilado: es un ejecutable
//! suelto en un directorio, que `zeruel serve` lanza como subproceso. Sumar uno
//! es compilar su crate y copiar el archivo; no hay que recompilar zeruel ni
//! reiniciar el scheduler, y un panic o un OOM se lleva puesto ese pipeline y
//! nada mas.
//!
//! El contrato con el binario (lo implementa el harness de la libreria `zeruel`,
//! asi que el autor del pipeline no escribe nada de esto):
//!
//! | Invocacion | Que hace |
//! |---|---|
//! | `<bin> --describe` | Imprime nombre, cron y grafo como JSON |
//! | `<bin> run --param X --state DIR --run-id UUID [--ram-max N]` | Ejecuta y checkpointea |
//!
//! ## La clave del contrato no se renombra a la ligera
//!
//! El JSON lleva `zeruel_contract`, y se acepta `nodo_contract` como alias
//! porque asi se llamaba antes del cambio de nombre del proyecto.
//!
//! Esto NO es nostalgia: la clave es el protocolo entre el scheduler y cada
//! binario de pipeline, y los pipelines se despliegan por separado, cada uno
//! con su propia version de la libreria compilada adentro. Cambiarla sin alias
//! significa que actualizar el scheduler deja de reconocer TODOS los pipelines
//! ya desplegados, de golpe y sin decir por que — que es exactamente lo que
//! paso al renombrar el proyecto, y como se descubrio que hacia falta el alias.
//!
//! Si algun dia cambia la forma, el scheduler
//! rechaza lo que no sabe leer en vez de adivinar.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::SystemTime;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Version maxima del contrato que este scheduler sabe hablar.
///
/// - **1**: un pipeline por binario.
/// - **2**: varios por binario (`pipelines: [...]` y `run --dag <nombre>`).
///
/// Se aceptan las dos: un binario viejo sigue funcionando sin recompilarlo.
pub const CONTRATO: u32 = 2;

/// Lo que un binario contesta a `--describe`.
///
/// Se acepta la forma del contrato 1 (un DAG suelto en la raiz) y la del 2
/// (`pipelines: [...]`). `serde(untagged)` prueba las dos.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum RespuestaDescribe {
    /// Contrato 2: varios pipelines.
    Bundle {
        #[serde(alias = "nodo_contract")]
        zeruel_contract: u32,
        pipelines: Vec<Describe>,
    },
    /// Contrato 1: uno solo, en la raiz.
    Suelto {
        #[serde(alias = "nodo_contract")]
        zeruel_contract: u32,
        #[serde(flatten)]
        dag: Describe,
    },
}

/// Un pipeline, tal como lo describe su binario.
#[derive(Clone, Debug, Deserialize)]
pub struct Describe {
    pub name: String,
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub max_retries: u32,
    #[serde(default)]
    pub tasks: Vec<TareaDesc>,
}

/// Un nodo del grafo, como lo publica el pipeline.
#[derive(Clone, Debug, Deserialize)]
pub struct TareaDesc {
    pub id: String,
    #[serde(default)]
    pub deps: Vec<String>,
    #[serde(default)]
    pub kind: Option<String>,
}

/// Un pipeline encontrado en el directorio.
#[derive(Clone, Debug)]
pub struct Pipeline {
    pub bin: PathBuf,
    pub desc: Describe,
    /// El binario expone mas de uno, asi que hay que decirle cual correr.
    pub necesita_dag: bool,
}

/// Registro de pipelines, con cache de `--describe` por fecha de modificacion.
///
/// Preguntarle a cada binario quien es cuesta un proceso (~170 ms), asi que no
/// se hace en cada tick: solo cuando el archivo cambio.
#[derive(Default)]
pub struct Registro {
    cache: HashMap<PathBuf, (SystemTime, Conocido)>,
}

/// Lo que ya se sabe de un binario.
///
/// Los rotos se recuerdan igual que los buenos, y por dos razones que se ven
/// recien en produccion: preguntarle a un binario cuesta un proceso (~170 ms),
/// y el aviso se repetiria en cada tick. Un solo pipeline roto llego a generar
/// **25 MB de log por dia** de la misma linea, mas un proceso por segundo que
/// no servia para nada.
#[derive(Clone, Debug)]
enum Conocido {
    Sirve(Vec<Describe>),
    /// No contesta, contesta cualquier cosa, o habla un contrato desconocido.
    Roto,
}

impl Registro {
    pub fn new() -> Self {
        Self::default()
    }

    /// Re-escanea el directorio. Devuelve los pipelines validos.
    ///
    /// Un binario que no contesta, contesta cualquier cosa, o habla un contrato
    /// que no conocemos, se saltea **con un warning y por su nombre** — no tira
    /// abajo el escaneo ni deja al resto sin correr.
    pub fn escanear(&mut self, dir: &Path) -> Vec<Pipeline> {
        let leidos = match std::fs::read_dir(dir) {
            Ok(r) => r,
            Err(e) => {
                // Que no exista es normal (todavia no desplegaron ninguno).
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        "no puedo leer el directorio de pipelines {}: {e}",
                        dir.display()
                    );
                }
                return Vec::new();
            }
        };

        let mut out = Vec::new();
        let mut vistos = Vec::new();
        for entrada in leidos.flatten() {
            let bin = entrada.path();
            if !es_ejecutable(&bin) {
                continue;
            }
            vistos.push(bin.clone());

            let mtime = entrada.metadata().and_then(|m| m.modified()).ok();
            if let (Some(m), Some((cm, conocido))) = (mtime, self.cache.get(&bin)) {
                if *cm == m {
                    // Vale tanto para los que sirven como para los rotos: si el
                    // archivo no cambio, no hay nada nuevo que preguntar. Basta
                    // con recompilarlo y volver a copiarlo para que se reintente.
                    if let Conocido::Sirve(ds) = conocido {
                        let varios = ds.len() > 1;
                        out.extend(ds.iter().cloned().map(|desc| Pipeline {
                            bin: bin.clone(),
                            desc,
                            necesita_dag: varios,
                        }));
                    }
                    continue;
                }
            }

            match describir(&bin) {
                Ok(ds) => {
                    tracing::info!(
                        "{} pipeline(s) en {}: {}",
                        ds.len(),
                        bin.display(),
                        recorte(
                            &ds.iter()
                                .map(|d| d.name.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    );
                    if let Some(m) = mtime {
                        self.cache
                            .insert(bin.clone(), (m, Conocido::Sirve(ds.clone())));
                    }
                    let varios = ds.len() > 1;
                    out.extend(ds.into_iter().map(|desc| Pipeline {
                        bin: bin.clone(),
                        desc,
                        necesita_dag: varios,
                    }));
                }
                Err(e) => {
                    // El aviso sale UNA vez por version del archivo, no una por
                    // tick. Repetido cada segundo deja de ser informacion y
                    // pasa a ser lo que llena el disco.
                    tracing::warn!("ignoro {}: {e:#}", bin.display());
                    if let Some(m) = mtime {
                        self.cache.insert(bin.clone(), (m, Conocido::Roto));
                    }
                }
            }
        }

        // Los que ya no estan salen del cache.
        self.cache.retain(|k, _| vistos.contains(k));
        out
    }
}

/// Un archivo que se puede ejecutar.
fn es_ejecutable(p: &Path) -> bool {
    if !p.is_file() {
        return false;
    }
    #[cfg(windows)]
    {
        p.extension().is_some_and(|e| e.eq_ignore_ascii_case("exe"))
    }
    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;
        p.metadata()
            .map(|m| m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
}

/// El codigo que el pipeline empotro al compilar.
///
/// Se le pregunta al binario, no al repositorio. Un pipeline desplegado puede
/// tener semanas mientras el repositorio siguio cambiando: mostrar el codigo de
/// HOY al lado de una corrida de hace tres dias es peor que no mostrar nada,
/// porque parece confiable.
pub fn fuente(p: &Pipeline) -> Result<serde_json::Value> {
    let mut cmd = Command::new(&p.bin);
    cmd.arg("--fuente");
    // Un bundle expone varios pipelines y cada uno empota su propio codigo:
    // hay que decirle cual. Sin esto contestaba por el primero, o fallaba.
    if p.necesita_dag {
        cmd.arg("--dag").arg(&p.desc.name);
    }
    let salida = cmd
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("no puedo ejecutar {}", p.bin.display()))?;

    if !salida.status.success() {
        // Un binario compilado antes de que existiera `--fuente` no lo conoce.
        // No es un error del sistema: es un pipeline que hay que recompilar.
        anyhow::bail!(
            "el binario no entiende `--fuente`: se compilo con una version de              zeruel anterior, o no llama a `.fuente(zeruel::fuente!())`"
        );
    }
    serde_json::from_slice(&salida.stdout).with_context(|| {
        format!(
            "`--fuente` no devolvio el JSON esperado: {}",
            recorte(&String::from_utf8_lossy(&salida.stdout))
        )
    })
}

/// Le pregunta al binario que pipelines tiene.
pub fn describir(bin: &Path) -> Result<Vec<Describe>> {
    let salida = Command::new(bin)
        .arg("--describe")
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("no puedo ejecutar {}", bin.display()))?;

    if !salida.status.success() {
        anyhow::bail!(
            "`--describe` fallo con codigo {}. ¿Es un pipeline de zeruel?",
            salida.status.code().unwrap_or(-1)
        );
    }

    let txt = String::from_utf8_lossy(&salida.stdout);
    let r: RespuestaDescribe = serde_json::from_str(txt.trim()).with_context(|| {
        format!(
            "`--describe` no devolvio el JSON esperado: {}",
            recorte(&txt)
        )
    })?;

    let (version, dags) = match r {
        RespuestaDescribe::Bundle {
            zeruel_contract,
            pipelines,
        } => (zeruel_contract, pipelines),
        RespuestaDescribe::Suelto {
            zeruel_contract,
            dag,
        } => (zeruel_contract, vec![dag]),
    };

    if version > CONTRATO {
        anyhow::bail!(
            "habla el contrato {version} y este scheduler entiende hasta el {CONTRATO}.              Actualiza zeruel, o recompila el pipeline contra esta version."
        );
    }
    if dags.is_empty() {
        anyhow::bail!("`--describe` no devolvio ningun pipeline");
    }
    if let Some(sin_nombre) = dags.iter().position(|d| d.name.trim().is_empty()) {
        anyhow::bail!("el pipeline #{sin_nombre} de `--describe` no trae nombre");
    }
    Ok(dags)
}

fn recorte(s: &str) -> String {
    let s = s.trim();
    if s.len() > 120 {
        format!("{}...", &s[..120])
    } else if s.is_empty() {
        "(vacio)".into()
    } else {
        s.to_string()
    }
}

/// Un run corriendo como subproceso.
pub struct EnCurso {
    pub child: Child,
    pub queue_id: i64,
    pub run_id: uuid::Uuid,
    pub dag: String,
    pub param: String,
    pub desde: std::time::Instant,
    pub log: PathBuf,
    /// Ya le pedimos que se muera (por cancelacion o timeout).
    pub matado: bool,
}

/// Lanza el pipeline como subproceso.
///
/// La salida (stdout y stderr) va a un archivo por run, en vez de a un pipe:
/// un pipe que nadie vacia se llena y **bloquea al hijo** a mitad del run. Y de
/// paso queda un log por ejecucion, que es lo que la UI necesita mostrar.
pub fn lanzar(
    p: &Pipeline,
    param: &str,
    state: &Path,
    run_id: uuid::Uuid,
    queue_id: i64,
    workers: usize,
    techo_mb: u64,
) -> Result<EnCurso> {
    let dir_logs = state.join("logs");
    std::fs::create_dir_all(&dir_logs).ok();
    let log = dir_logs.join(format!("run-{run_id}.log"));
    let archivo =
        std::fs::File::create(&log).with_context(|| format!("no puedo crear {}", log.display()))?;
    let err = archivo
        .try_clone()
        .context("no puedo duplicar el archivo de log")?;

    let mut cmd = Command::new(&p.bin);
    cmd.arg("run");
    if p.necesita_dag {
        cmd.arg("--dag").arg(&p.desc.name);
    }
    cmd.arg("--param")
        .arg(param)
        .arg("--state")
        .arg(state)
        .arg("--workers")
        .arg(workers.to_string())
        .arg("--run-id")
        .arg(run_id.to_string());
    // El techo va por argumento y no por variable de entorno: queda a la vista
    // en el log y en el arbol de procesos, que es donde se lo busca cuando un
    // run muere por memoria.
    if techo_mb > 0 {
        cmd.arg("--ram-max").arg(format!("{techo_mb}M"));
    }
    let child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::from(archivo))
        .stderr(Stdio::from(err))
        .spawn()
        .with_context(|| format!("no puedo lanzar {}", p.bin.display()))?;

    Ok(EnCurso {
        child,
        queue_id,
        run_id,
        dag: p.desc.name.clone(),
        param: param.to_string(),
        desde: std::time::Instant::now(),
        log,
        matado: false,
    })
}

/// El grafo de un pipeline, en el formato que ya consumen la UI y el MCP.
pub fn grafo_json(d: &Describe) -> serde_json::Value {
    let nodes: Vec<_> = d
        .tasks
        .iter()
        .map(|t| serde_json::json!({"id": t.id, "kind": t.kind}))
        .collect();
    let edges: Vec<_> = d
        .tasks
        .iter()
        .flat_map(|t| {
            t.deps
                .iter()
                .map(move |dep| serde_json::json!({"from": dep, "to": t.id}))
        })
        .collect();
    serde_json::json!({
        "name": d.name,
        "schedule": d.schedule,
        "max_retries": d.max_retries,
        "nodes": nodes,
        "edges": edges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desc_json(contrato: u32, nombre: &str) -> String {
        format!(
            r#"{{"zeruel_contract":{contrato},"name":"{nombre}","schedule":"0 6 * * *","max_retries":3,
                "tasks":[{{"id":"a","deps":[],"kind":"rust"}},{{"id":"b","deps":["a"],"kind":"rust"}}]}}"#
        )
    }

    #[test]
    fn parsea_un_describe_valido() {
        let d: Describe = serde_json::from_str(&desc_json(1, "ventas")).unwrap();
        assert_eq!(d.name, "ventas");
        assert_eq!(d.tasks.len(), 2);
        assert_eq!(d.tasks[1].deps, vec!["a"]);
        assert_eq!(d.schedule.as_deref(), Some("0 6 * * *"));
    }

    #[test]
    fn el_grafo_sale_del_describe() {
        let d: Describe = serde_json::from_str(&desc_json(1, "ventas")).unwrap();
        let g = grafo_json(&d);
        assert_eq!(g["name"], "ventas");
        assert_eq!(g["nodes"].as_array().unwrap().len(), 2);
        let e = &g["edges"].as_array().unwrap()[0];
        assert_eq!(e["from"], "a");
        assert_eq!(e["to"], "b");
    }

    #[test]
    fn un_describe_sin_tareas_igual_es_valido() {
        // Un pipeline recien creado, sin tareas todavia: no es un error.
        let d: Describe = serde_json::from_str(r#"{"zeruel_contract":1,"name":"vacio"}"#).unwrap();
        assert!(d.tasks.is_empty());
        assert!(d.schedule.is_none());
    }

    #[test]
    fn un_pipeline_compilado_antes_del_rename_sigue_andando() {
        // La clave del contrato se llamaba `nodo_contract`. Los binarios de
        // pipeline se despliegan por separado y traen su propia copia de la
        // libreria, asi que actualizar el scheduler no los recompila: si el
        // alias se cae, todos dejan de reconocerse a la vez.
        let viejo = r#"{"nodo_contract":2,"pipelines":[
            {"name":"lento","schedule":null,"max_retries":3,
             "tasks":[{"id":"preparar","deps":[],"kind":"rust"}]}]}"#;
        let r: RespuestaDescribe = serde_json::from_str(viejo).expect("tiene que parsear");
        match r {
            RespuestaDescribe::Bundle {
                zeruel_contract,
                pipelines,
            } => {
                assert_eq!(zeruel_contract, 2);
                assert_eq!(pipelines.len(), 1);
                assert_eq!(pipelines[0].name, "lento");
            }
            _ => panic!("un bundle del contrato 2 no puede leerse como suelto"),
        }
    }

    #[test]
    fn escanear_un_directorio_que_no_existe_no_es_error() {
        let mut r = Registro::new();
        assert!(r.escanear(Path::new("no/existe/pipelines")).is_empty());
    }

    #[test]
    fn a_un_binario_roto_se_le_pregunta_una_sola_vez() {
        // Un pipeline roto llego a generar 25 MB de log por dia repitiendo el
        // mismo aviso en cada tick, mas un proceso por segundo preguntandole lo
        // mismo. El escaneo tiene que recordarlo igual que a los que sirven.
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp
            .path()
            .join(if cfg!(windows) { "roto.exe" } else { "roto" });
        std::fs::write(&bin, b"esto no es un ejecutable valido").unwrap();
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let mut r = Registro::new();
        assert!(r.escanear(tmp.path()).is_empty());
        assert_eq!(r.cache.len(), 1, "el roto tiene que quedar en el cache");
        assert!(matches!(r.cache.values().next().unwrap().1, Conocido::Roto));

        // Segundo escaneo: sigue sin devolverlo, y sin volver a preguntarle.
        assert!(r.escanear(tmp.path()).is_empty());
        assert_eq!(r.cache.len(), 1);
    }

    #[test]
    fn escanear_ignora_lo_que_no_es_ejecutable() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("notas.txt"), "hola").unwrap();
        std::fs::write(tmp.path().join("datos.json"), "{}").unwrap();
        let mut r = Registro::new();
        assert!(r.escanear(tmp.path()).is_empty());
    }

    #[test]
    fn recorta_una_salida_larga_para_el_mensaje() {
        assert_eq!(recorte("  "), "(vacio)");
        assert_eq!(recorte(" ok "), "ok");
        let largo = "x".repeat(300);
        let r = recorte(&largo);
        assert!(r.len() < 130 && r.ends_with("..."));
    }
}
