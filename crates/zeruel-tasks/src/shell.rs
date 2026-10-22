//! `ShellTask`: correr un comando externo como una tarea del grafo.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use zeruel_sdk::{Task, TaskContext, TaskId, TaskOutput};

/// Prefijo que un comando puede imprimir para reportarle filas a zeruel.
///
/// Una linea `zeruel:rows=1234` en la salida hace que la tarea reporte 1234
/// filas, que es lo que despues se ve en `zeruel status`, en la UI y en las
/// metricas historicas. Sin eso la tarea reporta 0, que es honesto: zeruel no
/// tiene forma de saber cuanto proceso un programa ajeno.
pub const PREFIJO_FILAS: &str = "zeruel:rows=";

/// Corre un comando externo como una tarea del DAG.
///
/// Que resuelve, mas alla de lanzar el proceso:
///
/// - **Se puede cancelar.** Mira `ctx.cancel` mientras corre y mata al proceso
///   si alguien pide cancelar desde la UI, la CLI o el MCP.
/// - **Tiene tope de tiempo** opcional, para que un comando colgado no ocupe
///   un cupo del scheduler para siempre.
/// - **La salida va al log** linea por linea, en vivo, en vez de aparecer toda
///   junta al final. Y stdout/stderr se leen en hilos aparte: un pipe que nadie
///   vacia se llena y **bloquea al proceso hijo** a mitad de camino.
/// - **El error dice que paso**: codigo de salida y las ultimas lineas de la
///   salida, que es lo que se necesita para entender por que fallo.
pub struct ShellTask {
    id: TaskId,
    programa: String,
    args: Vec<String>,
    cwd: Option<PathBuf>,
    env: Vec<(String, String)>,
    timeout: Option<Duration>,
    codigos_ok: Vec<i32>,
    pasar_param: bool,
}

impl ShellTask {
    /// Una tarea que corre `programa`.
    pub fn new(id: impl Into<String>, programa: impl Into<String>) -> Self {
        Self {
            id: TaskId(id.into()),
            programa: programa.into(),
            args: Vec::new(),
            cwd: None,
            env: Vec::new(),
            timeout: None,
            codigos_ok: vec![0],
            pasar_param: false,
        }
    }

    /// Argumentos del comando.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Directorio de trabajo.
    pub fn cwd(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cwd = Some(dir.into());
        self
    }

    /// Una variable de entorno para el proceso hijo.
    pub fn env(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.env.push((k.into(), v.into()));
        self
    }

    /// Corta el comando si pasa de estos segundos.
    pub fn timeout_secs(mut self, s: u64) -> Self {
        self.timeout = Some(Duration::from_secs(s));
        self
    }

    /// Codigos de salida que cuentan como exito (por defecto, solo el 0).
    ///
    /// Hay comandos que usan otros codigos para decir "no habia nada que hacer",
    /// que no es un fallo.
    pub fn codigos_ok(mut self, codigos: impl IntoIterator<Item = i32>) -> Self {
        self.codigos_ok = codigos.into_iter().collect();
        self
    }

    /// Agrega el `param` del run como ultimo argumento.
    ///
    /// Para comandos que reciben la fecha del run: `--dia 2026-08-19`.
    pub fn con_param(mut self) -> Self {
        self.pasar_param = true;
        self
    }
}

/// Mata al hijo **y a sus descendientes**, y espera a que termine.
///
/// `Child::kill` en Windows es `TerminateProcess` sobre ese proceso solo: los
/// que él lanzó siguen vivos. Y el caso normal tiene nietos: el `python.exe` de
/// un venv de Windows es un lanzador que arranca al intérprete real como hijo,
/// así que matar al lanzador dejaba al Python real corriendo, huérfano, mientras
/// el reintento de la tarea arrancaba otro encima. `taskkill /T` recorre el árbol.
///
/// En Unix se mantiene `kill` directo: ahí el hijo suele ser el proceso real.
fn matar_arbol(hijo: &mut std::process::Child) {
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &hijo.id().to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    let _ = hijo.kill(); // por si taskkill no estaba o no alcanzó
    let _ = hijo.wait();
}

impl Task for ShellTask {
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    fn run(&self, ctx: &TaskContext) -> anyhow::Result<TaskOutput> {
        let mut cmd = Command::new(&self.programa);
        cmd.args(&self.args);
        if self.pasar_param {
            cmd.arg(&ctx.param);
        }
        if let Some(d) = &self.cwd {
            cmd.current_dir(d);
        }
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        // El param siempre disponible por entorno, aunque no se pase por CLI.
        cmd.env("ZERUEL_PARAM", &ctx.param);
        cmd.env("ZERUEL_RUN_ID", ctx.run_id.to_string());

        let mut hijo = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| self.explicar_fallo_al_lanzar(e))?;

        // Los pipes se leen en hilos: si nadie los vacia, se llenan y el hijo
        // queda bloqueado escribiendo.
        let id = self.id.0.clone();
        let ultimas = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let filas = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        let mut lectores = Vec::new();
        if let Some(out) = hijo.stdout.take() {
            lectores.push(leer(out, id.clone(), false, ultimas.clone(), filas.clone()));
        }
        if let Some(err) = hijo.stderr.take() {
            lectores.push(leer(err, id.clone(), true, ultimas.clone(), filas.clone()));
        }

        // Espera activa: cada 100 ms mira si termino, si lo cancelaron o si se
        // paso del tope.
        let t0 = Instant::now();
        let estado = loop {
            if let Some(e) = hijo.try_wait()? {
                break e;
            }
            if ctx
                .cancel
                .as_ref()
                .is_some_and(|c| c.load(Ordering::SeqCst))
            {
                matar_arbol(&mut hijo);
                anyhow::bail!("cancelado a los {:.1}s", t0.elapsed().as_secs_f64());
            }
            if let Some(tope) = self.timeout {
                if t0.elapsed() > tope {
                    matar_arbol(&mut hijo);
                    anyhow::bail!(
                        "supero el tope de {}s (iba por {:.1}s)",
                        tope.as_secs(),
                        t0.elapsed().as_secs_f64()
                    );
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        };

        // El pico del hijo, leido de su handle ANTES de soltarlo.
        //
        // Es la unica medicion exacta que se puede hacer de un proceso que ya
        // termino, y hace falta porque el muestreo del arbol no llega a verlo
        // si vivio menos de medio segundo: python reserva 300 MB y sale en
        // 0,12 s, que es la forma normal de una tarea de ETL.
        zeruel_sdk::pico_hijo::aportar(pico_del_hijo(&hijo));

        for h in lectores {
            let _ = h.join();
        }

        let codigo = estado.code().unwrap_or(-1);
        if !self.codigos_ok.contains(&codigo) {
            let cola = ultimas.lock().unwrap().join("\n  ");
            anyhow::bail!("'{}' termino con codigo {codigo}.\n  {cola}", self.programa);
        }

        let n = filas.load(Ordering::Relaxed);
        Ok(TaskOutput {
            rows: n,
            message: format!(
                "{} termino en {:.1}s{}",
                self.programa,
                t0.elapsed().as_secs_f64(),
                if n > 0 {
                    format!(" ({n} filas)")
                } else {
                    String::new()
                }
            ),
        })
    }
}

/// Vuelca un pipe al log, linea por linea, y va guardando las ultimas por si
/// el comando falla.
fn leer<R: std::io::Read + Send + 'static>(
    pipe: R,
    tarea: String,
    es_error: bool,
    ultimas: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    filas: std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        for linea in BufReader::new(pipe).lines().map_while(Result::ok) {
            if let Some(n) = linea.trim().strip_prefix(PREFIJO_FILAS) {
                if let Ok(v) = n.trim().parse::<u64>() {
                    filas.store(v, Ordering::Relaxed);
                    continue;
                }
            }
            if es_error {
                tracing::warn!(tarea = %tarea, "{linea}");
            } else {
                tracing::info!(tarea = %tarea, "{linea}");
            }
            let mut u = ultimas.lock().unwrap();
            u.push(linea);
            if u.len() > 15 {
                u.remove(0); // solo interesan las ultimas para el mensaje de error
            }
        }
    })
}

impl ShellTask {
    /// Convierte un fallo al lanzar en un mensaje que apunte al culpable real.
    ///
    /// # Por que existe
    ///
    /// El mensaje anterior era siempre el mismo —"no puedo ejecutar 'X':
    /// <error>. ¿Esta instalado y en el PATH?"— y eso manda a revisar el
    /// programa aunque el programa este perfecto.
    ///
    /// El caso que lo destapo: un pipeline con el directorio de trabajo mal
    /// configurado fallaba con *"no puedo ejecutar 'node.exe': El nombre del
    /// directorio no es valido"*. `node.exe` existia y andaba; lo que no existia
    /// era el `cwd`. Se perdieron horas mirando el binario, y despues el PATH,
    /// y despues los permisos del servicio. El sistema tenia el dato exacto de
    /// que estaba mal y lo estaba escondiendo detras de una pregunta que no
    /// venia al caso.
    ///
    /// Un orquestador no puede darse ese lujo: el mensaje de una tarea fallida
    /// suele ser lo unico que alguien va a leer.
    fn explicar_fallo_al_lanzar(&self, e: std::io::Error) -> anyhow::Error {
        // El cwd se revisa PRIMERO. Cuando el directorio no existe, el sistema
        // devuelve un error sobre el directorio pero la lectura natural es que
        // el problema es el programa que se nombra al lado.
        if let Some(d) = &self.cwd {
            let d = std::path::Path::new(d);
            if !d.exists() {
                return anyhow::anyhow!(
                    "el directorio de trabajo no existe: '{}'.\n\
                     El programa ('{}') puede estar perfecto: lo que falla es \
                     el directorio desde donde se lo intenta correr.\n\
                     Suele ser una variable de entorno que no llego al proceso \
                     y quedo en su valor por defecto.",
                    d.display(),
                    self.programa
                );
            }
            if !d.is_dir() {
                return anyhow::anyhow!(
                    "el directorio de trabajo '{}' existe pero no es un directorio",
                    d.display()
                );
            }
        }

        let programa = std::path::Path::new(&self.programa);
        // Con una ruta absoluta, "¿esta en el PATH?" no aplica: la ruta se dio
        // entera y o esta o no esta.
        if programa.is_absolute() && !programa.exists() {
            return anyhow::anyhow!(
                "no existe el programa '{}' (la ruta es absoluta, no se busca en el PATH): {e}",
                self.programa
            );
        }
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            return anyhow::anyhow!(
                "sin permiso para ejecutar '{}': {e}.\n\
                 Si esto corre como servicio, su cuenta puede no tener acceso \
                 a esa ruta.",
                self.programa
            );
        }
        if programa.is_absolute() {
            anyhow::anyhow!("no puedo ejecutar '{}': {e}", self.programa)
        } else {
            anyhow::anyhow!(
                "no puedo ejecutar '{}': {e}. ¿Esta instalado y en el PATH?",
                self.programa
            )
        }
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

    /// Un comando que existe en cualquier lado.
    fn eco(texto: &str) -> ShellTask {
        if cfg!(windows) {
            ShellTask::new("t", "cmd").args(["/C", &format!("echo {texto}")])
        } else {
            ShellTask::new("t", "sh").args(["-c", &format!("echo {texto}")])
        }
    }

    #[test]
    fn corre_un_comando_y_sale_bien() {
        let out = eco("hola").run(&ctx()).unwrap();
        assert!(out.message.contains("termino en"));
        assert_eq!(out.rows, 0, "sin la marca de filas, reporta 0");
    }

    #[test]
    fn levanta_las_filas_que_reporta_el_comando() {
        let out = eco("zeruel:rows=4321").run(&ctx()).unwrap();
        assert_eq!(out.rows, 4321);
        assert!(out.message.contains("4321 filas"));
    }

    #[test]
    fn un_codigo_de_salida_malo_es_error_y_dice_cual() {
        let t = if cfg!(windows) {
            ShellTask::new("t", "cmd").args(["/C", "echo se rompio & exit 3"])
        } else {
            ShellTask::new("t", "sh").args(["-c", "echo se rompio; exit 3"])
        };
        let e = t.run(&ctx()).unwrap_err().to_string();
        assert!(e.contains("codigo 3"), "{e}");
        assert!(e.contains("se rompio"), "el error trae la salida: {e}");
    }

    #[test]
    fn se_puede_aceptar_otro_codigo_como_exito() {
        let t = if cfg!(windows) {
            ShellTask::new("t", "cmd").args(["/C", "exit 2"])
        } else {
            ShellTask::new("t", "sh").args(["-c", "exit 2"])
        };
        assert!(t.run(&ctx()).is_err());
        let t = if cfg!(windows) {
            ShellTask::new("t", "cmd")
                .args(["/C", "exit 2"])
                .codigos_ok([0, 2])
        } else {
            ShellTask::new("t", "sh")
                .args(["-c", "exit 2"])
                .codigos_ok([0, 2])
        };
        assert!(t.run(&ctx()).is_ok(), "2 se declaro como exito");
    }

    #[test]
    fn un_programa_que_no_existe_lo_dice_claro() {
        let e = ShellTask::new("t", "no-existe-este-programa-xyz")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("no puedo ejecutar"), "{e}");
        assert!(e.contains("PATH"), "sugiere que mirar: {e}");
    }

    #[test]
    fn respeta_el_tope_de_tiempo() {
        let t = if cfg!(windows) {
            ShellTask::new("t", "cmd")
                .args(["/C", "ping -n 20 127.0.0.1 >nul"])
                .timeout_secs(1)
        } else {
            ShellTask::new("t", "sh")
                .args(["-c", "sleep 20"])
                .timeout_secs(1)
        };
        let t0 = Instant::now();
        let e = t.run(&ctx()).unwrap_err().to_string();
        assert!(e.contains("tope de 1s"), "{e}");
        assert!(t0.elapsed().as_secs() < 6, "tiene que cortar rapido");
    }

    #[test]
    fn se_corta_cuando_cancelan() {
        let c = ctx();
        let flag = c.cancel.clone().unwrap();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(400));
            flag.store(true, Ordering::SeqCst);
        });
        let t = if cfg!(windows) {
            ShellTask::new("t", "cmd").args(["/C", "ping -n 20 127.0.0.1 >nul"])
        } else {
            ShellTask::new("t", "sh").args(["-c", "sleep 20"])
        };
        let t0 = Instant::now();
        let e = t.run(&c).unwrap_err().to_string();
        assert!(e.contains("cancelado"), "{e}");
        assert!(t0.elapsed().as_secs() < 6, "corta al toque, no al final");
    }

    /// El caso real: el hijo lanza un nieto (como el lanzador de un venv de
    /// Windows lanza al Python real). Al cortar por tope tiene que morir el
    /// nieto también; si queda vivo, escribe la marca a los ~4 s.
    #[cfg(windows)]
    #[test]
    fn al_cortar_mata_tambien_a_los_nietos() {
        let dir = std::env::temp_dir().join(format!("zeruel-nieto-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let marca = dir.join("marca.txt");
        let nieto = dir.join("nieto.bat");
        let padre = dir.join("padre.bat");
        std::fs::write(
            &nieto,
            format!(
                "@ping -n 5 127.0.0.1 >nul\r\n@echo vivo> \"{}\"\r\n",
                marca.display()
            ),
        )
        .unwrap();
        std::fs::write(
            &padre,
            format!(
                "@start \"\" /B cmd /C \"{}\"\r\n@ping -n 30 127.0.0.1 >nul\r\n",
                nieto.display()
            ),
        )
        .unwrap();

        let t = ShellTask::new("t", "cmd")
            .args(["/C", &padre.display().to_string()])
            .timeout_secs(1);
        let e = t.run(&ctx()).unwrap_err().to_string();
        assert!(e.contains("tope de 1s"), "{e}");

        std::thread::sleep(Duration::from_secs(7));
        let sobrevivio = marca.exists();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            !sobrevivio,
            "el nieto siguio vivo despues del corte y escribio la marca"
        );
    }

    #[test]
    fn el_param_del_run_llega_por_argumento_y_por_entorno() {
        let t = if cfg!(windows) {
            ShellTask::new("t", "cmd").args(["/C", "echo"]).con_param()
        } else {
            ShellTask::new("t", "sh").args(["-c", "echo $ZERUEL_PARAM"])
        };
        assert!(t.run(&ctx()).is_ok());
    }

    #[test]
    fn corre_en_el_directorio_indicado() {
        // Se comprueba por donde CAE el archivo que escribe el comando, y no
        // por el codigo de salida de `dir`/`ls`: en Windows el quoting que hace
        // std::process con `cmd /C` da codigos raros y el test terminaria
        // midiendo eso en vez de `current_dir`.
        let tmp = tempfile::tempdir().unwrap();
        let t = if cfg!(windows) {
            ShellTask::new("t", "cmd").args(["/C", "echo hola>salida.txt"])
        } else {
            ShellTask::new("t", "sh").args(["-c", "echo hola > salida.txt"])
        };
        t.cwd(tmp.path()).run(&ctx()).unwrap();
        assert!(
            tmp.path().join("salida.txt").exists(),
            "el comando tiene que haber escrito dentro del cwd indicado"
        );
    }
}

/// Pico de memoria **residente** de un proceso hijo, en MB.
///
/// El handle sigue sirviendo despues de que el proceso murio —mientras no se
/// suelte—, y el sistema conserva sus contadores. Por eso se lee aca, con el
/// `Child` todavia vivo, y no despues.
///
/// Devuelve 0 si no se puede: es una mejora de la medicion, nunca un motivo
/// para que falle una tarea.
#[cfg(windows)]
fn pico_del_hijo(hijo: &std::process::Child) -> u64 {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    unsafe {
        let mut c: PROCESS_MEMORY_COUNTERS = std::mem::zeroed();
        c.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        let h = hijo.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
        if GetProcessMemoryInfo(h, &mut c, c.cb) == 0 {
            return 0;
        }
        // `PeakWorkingSetSize` es RSS, no memoria comprometida. La diferencia
        // no es menor: en un Python con pandas el commit es 5 veces el RSS.
        (c.PeakWorkingSetSize as u64) / (1024 * 1024)
    }
}

/// En Unix lo cubre `getrusage(RUSAGE_CHILDREN)`, que el core ya consulta y da
/// el RSS maximo de cualquier hijo esperado. No hace falta nada por hijo.
#[cfg(not(windows))]
fn pico_del_hijo(_hijo: &std::process::Child) -> u64 {
    0
}

#[cfg(test)]
mod tests_memoria_del_hijo {
    use super::*;
    use zeruel_sdk::TaskContext;

    /// Un intérprete disponible en esta máquina y el código para que reserve
    /// `mb` megabytes, los toque y salga.
    ///
    /// Se prueban varios porque la promesa es "cualquier lenguaje": lo único
    /// que zeruel sabe de un pipeline es que lanza procesos, y la medición no
    /// puede depender de cuál.
    fn interpretes(mb: usize) -> Vec<(&'static str, Vec<String>)> {
        let n = mb * 1024 * 1024;
        let candidatos: Vec<(&str, Vec<String>)> = vec![
            (
                "node",
                vec![
                    "-e".into(),
                    format!("const b=Buffer.alloc({n},1);process.stdout.write(''+b[0])"),
                ],
            ),
            (
                "python",
                vec![
                    "-c".into(),
                    format!("b=bytearray({n})\nb[0]=1\nprint(b[0])"),
                ],
            ),
            (
                "perl",
                vec!["-e".into(), format!("my $s = 'x' x {n}; print length($s);")],
            ),
        ];
        candidatos
            .into_iter()
            .filter(|(exe, _)| {
                std::process::Command::new(exe)
                    .arg("--version")
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .is_ok_and(|s| s.success())
            })
            .collect()
    }

    fn ctx() -> TaskContext {
        TaskContext {
            run_id: uuid::Uuid::new_v4(),
            param: "p".into(),
            cancel: None,
        }
    }

    /// **El caso que rompía la medición**: un proceso externo que reserva mucho
    /// y sale enseguida.
    ///
    /// El muestreo del árbol no llega a verlo. Descubrir qué procesos lo forman
    /// obliga a enumerar los del sistema —39 ms con los 518 de una máquina
    /// real—, así que el árbol no se puede redescubrir mucho más seguido que
    /// cada medio segundo. Y node y python reservan 300 MB y salen en 0,12 s,
    /// que es la forma normal de una tarea de ETL.
    ///
    /// Acá se verifica por el camino de verdad: `ShellTask` lee el pico del
    /// hijo desde su handle, que es RSS y sigue disponible después de que el
    /// proceso murió.
    #[test]
    fn el_pico_de_un_hijo_corto_se_mide_en_cualquier_lenguaje() {
        const MB: usize = 300;
        let disponibles = interpretes(MB);
        assert!(
            !disponibles.is_empty(),
            "no hay ningun interprete para probar; el test no puede afirmar nada"
        );

        let mut medidos = 0;
        for (exe, args) in &disponibles {
            zeruel_sdk::pico_hijo::reiniciar();
            let t = ShellTask::new("t", *exe).args(args.clone());
            let t0 = std::time::Instant::now();
            let r = t.run(&ctx());
            let duro = t0.elapsed();
            let pico = zeruel_sdk::pico_hijo::leer();

            if r.is_err() {
                println!("  {exe}: no corrio ({r:?}); no afirma nada");
                continue;
            }
            println!("  {exe}: duro {duro:?}, pico del hijo {pico} MB");
            assert!(
                pico >= (MB as u64) / 2,
                "{exe}: se perdio el pico de un proceso de {duro:?} que reservo \
                 {MB} MB (medido {pico} MB)"
            );
            medidos += 1;
        }
        assert!(medidos > 0, "ningun interprete llego a correr");
    }

    /// El buzón se limpia por tarea: si no, una tarea liviana heredaría el pico
    /// del hijo de la anterior y todas las posteriores a la más pesada
    /// reportarían el número de aquella.
    #[test]
    fn el_aporte_se_limpia_entre_tareas() {
        zeruel_sdk::pico_hijo::reiniciar();
        zeruel_sdk::pico_hijo::aportar(900);
        assert_eq!(zeruel_sdk::pico_hijo::leer(), 900);
        zeruel_sdk::pico_hijo::reiniciar();
        assert_eq!(
            zeruel_sdk::pico_hijo::leer(),
            0,
            "la tarea siguiente arranca limpia"
        );
    }

    /// Se queda con el mayor, no con el último: una tarea puede lanzar varios
    /// procesos y lo que importa es el peor momento.
    #[test]
    fn se_queda_con_el_mayor_de_los_hijos() {
        zeruel_sdk::pico_hijo::reiniciar();
        for mb in [120, 800, 300] {
            zeruel_sdk::pico_hijo::aportar(mb);
        }
        assert_eq!(zeruel_sdk::pico_hijo::leer(), 800);
    }
}

#[cfg(test)]
mod tests_diagnostico {
    use super::*;
    use zeruel_sdk::TaskContext;

    fn ctx() -> TaskContext {
        TaskContext {
            run_id: uuid::Uuid::new_v4(),
            param: "p".into(),
            cancel: None,
        }
    }

    /// Un programa que existe, con un directorio de trabajo que no.
    ///
    /// Este es el caso real que costo horas: el pipeline fallaba con
    /// *"no puedo ejecutar 'node.exe': El nombre del directorio no es valido"*.
    /// `node.exe` existia y andaba. Lo que faltaba era el `cwd`, porque una
    /// variable de entorno no habia llegado al proceso y quedo en su valor por
    /// defecto. El mensaje mandaba a revisar el binario, el PATH y los permisos
    /// del servicio, que estaban todos bien.
    #[test]
    fn si_falta_el_directorio_el_mensaje_no_culpa_al_programa() {
        let exe = std::env::current_exe().unwrap();
        let t = ShellTask::new("t", exe.to_string_lossy().to_string())
            .args(["--ayuda-que-no-existe"])
            .cwd("C:/directorio-que-no-existe-jamas-12345");
        let e = t.run(&ctx()).unwrap_err().to_string();

        assert!(
            e.contains("directorio de trabajo no existe"),
            "tiene que señalar el directorio: {e}"
        );
        assert!(
            e.contains("puede estar perfecto"),
            "y decir explicitamente que el programa no es el problema: {e}"
        );
        assert!(
            !e.contains("¿Esta instalado y en el PATH?"),
            "y NO mandar a revisar el PATH, que no tiene nada que ver: {e}"
        );
    }

    /// Con una ruta absoluta, preguntar por el PATH no aplica: la ruta se dio
    /// entera. Mandar a revisar el PATH manda a mirar donde no hay nada.
    #[test]
    fn con_ruta_absoluta_no_pregunta_por_el_path() {
        let t = ShellTask::new("t", "C:/no-existe/programa-inventado.exe");
        let e = t.run(&ctx()).unwrap_err().to_string();
        assert!(e.contains("no existe el programa"), "{e}");
        // Mencionar el PATH para descartarlo esta bien; lo que no puede hacer
        // es PREGUNTAR por el, que es lo que manda a mirar donde no hay nada.
        assert!(
            !e.contains("¿Esta instalado y en el PATH?"),
            "una ruta absoluta no se busca en el PATH: {e}"
        );
    }

    /// Con un nombre suelto SI corresponde: ahi el PATH es justamente donde se
    /// lo busca.
    #[test]
    fn con_un_nombre_suelto_si_menciona_el_path() {
        let t = ShellTask::new("t", "programa-que-no-existe-en-ningun-lado");
        let e = t.run(&ctx()).unwrap_err().to_string();
        assert!(e.contains("PATH"), "{e}");
    }

    /// Un cwd correcto no tiene que estorbar: el diagnostico solo se mete
    /// cuando hay algo que decir.
    #[test]
    fn con_todo_bien_no_inventa_un_problema_de_directorio() {
        let dir = std::env::temp_dir();
        let t = ShellTask::new("t", "programa-que-no-existe-en-ningun-lado").cwd(&dir);
        let e = t.run(&ctx()).unwrap_err().to_string();
        assert!(
            !e.contains("directorio de trabajo"),
            "el directorio estaba bien: {e}"
        );
    }
}
