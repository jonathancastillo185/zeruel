//! Servicio nativo de Windows.
//!
//! En Linux alcanza con una unidad de systemd porque `zeruel serve` ya hace lo
//! que systemd espera: corre en primer plano y apaga ordenado con SIGTERM. En
//! Windows no alcanza, y por una razon concreta: el Administrador de servicios
//! (SCM) no *ejecuta* un programa y ya, sino que **conversa** con el. El
//! proceso tiene que registrar un despachador, contestar "arrancando", despues
//! "corriendo", y reaccionar a las ordenes de control. Una app de consola comun
//! nunca contesta, asi que el SCM la marca como colgada y la mata.
//!
//! Por eso hasta ahora hacia falta un envoltorio como NSSM o WinSW: traducian
//! esa conversacion. Este modulo la tiene de primera mano, y con eso Windows
//! queda al mismo nivel que Linux — instalar, arrancar, parar y ver el estado
//! con las herramientas del sistema, sin nada extra que mantener.
//!
//! ```text
//!   zeruel servicio instalar --state C:\zeruel\state --pipelines C:\zeruel\pipelines
//!   sc start zeruel        (o: Servicios, o `zeruel servicio arrancar`)
//!   sc stop zeruel
//!   zeruel servicio desinstalar
//! ```
//!
//! ## El apagado, que es donde esto se rompe en silencio
//!
//! Al pedir la parada, el SCM manda un control `Stop` y **espera**. Si el
//! servicio no reporta `Stopped` dentro de su plazo, Windows lo mata. Es el
//! mismo problema que `TimeoutStopSec` en systemd y `--stop-timeout` en Docker:
//! el plazo del supervisor tiene que ser mayor que `--gracia-secs`, o se pierde
//! justo el trabajo que el apagado ordenado existe para no perder.
//!
//! Acá se maneja avisando `StopPending` con un `wait_hint` derivado de la
//! gracia configurada, en vez de dejar que Windows adivine.

use std::ffi::OsString;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use windows_service::service::{
    ServiceAccess, ServiceControl, ServiceControlAccept, ServiceErrorControl, ServiceExitCode,
    ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

/// Nombre por defecto del servicio.
///
/// En Windows el nombre de un servicio es unico por maquina, asi que una sola
/// instalacion con nombre fijo alcanzaria para una sola instancia. Y tener dos
/// es el caso normal, no el raro: el zeruel de desarrollo y el de produccion en
/// la misma maquina, o un zeruel por proyecto con su propio estado y sus
/// propios pipelines.
///
/// Por eso el nombre es un parametro (`--nombre`), y esto es solo el default:
///
/// ```text
///   zeruel servicio instalar --nombre zeruel-dev     --state C:/dev/state ...
///   zeruel servicio instalar --nombre zeruel-alertas --state C:/alertas/state ...
/// ```
///
/// Cada uno queda con su propia cuenta virtual (`NT SERVICE\<nombre>`), asi que
/// tampoco comparten permisos: el de desarrollo no puede tocar el estado del de
/// produccion.
pub const NOMBRE_DEFECTO: &str = "zeruel";

/// Que corre el servicio.
///
/// Son dos servicios separados a proposito, no uno que levante los dos. El
/// scheduler es lo que no puede caerse: si la web se cuelga o hay que
/// reiniciarla para cambiarle el puerto, eso no tiene por que interrumpir un
/// ETL de dos horas que esta a la mitad. Es el mismo principio que separa cada
/// pipeline en su propio proceso.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Modo {
    /// El que consume la cola y lanza los pipelines.
    Scheduler,
    /// La interfaz web. Solo lee y encola: no ejecuta nada.
    Web,
}

impl Modo {
    fn como_texto(self) -> &'static str {
        match self {
            Modo::Scheduler => "scheduler",
            Modo::Web => "web",
        }
    }
}
const TIPO: ServiceType = ServiceType::OWN_PROCESS;

/// Registra el servicio en el SCM.
///
/// Los argumentos con los que va a arrancar quedan guardados en el registro,
/// asi que las rutas tienen que ser absolutas: el servicio no hereda el
/// directorio actual de quien lo instalo.
pub fn instalar(
    nombre: &str,
    modo: Modo,
    state: &Path,
    pipelines: &Path,
    gracia: u64,
    puerto: u16,
    config: Option<&Path>,
) -> Result<()> {
    let exe = std::env::current_exe().context("no pude ubicar el ejecutable")?;
    let (state, pipelines) = (absoluta(state)?, absoluta(pipelines)?);

    // El archivo de configuracion, resuelto AHORA y guardado en el servicio.
    //
    // Un servicio arranca desde System32, no desde donde uno lo instalo, asi
    // que `Config::cargar(None)` -que busca `zeruel.toml` en el directorio
    // actual- no encuentra nada. Sin config no hay `[entorno]`, y sin
    // `[entorno]` los pipelines arrancan sin las variables que necesitan: el
    // primer run muere con "no puedo ejecutar '/opt/scraper-venv/bin/python'".
    //
    // Por eso la ruta se guarda en los argumentos del servicio. Si no la pasan,
    // se busca el `zeruel.toml` al lado del ejecutable, que es como queda una
    // instalacion normal (`zeruel deploy` deja los dos juntos).
    let config = match config {
        Some(c) => Some(absoluta(c)?),
        None => {
            let junto = exe.with_file_name(crate::config::NOMBRE_DEFAULT);
            junto.is_file().then_some(junto)
        }
    };
    if let Some(c) = &config {
        if !c.is_file() {
            anyhow::bail!("no existe el archivo de configuracion {}", c.display());
        }
    }

    // Instalar un servicio escribe en la base del SCM (bajo HKLM): es una
    // frontera de seguridad de Windows, no un capricho, y no hay forma de
    // esquivarla — el equivalente de que `systemctl enable` necesite root.
    //
    // En vez de fallar con "Acceso denegado", se pide la elevacion: el usuario
    // ve un dialogo de UAC en lugar de un error que tiene que interpretar.
    // Solo hace falta para ESTE comando; `serve`, `web` y `run` no piden nada.
    let mgr = match ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    ) {
        Ok(m) => m,
        Err(e) if acceso_denegado(&e) => {
            return elevar(
                &exe,
                &["servicio", "instalar"],
                &[
                    ("--nombre", nombre.to_string()),
                    ("--modo", modo.como_texto().to_string()),
                    ("--puerto", puerto.to_string()),
                    ("--state", state.to_string_lossy().into_owned()),
                    ("--pipelines", pipelines.to_string_lossy().into_owned()),
                    ("--gracia-secs", gracia.to_string()),
                ],
            );
        }
        Err(e) => return Err(e).context("no pude abrir el Administrador de servicios"),
    };

    let info = ServiceInfo {
        name: OsString::from(nombre),
        display_name: OsString::from(match modo {
            Modo::Scheduler => "zeruel — scheduler de pipelines",
            Modo::Web => "zeruel — interfaz web",
        }),
        service_type: TIPO,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: exe,
        // `servicio correr` es el modo que habla con el SCM. No es para
        // ejecutarlo a mano: sin un SCM del otro lado falla al registrarse.
        launch_arguments: vec![
            OsString::from("servicio"),
            OsString::from("correr"),
            // El nombre va explicito. Windows lo ignora en servicios
            // OWN_PROCESS, asi que sin esto igual arranca — pero con dos
            // instancias el proceso reportaria el nombre equivocado, y eso es
            // justo lo que despues nadie entiende al leer un log.
            OsString::from("--nombre"),
            OsString::from(nombre),
            OsString::from("--modo"),
            OsString::from(modo.como_texto()),
            OsString::from("--puerto"),
            OsString::from(puerto.to_string()),
            OsString::from("--state"),
            state.clone().into_os_string(),
            OsString::from("--pipelines"),
            pipelines.clone().into_os_string(),
            OsString::from("--gracia-secs"),
            OsString::from(gracia.to_string()),
        ]
        .into_iter()
        .chain(
            config
                .iter()
                .flat_map(|c| [OsString::from("--config"), c.clone().into_os_string()]),
        )
        .collect(),
        dependencies: vec![],
        // NO LocalSystem, que es el default y la cuenta mas privilegiada del
        // sistema. `NT SERVICE\zeruel` es una cuenta virtual: Windows la crea
        // sola al registrar el servicio, existe solo para el, y no tiene
        // permisos sobre nada que no se le conceda explicitamente.
        //
        // Es el equivalente de lo que hace la unidad de systemd con un usuario
        // dedicado y ProtectSystem=strict. Dejarlo en LocalSystem significaria
        // que cualquier pipeline corre como administrador absoluto de la
        // maquina, que es mucho mas de lo que un ETL necesita.
        account_name: Some(OsString::from(format!(r"NT SERVICE\{nombre}"))),
        account_password: None,
    };

    let svc = mgr
        .create_service(&info, ServiceAccess::CHANGE_CONFIG)
        .map_err(|e| {
            // 1072 = ERROR_SERVICE_MARKED_FOR_DELETE. Es un estado del que no se
            // sale reintentando, y el mensaje de Windows —"el servicio especificado
            // se ha marcado para ser eliminado"— no dice que hacer.
            if marcado_para_borrar(&e) {
                anyhow::anyhow!(
                    "el servicio '{nombre}' quedo marcado para eliminacion y todavia no se fue.
Windows no lo borra mientras siga corriendo o alguien tenga un handle abierto.

  1. Stop-Service {nombre} -Force
  2. cerra services.msc y el Administrador de tareas si estan abiertos
  3. Get-Service {nombre}   (no tiene que aparecer)

Recien ahi se puede volver a instalar."
                )
            } else {
                anyhow::Error::new(e).context("no pude crear el servicio")
            }
        })?;
    svc.set_description(
        "Ejecuta los pipelines de la cola de zeruel. Desplegar un pipeline nuevo \
         no requiere reiniciar el servicio: alcanza con copiar el binario al \
         directorio de pipelines.",
    )
    .ok();

    // La cuenta virtual no tiene permisos sobre nada: es exactamente el punto.
    // Hay que concederle lo minimo, y nada mas que eso.
    conceder(nombre, &state, "(OI)(CI)M")?; // modificar: base, WAL y logs
    conceder(nombre, &pipelines, "(OI)(CI)RX")?; // leer y ejecutar: no escribe binarios
    if let Some(c) = &config {
        // Leer, nada mas: el servicio no escribe su propia configuracion. El
        // permiso va sobre el archivo, no sobre la carpeta, porque ahi al lado
        // suele estar el binario y no hace falta darle acceso a todo.
        conceder(nombre, c, "R")?;
    }

    reiniciar_solo(nombre);

    println!("servicio '{nombre}' instalado ({}).", modo.como_texto());
    println!("  corre como  NT SERVICE\\{nombre}  (cuenta aislada, sin privilegios de admin)");
    if let Some(c) = &config {
        println!("  configuracion   {}", c.display());
    } else {
        println!(
            "  SIN CONFIGURACION: no habia {} al lado del ejecutable.",
            crate::config::NOMBRE_DEFAULT
        );
        println!("  Los pipelines van a arrancar sin las variables de [entorno].");
        println!("  Reinstalalo con --config <ruta> si las necesitan.");
    }
    println!("  si se cae      lo reinicia el sistema (5s, 10s, 60s)");
    println!("  arrancalo con:  sc start {nombre}");
    Ok(())
}

/// Le pide al SCM que lo levante solo si el proceso se muere.
///
/// Sin esto, un servicio que cae se queda caido hasta que alguien lo note — y
/// un scheduler caido no avisa: no falla ningun run, simplemente **no corre
/// ninguno**, que desde afuera se ve igual que "no habia trabajo". Es la peor
/// forma de romperse que tiene un orquestador.
///
/// 5 s, 10 s y despues cada minuto. El contador se resetea cada dia, asi que
/// una caida aislada por semana no agota los reintentos.
///
/// Se hace con `sc.exe` y no con la API por la misma razon que `conceder` usa
/// `icacls`: es una llamada, el comando es legible, y si falla se puede repetir
/// a mano. Y si falla no se aborta la instalacion: el servicio quedo bien, lo
/// que falta es una comodidad.
fn reiniciar_solo(nombre: &str) {
    let salida = std::process::Command::new("sc.exe")
        .args([
            "failure",
            nombre,
            "reset=",
            "86400",
            "actions=",
            "restart/5000/restart/10000/restart/60000",
        ])
        .output();
    match salida {
        Ok(s) if s.status.success() => {}
        Ok(s) => eprintln!(
            "aviso: no pude configurar el reinicio automatico ({}).              Se puede repetir con: sc failure {nombre} reset= 86400              actions= restart/5000/restart/10000/restart/60000",
            String::from_utf8_lossy(&s.stderr).trim()
        ),
        Err(e) => eprintln!("aviso: no pude ejecutar sc.exe para el reinicio automatico: {e}"),
    }
}

/// Le da a la cuenta del servicio el permiso mínimo sobre un directorio.
///
/// Se usa `icacls` en vez de las APIs de ACL de Win32 porque para dos llamadas
/// no vale la pena arrastrar `Win32_Security` entero, y porque el comando es
/// legible: si algo sale mal, se puede repetir a mano y ver qué contesta.
fn conceder(nombre: &str, dir: &Path, permisos: &str) -> Result<()> {
    let cuenta = format!(r"NT SERVICE\{nombre}");
    let salida = std::process::Command::new("icacls")
        .arg(dir)
        .arg("/grant")
        .arg(format!("{cuenta}:{permisos}"))
        .output()
        .context("no pude ejecutar icacls")?;

    if !salida.status.success() {
        // No se aborta: el servicio ya quedó registrado y esto se puede
        // arreglar a mano. Fallar acá dejaría un servicio instalado y un error
        // que sugiere que no lo está, que es peor.
        eprintln!(
            "aviso: no pude dar permisos sobre {}. El servicio no va a poder \
             escribir ahi hasta que corras:\n  icacls \"{}\" /grant \"{cuenta}:{permisos}\"",
            dir.display(),
            dir.display()
        );
    }
    Ok(())
}

pub fn desinstalar(nombre: &str) -> Result<()> {
    let exe = std::env::current_exe().context("no pude ubicar el ejecutable")?;
    let mgr = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .context("no pude abrir el Administrador de servicios")?;
    let acceso = ServiceAccess::DELETE | ServiceAccess::QUERY_STATUS | ServiceAccess::STOP;
    let svc = match mgr.open_service(nombre, acceso) {
        Ok(s) => s,
        Err(e) if acceso_denegado(&e) => {
            return elevar(
                &exe,
                &["servicio", "desinstalar"],
                &[("--nombre", nombre.to_string())],
            )
        }
        Err(e) => return Err(e).with_context(|| format!("no encontre el servicio '{nombre}'")),
    };

    // Hay que PARARLO, no avisar que hay que pararlo.
    //
    // Windows no borra un servicio que esta corriendo: lo deja "marcado para
    // eliminacion" y recien lo saca cuando para y nadie tiene un handle
    // abierto. Mientras tanto queda en un limbo donde no se puede recrear
    // —`CreateService` falla con el error 1072— ni arrancar.
    //
    // Antes esto solo IMPRIMIA una advertencia y borraba igual, asi que seguir
    // los pasos de "desinstalar y volver a instalar" dejaba el sistema sin
    // servicio y sin forma de rehacerlo. Un comando que deja al usuario en un
    // estado del que no puede salir es peor que uno que se niega a correr.
    if let Ok(st) = svc.query_status() {
        if st.current_state != ServiceState::Stopped {
            println!("el servicio esta corriendo: lo paro primero.");
            if let Err(e) = svc.stop() {
                eprintln!("aviso: no pude pararlo ({e}). Se intenta borrar igual.");
            }
            esperar_parada(&svc);
        }
    }

    svc.delete().context("no pude borrar el servicio")?;
    println!("servicio '{nombre}' desinstalado.");
    Ok(())
}

/// Espera a que el servicio termine de parar.
///
/// El tope son 90 s por la misma razon que la gracia del apagado: es lo que
/// espera systemd antes de mandar SIGKILL, y pasarse no aporta — si en ese
/// tiempo no paro, no va a parar solo.
///
/// Si se agota, no se aborta: se borra igual y queda marcado. Es lo mismo que
/// pasaba antes, pero ahora al menos se intento y se dice.
fn esperar_parada(svc: &windows_service::service::Service) {
    let limite = std::time::Duration::from_secs(90);
    let t0 = std::time::Instant::now();
    while t0.elapsed() < limite {
        match svc.query_status() {
            Ok(st) if st.current_state == ServiceState::Stopped => return,
            Ok(_) => {}
            // Si no se puede consultar, esperar mas no va a mejorar nada.
            Err(_) => return,
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    eprintln!(
        "aviso: sigue sin parar despues de {}s. Se borra igual, pero puede quedar \
         marcado para eliminacion hasta que el proceso muera.",
        limite.as_secs()
    );
}

/// Arranca el servicio.
///
/// Tambien necesita elevacion: `instalar` la pide y despues `sc start` chocaba
/// contra Acceso Denegado, que es hacer elevar al usuario una vez y dejarlo
/// tirado en el paso siguiente.
pub fn arrancar(nombre: &str) -> Result<()> {
    let exe = std::env::current_exe().context("no pude ubicar el ejecutable")?;
    let svc = match abrir(nombre, ServiceAccess::START | ServiceAccess::QUERY_STATUS) {
        Ok(s) => s,
        Err(Elevar) => {
            return elevar(
                &exe,
                &["servicio", "arrancar"],
                &[("--nombre", nombre.into())],
            )
        }
        Err(Otro(e)) => return Err(e),
    };
    svc.start::<&str>(&[])
        .context("no pude arrancar el servicio")?;
    println!("servicio '{nombre}' arrancado.");
    Ok(())
}

/// Para el servicio. El apagado ordenado corre del lado del servicio: deja de
/// lanzar, espera a los runs en curso y devuelve a la cola lo que corte.
pub fn parar(nombre: &str) -> Result<()> {
    let exe = std::env::current_exe().context("no pude ubicar el ejecutable")?;
    let svc = match abrir(nombre, ServiceAccess::STOP | ServiceAccess::QUERY_STATUS) {
        Ok(s) => s,
        Err(Elevar) => return elevar(&exe, &["servicio", "parar"], &[("--nombre", nombre.into())]),
        Err(Otro(e)) => return Err(e),
    };
    svc.stop().context("no pude parar el servicio")?;
    println!("servicio '{nombre}' parando. Espera a los runs en curso antes de cerrar.");
    Ok(())
}

/// Para el servicio y espera a que pare de verdad.
///
/// Devuelve `true` si estaba corriendo y paro, `false` si ya estaba parado o no
/// existe. A diferencia de `parar`, esto **espera**: el instalador necesita
/// saber que el binario quedo libre antes de reemplazarlo.
pub fn parar_y_esperar(nombre: &str) -> Result<bool> {
    let svc = match abrir(nombre, ServiceAccess::STOP | ServiceAccess::QUERY_STATUS) {
        Ok(s) => s,
        // No existe todavia: no es un error, es una instalacion nueva.
        Err(_) => return Ok(false),
    };
    match svc.query_status() {
        Ok(st) if st.current_state == ServiceState::Stopped => return Ok(false),
        Ok(_) => {}
        Err(_) => return Ok(false),
    }
    svc.stop().context("no pude parar el servicio")?;
    esperar_parada(&svc);
    Ok(true)
}

/// Saca el servicio sin quejarse si no estaba. Lo usa el instalador antes de
/// registrarlo de nuevo: los argumentos de arranque viven en el registro del
/// servicio, asi que un `--config` nuevo no llega si no se vuelve a crear.
pub fn desinstalar_silencioso(nombre: &str) -> Result<()> {
    let mgr = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .context("no pude abrir el Administrador de servicios")?;
    let acceso = ServiceAccess::DELETE | ServiceAccess::QUERY_STATUS | ServiceAccess::STOP;
    let Ok(svc) = mgr.open_service(nombre, acceso) else {
        return Ok(()); // no estaba
    };
    if let Ok(st) = svc.query_status() {
        if st.current_state != ServiceState::Stopped {
            let _ = svc.stop();
            esperar_parada(&svc);
        }
    }
    let _ = svc.delete();
    Ok(())
}

/// Arranca y confirma que quedo corriendo.
///
/// `sc start` vuelve apenas el SCM acepta la orden, no cuando el servicio esta
/// listo. Sin esperar, un instalador diria "arrancado" de algo que murio dos
/// segundos despues — que es justo el tipo de mentira que este proyecto ya
/// pago caro.
pub fn arrancar_y_verificar(nombre: &str) -> Result<()> {
    let svc = abrir(nombre, ServiceAccess::START | ServiceAccess::QUERY_STATUS)
        .map_err(|_| anyhow::anyhow!("no pude abrir el servicio '{nombre}'"))?;
    if let Ok(st) = svc.query_status() {
        if st.current_state == ServiceState::Running {
            return Ok(());
        }
    }
    svc.start(&[] as &[&std::ffi::OsStr])
        .context("no pude arrancar el servicio")?;

    let limite = std::time::Duration::from_secs(30);
    let t0 = std::time::Instant::now();
    while t0.elapsed() < limite {
        match svc.query_status() {
            Ok(st) if st.current_state == ServiceState::Running => return Ok(()),
            Ok(st) if st.current_state == ServiceState::Stopped && t0.elapsed().as_secs() > 2 => {
                anyhow::bail!(
                    "arranco y se detuvo solo. Mira el log en <state>/logs o \
                     el Visor de eventos de Windows."
                );
            }
            _ => {}
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
    anyhow::bail!("no llego a 'Running' en {}s", limite.as_secs())
}

/// Un servicio esta sano si existe, esta corriendo **y va a volver despues de
/// un reinicio**.
///
/// Lo tercero no es un detalle: una instalacion real quedo con los dos
/// servicios en `Running` pero con arranque `Disabled`, secuela de un
/// `desinstalar` que los habia dejado marcados para eliminacion. Andaba, y
/// nadie se iba a enterar hasta el primer reinicio de la maquina — que es
/// exactamente cuando peor viene enterarse.
///
/// Devuelve `false` ante cualquier duda (no existe, no se puede consultar): se
/// usa para decidir si reinstalar, y reinstalar de mas es barato.
pub fn sano(nombre: &str) -> bool {
    let Ok(svc) = abrir(
        nombre,
        ServiceAccess::QUERY_STATUS | ServiceAccess::QUERY_CONFIG,
    ) else {
        return false;
    };
    let corriendo = matches!(
        svc.query_status(),
        Ok(st) if st.current_state == ServiceState::Running
    );
    let arranca_solo = matches!(
        svc.query_config(),
        Ok(c) if c.start_type == ServiceStartType::AutoStart
    );
    corriendo && arranca_solo
}

/// Estado del servicio. Consultar NO necesita elevacion, a diferencia de
/// arrancar y parar: por eso este comando no pide nada.
pub fn estado(nombre: &str) -> Result<()> {
    let svc = match abrir(nombre, ServiceAccess::QUERY_STATUS) {
        Ok(s) => s,
        Err(Elevar) => anyhow::bail!("sin permiso para consultar '{nombre}'"),
        Err(Otro(e)) => return Err(e),
    };
    let st = svc.query_status().context("no pude consultar el estado")?;
    println!("servicio '{nombre}': {:?}", st.current_state);
    if let Some(pid) = st.process_id {
        println!("  pid {pid}");
    }
    Ok(())
}

/// Por que hace falta un error propio: al abrir un servicio hay que distinguir
/// "no tenes permiso" —que se resuelve elevando— de cualquier otro fallo, que
/// hay que reportar. Un `Result<_, anyhow::Error>` perderia esa distincion.
enum FalloAbrir {
    Elevar,
    Otro(anyhow::Error),
}
use FalloAbrir::{Elevar, Otro};

fn abrir(
    nombre: &str,
    acceso: ServiceAccess,
) -> std::result::Result<windows_service::service::Service, FalloAbrir> {
    let mgr = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT).map_err(
        |e| Otro(anyhow::Error::new(e).context("no pude abrir el Administrador de servicios")),
    )?;
    match mgr.open_service(nombre, acceso) {
        Ok(s) => Ok(s),
        Err(e) if acceso_denegado(&e) => Err(Elevar),
        Err(e) => Err(Otro(anyhow::Error::new(e).context(format!(
            "no encontre el servicio '{nombre}'. ¿Lo instalaste con --nombre {nombre}?"
        )))),
    }
}

/// Los argumentos con los que arrancar. El despachador que genera
/// `define_windows_service!` tiene firma fija y no admite contexto, asi que
/// esta es la unica via para hacerselos llegar.
static ARGS: OnceLock<(String, Modo, u16, crate::scheduler::ServeArgs)> = OnceLock::new();

/// Punto de entrada del modo servicio. Le entrega el proceso al SCM.
pub fn correr(
    nombre: &str,
    modo: Modo,
    puerto: u16,
    args: crate::scheduler::ServeArgs,
) -> Result<()> {
    let _ = ARGS.set((nombre.to_string(), modo, puerto, args));
    windows_service::service_dispatcher::start(nombre, ffi_service_main).context(
        "no pude registrarme en el Administrador de servicios. \
                  `servicio correr` lo invoca Windows, no se ejecuta a mano: \
                  para correrlo en primer plano usa `zeruel serve`.",
    )?;
    Ok(())
}

windows_service::define_windows_service!(ffi_service_main, service_main);

fn service_main(_args: Vec<OsString>) {
    if let Err(e) = despachar() {
        // Sin consola donde escribir: el log de archivo es el unico lugar donde
        // esto se puede ver despues.
        tracing::error!("el servicio termino con error: {e:#}");
    }
}

fn despachar() -> Result<()> {
    let (nombre, modo, puerto, args) = ARGS
        .get()
        .cloned()
        .context("el modo servicio arranco sin argumentos")?;

    // La orden de parada llega en el hilo del manejador de control, que tiene
    // que contestar enseguida: no puede quedarse esperando a que el scheduler
    // termine. Por eso solo enciende la bandera y vuelve.
    let apagando = Arc::new(AtomicBool::new(false));
    let bandera = Arc::clone(&apagando);
    let manejador = move |control| match control {
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        // Stop es `sc stop` o el boton de Servicios; Shutdown es la maquina
        // apagandose. Los dos merecen el mismo apagado ordenado.
        ServiceControl::Stop | ServiceControl::Shutdown => {
            bandera.store(true, Ordering::SeqCst);
            ServiceControlHandlerResult::NoError
        }
        _ => ServiceControlHandlerResult::NotImplemented,
    };
    let estado = service_control_handler::register(&nombre, manejador)
        .context("no pude registrar el manejador de control")?;

    estado.set_service_status(ServiceStatus {
        service_type: TIPO,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    let gracia = args.gracia_secs;
    let resultado = match modo {
        Modo::Scheduler => crate::scheduler::serve_con_bandera(args, apagando),
        Modo::Web => {
            // `config_origen` sale de los argumentos con los que se registro el
            // servicio. Si es None, la web lo va a mostrar como "sin archivo",
            // que es exactamente lo que hay que ver cuando pasa.
            let config = args.config_origen.clone();
            crate::web::run_con_bandera(
                args.state,
                args.pipelines,
                puerto,
                0,
                "127.0.0.1",
                None,
                config,
                Some(apagando),
            )
        }
    };

    // El plazo se lo decimos a Windows en vez de dejar que lo adivine: si no,
    // mata el proceso a mitad del apagado y se pierde el reencolado.
    estado.set_service_status(ServiceStatus {
        service_type: TIPO,
        current_state: ServiceState::StopPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::from_secs(gracia + 15),
        process_id: None,
    })?;

    let salida = match &resultado {
        Ok(()) => ServiceExitCode::Win32(0),
        Err(e) => {
            tracing::error!("el scheduler termino con error: {e:#}");
            ServiceExitCode::ServiceSpecific(1)
        }
    };
    estado.set_service_status(ServiceStatus {
        service_type: TIPO,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: salida,
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;
    resultado
}

/// Las rutas se guardan en el registro: relativas no sirven, porque el servicio
/// arranca desde `C:\Windows\System32` y no desde donde se instalo.
fn absoluta(p: &Path) -> Result<std::path::PathBuf> {
    if p.is_absolute() {
        return Ok(p.to_path_buf());
    }
    let abs = std::env::current_dir()?.join(p);
    println!("nota: '{}' se guarda como '{}'", p.display(), abs.display());
    Ok(abs)
}

/// ¿El error es "no tenés permiso" y no otra cosa?
///
/// Se mira el codigo del sistema, no el texto: el mensaje viene traducido al
/// ¿El error es "el servicio esta marcado para eliminacion"?
///
/// Se detecta igual que `acceso_denegado`, y por la misma razon: hay que
/// distinguir un fallo del que se sale —parando el servicio— de uno que hay que
/// reportar tal cual. Reintentar no sirve, asi que el mensaje tiene que decir
/// que hacer.
fn marcado_para_borrar(e: &windows_service::Error) -> bool {
    const MARCADO: i32 = 1072; // ERROR_SERVICE_MARKED_FOR_DELETE
    let mut fuente: Option<&dyn std::error::Error> = std::error::Error::source(e);
    while let Some(f) = fuente {
        if let Some(io) = f.downcast_ref::<std::io::Error>() {
            return io.raw_os_error() == Some(MARCADO);
        }
        fuente = std::error::Error::source(f);
    }
    e.to_string().contains("os error 1072")
}

/// idioma de Windows y compararlo por string funcionaria solo en español.
fn acceso_denegado(e: &windows_service::Error) -> bool {
    const ACCESO_DENEGADO: i32 = 5;
    let s = e.to_string();
    // windows-service envuelve el io::Error; la cadena de `source` lo expone.
    let mut fuente: Option<&dyn std::error::Error> = std::error::Error::source(e);
    while let Some(f) = fuente {
        if let Some(io) = f.downcast_ref::<std::io::Error>() {
            return io.raw_os_error() == Some(ACCESO_DENEGADO);
        }
        fuente = std::error::Error::source(f);
    }
    s.contains("os error 5")
}

/// Vuelve a lanzar este mismo comando pidiendo elevacion, que en Windows es un
/// dialogo de UAC.
///
/// Se hace via PowerShell en vez de llamar a ShellExecute directamente para no
/// sumar features de windows-sys por una sola llamada. `-Wait` es lo que
/// permite enterarse de si funciono: sin eso el proceso padre termina antes que
/// ¿Este proceso tiene permisos de administrador?
///
/// Se pregunta intentando abrir el Administrador de servicios con permiso de
/// creacion, que es exactamente lo que el instalador va a necesitar. Preguntar
/// por el grupo del usuario mentiria: un administrador con UAC activo corre
/// SIN privilegios hasta que eleva.
pub fn soy_administrador() -> bool {
    ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )
    .is_ok()
}

/// Relanza este mismo comando elevado, con los argumentos que se le pasen.
///
/// Publico porque el instalador tiene que elevarse **una sola vez, al
/// principio**. Elevar por operacion —como hacia cada `servicio instalar`—
/// significa varios dialogos de UAC y, peor, que todo lo que NO eleva (tocar el
/// PATH, arrancar los servicios) corra sin permisos y falle.
pub fn relanzar_elevado(sub: &[&str], flags: &[(&str, String)]) -> Result<()> {
    let exe = std::env::current_exe().context("no pude ubicar el ejecutable")?;
    elevar(&exe, sub, flags)
}

/// el hijo y siempre diria que salio bien.
fn elevar(exe: &Path, sub: &[&str], flags: &[(&str, String)]) -> Result<()> {
    let mut args: Vec<String> = sub.iter().map(|s| s.to_string()).collect();
    for (k, v) in flags {
        args.push(k.to_string());
        args.push(v.clone());
    }
    // PowerShell escapa la comilla simple duplicandola. Sin esto, una ruta con
    // una comilla cortaria el argumento y el resto se interpretaria como
    // codigo: las rutas salen de la linea de comandos del propio usuario, pero
    // un `C:\datos\O'Brien\state` no tiene por que romper la instalacion.
    let lista = args
        .iter()
        .map(|a| format!("'{}'", a.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");

    println!("esto necesita permisos de administrador: te va a pedir confirmacion.");

    // Cancelar el dialogo de UAC hace que Start-Process lance una excepcion, y
    // PowerShell la escupe con stack trace incluido. Eso no es un error del
    // programa sino una respuesta del usuario, asi que se captura y se traduce
    // a un codigo de salida limpio.
    let script = format!(
        "try {{ $p = Start-Process -FilePath '{}' -ArgumentList {} -Verb RunAs -Wait -PassThru;          exit $p.ExitCode }} catch {{ exit 1223 }}",
        exe.display(),
        lista
    );
    const CANCELADO: i32 = 1223; // ERROR_CANCELLED de Windows

    let salida = std::process::Command::new("powershell")
        .args(["-NoProfile", "-Command", &script])
        .stderr(std::process::Stdio::null())
        .status()
        .context("no pude pedir la elevacion")?;

    if salida.code() == Some(CANCELADO) {
        anyhow::bail!(
            "cancelaste el pedido de permisos, asi que no se instalo nada.
             Volve a intentarlo y acepta el dialogo, o abri una consola como              administrador y corre el mismo comando."
        );
    }

    if salida.success() {
        return Ok(());
    }
    anyhow::bail!(
        "el comando con permisos de administrador fallo (codigo {}).          Abri una consola como administrador y corre el mismo comando para ver          que dice.",
        salida.code().unwrap_or(-1)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn las_rutas_relativas_se_vuelven_absolutas() {
        // Un servicio arranca desde System32. Guardar "./state" lo mandaria a
        // C:\Windows\System32\state, que es un directorio que nadie mira.
        let a = absoluta(Path::new("state")).unwrap();
        assert!(a.is_absolute());
        assert!(a.ends_with("state"));
    }

    #[test]
    fn una_ruta_absoluta_no_se_toca() {
        let p = Path::new(r"C:\zeruel\state");
        assert_eq!(absoluta(p).unwrap(), p);
    }
}
