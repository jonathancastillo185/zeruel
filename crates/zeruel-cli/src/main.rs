//! zeruel: CLI
//!
//! Comandos:
//!   zeruel gen      --root DATOS --n 10000000      genera el dataset sintetico
//!   zeruel run      --root DATOS --state STATE     corre el pipeline ETL
//!   zeruel list     --state STATE                  lista runs recientes (paginado)
//!   zeruel status   --state STATE [--run UUID]     muestra el run puntual + higiene + cola
//!   zeruel gc       --state STATE [--dry-run]      purga por retencion
//!   zeruel enqueue  --dag X --param Y --priority N encola un run
//!   zeruel backfill --dag X --desde F --hasta F    encola un run por dia del rango
//!   zeruel sla      --dag X --max-wait SEGS        tope de espera en cola (0 = sin tope)
//!   zeruel cola     [--max N]                      tope global de items en cola
//!   zeruel serve    --root DATOS --state STATE     scheduler global (daemon)
//!   zeruel web      --state STATE [--bind IP]      UI web (solo local por defecto)
//!   zeruel mcp      --state STATE                  MCP server por stdio (IA)
//!   zeruel puro     --root DATOS                   pipeline sin zeruel (benchmark)
//!
//! --json (global): salida parseable para scripts/IA.

mod capacidad;
mod config;
mod configurar;
mod cron;
mod dags;
mod dataset;
mod dependencias;
mod deploy;
mod ia;
mod init;
#[cfg(windows)]
mod instalador;
mod mcp;
mod ops;
mod pipeline;
mod pipelines;
mod puro;
mod salud;
mod scheduler;
#[cfg(windows)]
mod servicio;
mod web;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use uuid::Uuid;
use zeruel_core::{Hygiene, QueueItem, Run, Store};
use zeruel_data::DataEnv;
use zeruel_sdk::Task;

#[derive(Parser)]
#[command(
    name = "zeruel",
    version,
    about = "Motor de orquestacion de data pipelines en Rust"
)]
struct Cli {
    /// Salida JSON (parseable por scripts/IA)
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Instala zeruel en esta maquina: copia el binario, crea la
    /// configuracion, registra los servicios y los arranca.
    ///
    /// Si ya hay una instalacion en el destino, actualiza: para los servicios
    /// esperando a los runs en curso, reemplaza el binario y vuelve a arrancar.
    #[cfg(windows)]
    Instalar {
        /// Donde instalarlo (por defecto C:/zeruel).
        #[arg(long)]
        destino: Option<PathBuf>,
        /// Nombre base de los servicios. El de la web lleva "-web".
        #[arg(long, default_value = servicio::NOMBRE_DEFECTO)]
        nombre: String,
        #[arg(long, default_value_t = 9876)]
        puerto: u16,
    },
    /// Prueba las herramientas de [entorno] y dice cuales no responden.
    ///
    /// Un pipeline llama a programas que zeruel no conoce. Si alguno no se
    /// puede ejecutar, sin esto se descubre corrida por corrida y cada fallo
    /// llega a nombre del DAG en vez del problema.
    Doctor {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Ver y cambiar los ajustes sin editar el zeruel.toml a mano.
    ///
    /// Sin argumentos muestra los ajustes y sus valores. `config set <clave>
    /// <valor>` cambia uno, tocando solo esa linea: los comentarios del archivo
    /// son documentacion y reserializar el TOML los borraria todos.
    ///
    /// Cambia el archivo, no el proceso: el scheduler lee su configuracion al
    /// arrancar, asi que hay que reiniciarlo para que tome efecto.
    Config {
        /// El zeruel.toml a leer o modificar.
        #[arg(long)]
        config: Option<PathBuf>,
        #[command(subcommand)]
        que: Option<CmdConfig>,
    },
    /// Crea el zeruel.toml de una instalacion nueva, preguntando los limites.
    ///
    /// Los topes de tiempo y de memoria vienen apagados por defecto, y nadie
    /// descubre que le faltan hasta que un DAG queda corriendo dos dias.
    Init {
        /// Donde escribirlo (por defecto ./zeruel.toml).
        #[arg(long)]
        salida: Option<PathBuf>,
        /// No preguntar nada: usar los valores sugeridos para esta maquina.
        #[arg(long, short = 'y')]
        si: bool,
        /// Pisar un zeruel.toml que ya exista.
        #[arg(long)]
        forzar: bool,
    },
    /// Genera el dataset sintetico de e-commerce
    Gen {
        #[arg(long, default_value = "data")]
        root: PathBuf,
        #[arg(long, default_value_t = 10_000_000)]
        n: u32,
    },
    /// Ejecuta el pipeline ETL
    Run {
        #[arg(long, default_value = "data")]
        root: PathBuf,
        #[arg(long, default_value = "state")]
        state: PathBuf,
        #[arg(long, default_value = "demo")]
        param: String,
        #[arg(long, default_value_t = 4)]
        workers: usize,
        #[arg(long)]
        fresh: bool,
        /// Agrega una tarea WASM al DAG (B.1): el .wasm corre dentro del grafo
        #[arg(long)]
        wasm: Option<PathBuf>,
        /// Fuel para la tarea WASM (default 100_000_000)
        #[arg(long, default_value_t = 100_000_000)]
        fuel: u64,
        /// Tope de memoria en MB para la tarea WASM (default 16)
        #[arg(long, default_value_t = 16)]
        mem_mb: u32,
        /// Agrega una tarea SQL al DAG (B.2): la query corre dentro del grafo
        #[arg(long)]
        sql_file: Option<PathBuf>,
        /// Tablas para la tarea SQL: `nombre=ruta,otra=ruta2` (rutas Parquet)
        #[arg(long)]
        sql_tables: Option<String>,
        /// Salida (Parquet) del resultado de la tarea SQL
        #[arg(long)]
        sql_out: Option<PathBuf>,
    },
    /// Lista runs recientes (paginado)
    List {
        #[arg(long, default_value = "state")]
        state: PathBuf,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long, default_value_t = 0)]
        offset: usize,
    },
    /// GC: purga por retencion
    Gc {
        #[arg(long, default_value = "state")]
        state: PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
    /// Pipeline ETL puro (polars en Rust, sin zeruel) — solo benchmark
    Puro {
        #[arg(long, default_value = "data")]
        root: PathBuf,
    },
    /// Encola un run para el scheduler
    Enqueue {
        #[arg(long, default_value = "ecommerce-etl")]
        dag: String,
        #[arg(long, default_value = "demo")]
        param: String,
        #[arg(long, default_value_t = 0)]
        priority: i64,
        #[arg(long, default_value = "state")]
        state: PathBuf,
    },
    /// Tope de items en cola (cola acotada): mostrar, o fijar con --max N (0 quita el tope)
    Cola {
        /// Fija el tope de items en cola (0 quita el tope). Sin esto, muestra el tope y la cola.
        #[arg(long)]
        max: Option<usize>,
        #[arg(long, default_value = "state")]
        state: PathBuf,
    },
    /// Encola un run por dia en un rango de fechas
    Backfill {
        #[arg(long, default_value = "ecommerce-etl")]
        dag: String,
        #[arg(long)]
        desde: String,
        #[arg(long)]
        hasta: String,
        #[arg(long, default_value_t = 0)]
        priority: i64,
        #[arg(long, default_value = "state")]
        state: PathBuf,
    },
    /// Configura el maximo tiempo de espera en cola para un DAG (SLA)
    Sla {
        #[arg(long, default_value = "ecommerce-etl")]
        dag: String,
        /// Maximo tiempo en segundos que un run puede estar en cola
        #[arg(long)]
        max_wait: i64,
        #[arg(long, default_value = "state")]
        state: PathBuf,
    },
    /// Servicio de Windows: instalar, desinstalar y el modo que habla con el
    /// Administrador de servicios.
    ///
    /// En Linux esto no existe: ahi el servicio lo maneja systemd con la
    /// unidad de packaging/zeruel.service.
    #[cfg(windows)]
    Servicio {
        #[command(subcommand)]
        que: CmdServicio,
    },

    /// Scheduler: daemon que consume la cola lanzando binarios de pipeline
    ///
    /// Todo lo de aca se puede fijar en [zeruel] del zeruel.toml; el flag pisa al
    /// archivo, y el archivo pisa al default.
    Serve {
        /// Archivo de configuracion (por defecto ./zeruel.toml si existe)
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        state: Option<PathBuf>,
        /// Directorio con los binarios de pipeline. Se re-escanea en cada tick:
        /// copiar uno ahi alcanza para que aparezca, sin reiniciar el scheduler.
        #[arg(long)]
        pipelines: Option<PathBuf>,
        /// Tareas en paralelo DENTRO de cada run
        #[arg(long)]
        workers: Option<usize>,
        /// Runs en paralelo (subprocesos a la vez)
        #[arg(long)]
        paralelo: Option<usize>,
        /// Sale despues de N runs terminados (0 = daemon)
        #[arg(long, default_value_t = 0)]
        max: usize,
        #[arg(long)]
        tick_ms: Option<u64>,
        /// Corta un run que pase de estos segundos (0 = sin tope)
        #[arg(long)]
        timeout_secs: Option<u64>,
        /// Al apagar (SIGTERM/Ctrl+C), cuanto esperar a los runs en curso
        /// antes de cortarlos. Lo cortado vuelve a la cola.
        #[arg(long)]
        gracia_secs: Option<u64>,
        /// Techo de RAM por run: 2G, 512M, o un numero en MB (0 = sin tope).
        /// Un run que se pase se corta con un error que lo dice, en vez de
        /// dejar a la maquina swapeando.
        #[arg(long)]
        ram_max: Option<String>,
    },
    /// Estado de un run
    Status {
        #[arg(long, default_value = "state")]
        state: PathBuf,
        #[arg(long, default_value = "latest")]
        run: String,
    },
    /// Ejecuta un modulo .wasm en el sandbox (Fase 5: WASM)
    WasmRun {
        #[arg(long)]
        wasm: PathBuf,
        #[arg(long, default_value = "demo")]
        param: String,
        /// Tope de instrucciones WASM (fuel). Default 100_000_000
        #[arg(long, default_value_t = 100_000_000)]
        fuel: u64,
        /// Tope de memoria lineal en MB (paginas de 64KB). Default 16
        #[arg(long, default_value_t = 16)]
        mem_mb: u32,
    },
    /// UI web embebida (Fase 6): runs en vivo, metricas, alertas, cola y logs.
    /// Sirve desde el propio binario. No ejecuta pipelines: encola (los corre `serve`).
    Web {
        #[arg(long, default_value = "state")]
        state: PathBuf,
        #[arg(long, default_value_t = 8787)]
        port: u16,
        /// Interfaz donde escuchar. Por defecto solo local: la UI no tiene
        /// autenticacion, exponerla a la red tiene que ser explicito.
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        /// Sale despues de N requests (0 = para siempre). Utile para tests.
        #[arg(long, default_value_t = 0)]
        max: u32,
        /// Token que hay que presentar para usar la UI. Sin esto no pide nada.
        /// Tambien se puede fijar en [web] del zeruel.toml.
        #[arg(long)]
        token: Option<String>,
        /// Directorio con los binarios de pipeline. Tiene que ser el mismo que
        /// usa `zeruel serve`, o la web va a listar DAGs que el scheduler no
        /// puede ejecutar.
        #[arg(long, default_value = "pipelines")]
        pipelines: PathBuf,
    },

    /// MCP server por stdio (Fase IA-friendly): expone zeruel a herramientas de IA
    Mcp {
        #[arg(long, default_value = "state")]
        state: PathBuf,
    },
    /// Verifica el binario end-to-end (gen -> run -> status -> gc) en un dir temporal (Fase 7)
    SelfTest {
        #[arg(long, default_value_t = 200_000)]
        n: u32,
        #[arg(long, default_value_t = 4)]
        workers: usize,
        /// No borra el dir temporal al terminar
        #[arg(long)]
        keep: bool,
    },
    /// Genera una carpeta portable (binario + wrappers + docs + opcional data/state/docker/zip) (Fase 7)
    Deploy {
        #[arg(long, default_value = "zeruel-portable")]
        dest: PathBuf,
        /// Carpeta data/ a incluir (dataset de prueba)
        #[arg(long, alias = "with-data")]
        data: Option<PathBuf>,
        /// Carpeta state/ a incluir (estado actual)
        #[arg(long, alias = "with-state")]
        state: Option<PathBuf>,
        /// Genera Dockerfile + docker-run.sh
        #[arg(long)]
        docker: bool,
        /// Genera un .zip portable junto al destino
        #[arg(long)]
        zip: bool,
    },
}

impl Cmd {
    fn state(&self) -> Option<&PathBuf> {
        match self {
            Cmd::Run { state, .. }
            | Cmd::List { state, .. }
            | Cmd::Gc { state, .. }
            | Cmd::Enqueue { state, .. }
            | Cmd::Cola { state, .. }
            | Cmd::Backfill { state, .. }
            | Cmd::Sla { state, .. }
            | Cmd::Status { state, .. }
            | Cmd::Mcp { state, .. }
            | Cmd::Web { state, .. } => Some(state),
            // `serve` resuelve su state contra la config (flag > entorno >
            // archivo), asi que no puede contestar aca con el flag suelto.
            Cmd::Serve { state, .. } => state.as_ref(),
            #[cfg(windows)]
            Cmd::Servicio { que } => match que {
                CmdServicio::Instalar { state, .. } | CmdServicio::Correr { state, .. } => {
                    Some(state)
                }
                CmdServicio::Desinstalar { .. }
                | CmdServicio::Arrancar { .. }
                | CmdServicio::Parar { .. }
                | CmdServicio::Estado { .. } => None,
            },
            #[cfg(windows)]
            Cmd::Instalar { .. } => None,
            Cmd::Gen { .. }
            | Cmd::Doctor { .. }
            | Cmd::Config { .. }
            | Cmd::Init { .. }
            | Cmd::Puro { .. }
            | Cmd::WasmRun { .. }
            | Cmd::SelfTest { .. }
            | Cmd::Deploy { .. } => None,
        }
    }
}

#[derive(clap::Subcommand)]
enum CmdConfig {
    /// Cambia un ajuste: `zeruel config set paralelo 10`
    Set {
        /// El ajuste. `zeruel config` los lista.
        clave: String,
        valor: String,
    },
}

#[cfg(windows)]
#[derive(clap::Subcommand)]
enum CmdServicio {
    /// Registra el servicio en el sistema (necesita permisos de administrador)
    Instalar {
        /// Nombre del servicio. Es unico por maquina: hace falta cambiarlo para
        /// tener mas de una instancia (dev y produccion, o una por proyecto).
        #[arg(long, default_value = servicio::NOMBRE_DEFECTO)]
        nombre: String,
        /// Que corre este servicio. Se instalan por separado a proposito: la
        /// web no puede llevarse puesto al scheduler.
        #[arg(long, value_enum, default_value_t = servicio::Modo::Scheduler)]
        modo: servicio::Modo,
        /// Puerto de la interfaz (solo con --modo web)
        #[arg(long, default_value_t = 9876)]
        puerto: u16,
        #[arg(long, default_value = "state")]
        state: PathBuf,
        #[arg(long, default_value = "pipelines")]
        pipelines: PathBuf,
        /// Al parar, cuanto esperar a los runs en curso antes de cortarlos
        #[arg(long, default_value_t = 30)]
        gracia_secs: u64,
        /// Archivo de configuracion que va a usar el servicio.
        ///
        /// Se guarda en el registro del servicio, porque un servicio arranca
        /// desde System32 y ahi no hay ningun zeruel.toml que encontrar. Si no
        /// se pasa, se usa el que este al lado del ejecutable.
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Arranca el servicio (pide los permisos si hacen falta)
    Arrancar {
        #[arg(long, default_value = servicio::NOMBRE_DEFECTO)]
        nombre: String,
    },
    /// Para el servicio, esperando a los runs en curso
    Parar {
        #[arg(long, default_value = servicio::NOMBRE_DEFECTO)]
        nombre: String,
    },
    /// Estado del servicio (este no necesita permisos)
    Estado {
        #[arg(long, default_value = servicio::NOMBRE_DEFECTO)]
        nombre: String,
    },
    /// Saca el servicio del sistema (necesita permisos de administrador)
    Desinstalar {
        #[arg(long, default_value = servicio::NOMBRE_DEFECTO)]
        nombre: String,
    },
    /// Lo invoca Windows al arrancar el servicio. No es para ejecutar a mano:
    /// para correr en primer plano esta `zeruel serve`.
    #[command(hide = true)]
    Correr {
        #[arg(long, default_value = servicio::NOMBRE_DEFECTO)]
        nombre: String,
        #[arg(long, value_enum, default_value_t = servicio::Modo::Scheduler)]
        modo: servicio::Modo,
        #[arg(long, default_value_t = 9876)]
        puerto: u16,
        /// La ruta que guardo `servicio instalar`. Ver ahi por que hace falta.
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        pipelines: PathBuf,
        #[arg(long, default_value_t = 30)]
        gracia_secs: u64,
    },
}

/// El menu que aparece al hacer doble clic en `zeruel.exe`.
///
/// # Por que existe
///
/// El pedido fue "no quiero andar instalando por comandos". Un ejecutable que
/// al abrirse muestra opciones es lo mas parecido a un instalador comun sin
/// arrastrar un MSI ni una cadena de build que solo sirve en Windows.
///
/// # Los dos cuidados
///
/// - **Solo si hay terminal interactiva.** Un script, una tarea programada o
///   una tuberia tienen que seguir viendo la ayuda de siempre: cambiar eso
///   romperia a quien ya lo automatizo.
/// - **La ventana no se cierra sola.** Un instalador que hace doble clic,
///   parpadea y desaparece no deja leer si salio bien.
pub fn menu_interactivo() -> Result<()> {
    use std::io::Write;

    let destino = std::path::PathBuf::from(crate::instalador::DESTINO_DEFECTO);
    let instalada = crate::instalador::version_instalada(&destino);
    let propia = crate::instalador::version_propia();

    println!("\nzeruel {propia}\n");
    match &instalada {
        Some(v) if v == propia => {
            println!("  Ya instalado en {} (misma version)\n", destino.display())
        }
        Some(v) => println!("  Instalado en {}: version {v}\n", destino.display()),
        None => println!("  Sin instalar en esta maquina\n"),
    }

    println!(
        "  1) {}",
        match &instalada {
            Some(v) if v != propia => format!("Actualizar  ({v} -> {propia})"),
            Some(_) => "Reinstalar / reparar".to_string(),
            None => "Instalar en esta maquina".to_string(),
        }
    );
    println!("  2) Ver el estado de los servicios");
    println!("  3) Probar las herramientas (doctor)");
    println!("  4) Salir\n");
    print!("Elegi una opcion [1]: ");
    let _ = std::io::stdout().flush();

    let mut linea = String::new();
    let _ = std::io::stdin().read_line(&mut linea);
    let opcion = linea.trim();

    let r = match opcion {
        "" | "1" => {
            crate::instalador::instalar(Some(destino.clone()), servicio::NOMBRE_DEFECTO, 9876)
        }
        "2" => {
            for s in [
                servicio::NOMBRE_DEFECTO.to_string(),
                format!("{}-web", servicio::NOMBRE_DEFECTO),
            ] {
                if let Err(e) = servicio::estado(&s) {
                    println!("  {s}: {e}");
                }
            }
            Ok(())
        }
        "3" => {
            let cfg = config::Config::cargar(Some(&destino.join(config::NOMBRE_DEFAULT)))?;
            dependencias::chequear_y_reportar(&cfg.entorno);
            Ok(())
        }
        _ => Ok(()),
    };

    if let Err(e) = &r {
        println!("\nNo se pudo completar: {e:#}");
    }
    // Sin esto la ventana se cierra sola y no se lee nada de lo de arriba.
    println!("\nEnter para cerrar.");
    let mut _x = String::new();
    let _ = std::io::stdin().read_line(&mut _x);
    r
}

/// ¿Se abrio con doble clic?
///
/// La señal es que no hay argumentos Y hay una terminal del otro lado. Sin
/// terminal —un script, una tarea programada, una tuberia— no hay nadie para
/// contestar un menu, y hay que comportarse como siempre.
#[cfg(windows)]
fn abierto_con_doble_clic() -> bool {
    use std::io::IsTerminal;
    std::env::args().count() == 1
        && std::io::stdin().is_terminal()
        && std::io::stdout().is_terminal()
}

fn main() -> Result<()> {
    // Doble clic: sin argumentos y con una terminal del otro lado. `Cli::parse`
    // aca imprimiria la ayuda y cerraria la ventana antes de que se lea, que es
    // exactamente lo que hace que un ejecutable parezca roto.
    #[cfg(windows)]
    if abierto_con_doble_clic() {
        init_logs(None, false);
        return menu_interactivo();
    }
    let cli = Cli::parse();
    let json = cli.json;
    // `mcp` habla JSON-RPC por stdout: un solo renglon de log ahi rompe el
    // protocolo y el cliente deja de poder parsear. Sus logs van a stderr.
    let a_stderr = matches!(cli.cmd, Cmd::Mcp { .. });
    init_logs(cli.cmd.state().map(|p| p.as_path()), a_stderr);
    match cli.cmd {
        #[cfg(windows)]
        Cmd::Instalar {
            destino,
            nombre,
            puerto,
        } => instalador::instalar(destino, &nombre, puerto),
        Cmd::Config { config, que } => {
            // Se busca el archivo REAL, no la configuracion ya cargada: para
            // editar hace falta la ruta, y `Config::cargar` cae a los valores
            // de fabrica cuando no encuentra nada — que es justo lo que aca no
            // se puede hacer en silencio.
            let ruta = config
                .or_else(config::Config::buscar_junto_al_ejecutable)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no encontre ningun {}. Pasalo con --config <ruta>.",
                        config::NOMBRE_DEFAULT
                    )
                })?;
            match que {
                Some(CmdConfig::Set { clave, valor }) => configurar::fijar(&ruta, &clave, &valor),
                None => configurar::mostrar(&ruta),
            }
        }
        Cmd::Doctor { config } => {
            let cfg = config::Config::cargar(config.as_deref())?;
            if cfg.entorno.is_empty() {
                println!("no hay [entorno] en la configuracion: nada que probar.");
                return Ok(());
            }
            println!(
                "probando las herramientas de [entorno]...
"
            );
            let r = dependencias::chequear(&cfg.entorno);
            if r.is_empty() {
                println!("ninguna entrada de [entorno] apunta a un ejecutable.");
                return Ok(());
            }
            let mut problemas = 0;
            for c in &r {
                match &c.estado {
                    dependencias::Estado::Bien(v) => {
                        println!("  OK     {:<22} {v}", c.variable)
                    }
                    dependencias::Estado::Dudoso { codigo, detalle } => {
                        problemas += 1;
                        println!("  DUDA   {:<22} salio con codigo {codigo}", c.variable);
                        println!("         {}", c.programa);
                        println!("         {detalle}");
                    }
                    dependencias::Estado::NoArranca(e) => {
                        problemas += 1;
                        println!("  FALLA  {:<22} no se puede ejecutar", c.variable);
                        println!("         {}", c.programa);
                        println!("         {e}");
                    }
                }
            }
            println!();
            if problemas > 0 {
                println!(
                    "{problemas} de {} no responden como se espera. Los pipelines que las \
                     usen van a fallar.",
                    r.len()
                );
                // La cuenta virtual del servicio no tiene permiso sobre nada que
                // no se le haya concedido, y eso incluye a los interpretes: es
                // la causa mas comun de que una herramienta que "esta ahi" no se
                // pueda ejecutar. Se da el comando entero para no obligar a
                // buscarlo.
                if cfg!(windows) {
                    println!(
                        "\nSi zeruel corre como servicio, lo mas comun es que su cuenta no \
                         tenga permiso sobre esa ruta:"
                    );
                    println!(r#"  icacls "<ruta>" /grant "NT SERVICE\<servicio>:(OI)(CI)RX" /T"#);
                }
                std::process::exit(1);
            }
            println!("las {} herramientas responden bien.", r.len());
            Ok(())
        }
        Cmd::Init { salida, si, forzar } => init::ejecutar(salida, si, forzar),
        Cmd::Gen { root, n } => cmd_gen(root, n, json),
        Cmd::Run {
            root,
            state,
            param,
            workers,
            fresh,
            wasm,
            fuel,
            mem_mb,
            sql_file,
            sql_tables,
            sql_out,
        } => cmd_run(
            root,
            state,
            param,
            workers,
            fresh,
            RunOpts {
                wasm,
                fuel,
                mem_mb,
                sql_file,
                sql_tables,
                sql_out,
            },
            json,
        ),
        Cmd::List {
            state,
            limit,
            offset,
        } => cmd_list(state, limit, offset, json),
        Cmd::Gc { state, dry_run } => cmd_gc(state, dry_run, json),
        Cmd::Enqueue {
            dag,
            param,
            priority,
            state,
        } => cmd_enqueue(state, dag, param, priority, json),
        Cmd::Cola { max, state } => cmd_cola(state, max, json),
        Cmd::Backfill {
            dag,
            desde,
            hasta,
            priority,
            state,
        } => cmd_backfill(state, dag, desde, hasta, priority, json),
        Cmd::Sla {
            dag,
            max_wait,
            state,
        } => cmd_sla(state, dag, max_wait, json),
        Cmd::Serve {
            config: ruta_config,
            state,
            pipelines,
            workers,
            paralelo,
            max,
            tick_ms,
            timeout_secs,
            gracia_secs,
            ram_max,
        } => {
            let cfg = config::Config::cargar(ruta_config.as_deref())?;
            // Las variables de [entorno] se exportan ANTES de arrancar: los
            // subprocesos de pipeline las heredan, y asi les llegan las cadenas
            // de conexion sin que cada uno lea la configuracion.
            cfg.aplicar_entorno();
            // Antes de lanzar nada: probar las herramientas de [entorno] y
            // reportarlas TODAS juntas. Sin esto, una que no se puede ejecutar
            // se descubre corrida por corrida, y cada fallo llega a nombre del
            // DAG y no del problema.
            dependencias::chequear_y_reportar(&cfg.entorno);

            let techo = config::elegir(
                ram_max,
                std::env::var("ZERUEL_RAM_MAX").ok(),
                cfg.zeruel.ram_max.clone(),
                "0".to_string(),
            );
            scheduler::serve(scheduler::ServeArgs {
                state: config::elegir(
                    state,
                    config::de_entorno("ZERUEL_STATE"),
                    cfg.zeruel.state.clone(),
                    PathBuf::from("state"),
                ),
                pipelines: config::elegir(
                    pipelines,
                    config::de_entorno("ZERUEL_PIPELINES"),
                    cfg.zeruel.pipelines.clone(),
                    PathBuf::from("pipelines"),
                ),
                workers: config::elegir(
                    workers,
                    config::de_entorno("ZERUEL_WORKERS"),
                    cfg.zeruel.workers,
                    4,
                ),
                paralelo: config::elegir(
                    paralelo,
                    config::de_entorno("ZERUEL_PARALELO"),
                    cfg.zeruel.paralelo,
                    2,
                ),
                max,
                tick_ms: config::elegir(tick_ms, None, cfg.zeruel.tick_ms, 1000),
                timeout_secs: config::elegir(timeout_secs, None, cfg.zeruel.timeout_secs, 0),
                retencion_detalle_dias: cfg
                    .zeruel
                    .retencion_detalle_dias
                    .unwrap_or(zeruel_core::store::RETENCION_DETALLE_DIAS),
                retencion_runs_dias: cfg
                    .zeruel
                    .retencion_runs_dias
                    .unwrap_or(zeruel_core::store::RETENCION_RUNS_DIAS),
                gracia_secs: config::elegir(gracia_secs, None, cfg.zeruel.gracia_secs, 30),
                ram_max_mb: zeruel::parsear_memoria(&techo).map_err(|e| anyhow::anyhow!("{e}"))?,
                config_origen: cfg.origen().map(|p| p.to_path_buf()),
            })
        }
        #[cfg(windows)]
        Cmd::Servicio { que } => match que {
            CmdServicio::Instalar {
                nombre,
                modo,
                puerto,
                state,
                pipelines,
                gracia_secs,
                config,
            } => servicio::instalar(
                &nombre,
                modo,
                &state,
                &pipelines,
                gracia_secs,
                puerto,
                config.as_deref(),
            ),
            CmdServicio::Arrancar { nombre } => servicio::arrancar(&nombre),
            CmdServicio::Parar { nombre } => servicio::parar(&nombre),
            CmdServicio::Estado { nombre } => servicio::estado(&nombre),
            CmdServicio::Desinstalar { nombre } => servicio::desinstalar(&nombre),
            CmdServicio::Correr {
                nombre,
                modo,
                puerto,
                config: ruta_config,
                state,
                pipelines,
                gracia_secs,
            } => {
                // El servicio arranca desde System32 y sin zeruel.toml a la vista,
                // asi que todo lo que necesita viene por argumentos: son los que
                // quedaron guardados en el registro al instalarlo, incluida la
                // ruta del archivo de configuracion.
                let cfg = match &ruta_config {
                    // Con ruta explicita se falla si no se puede leer, en vez de
                    // arrancar con la configuracion vacia: un servicio que
                    // ignora en silencio su config es peor que uno que no
                    // arranca, porque los pipelines fallan uno por uno y el
                    // motivo aparece a nombre de ellos.
                    Some(r) => config::Config::cargar(Some(r.as_path()))?,
                    // Sin ruta explicita —un servicio registrado antes de que
                    // `--config` existiera— hay que buscarla, y NO en el
                    // directorio actual: el de un servicio es System32.
                    //
                    // Este caso estuvo dos dias corriendo con la configuracion
                    // de fabrica sin decir una palabra: paralelo=2 en vez de 6,
                    // sin techo de RAM y sin tope de tiempo. La cola llego a
                    // 1.400 items y el sintoma aparecio lejisimos de la causa.
                    None => match config::Config::buscar_junto_al_ejecutable() {
                        Some(p) => config::Config::cargar(Some(p.as_path()))?,
                        None => {
                            tracing::error!(
                                "el servicio '{nombre}' se registro SIN --config y no hay                                  ningun {} al lado del ejecutable. Arranca con la                                  configuracion de fabrica, que NO es la que escribiste:                                  paralelo=2, sin techo de RAM y sin tope de tiempo.                                  Reinstalalo con `zeruel servicio instalar --config <ruta>`.",
                                config::NOMBRE_DEFAULT
                            );
                            config::Config::default()
                        }
                    },
                };
                // Lo que hacia falta y no se hacia: exportar `[entorno]` antes
                // de lanzar nada. `zeruel serve` lo hace; el servicio no lo
                // hacia, y por eso los pipelines arrancaban sin sus variables.
                cfg.aplicar_entorno();
                // Antes de lanzar nada: probar las herramientas de [entorno] y
                // reportarlas TODAS juntas. Sin esto, una que no se puede ejecutar
                // se descubre corrida por corrida, y cada fallo llega a nombre del
                // DAG y no del problema.
                dependencias::chequear_y_reportar(&cfg.entorno);
                servicio::correr(
                    &nombre,
                    modo,
                    puerto,
                    scheduler::ServeArgs {
                        state,
                        pipelines,
                        workers: config::elegir(None, None, cfg.zeruel.workers, 4),
                        paralelo: config::elegir(None, None, cfg.zeruel.paralelo, 2),
                        max: 0,
                        tick_ms: config::elegir(None, None, cfg.zeruel.tick_ms, 1000),
                        timeout_secs: config::elegir(None, None, cfg.zeruel.timeout_secs, 0),
                        retencion_detalle_dias: cfg
                            .zeruel
                            .retencion_detalle_dias
                            .unwrap_or(zeruel_core::store::RETENCION_DETALLE_DIAS),
                        retencion_runs_dias: cfg
                            .zeruel
                            .retencion_runs_dias
                            .unwrap_or(zeruel_core::store::RETENCION_RUNS_DIAS),
                        gracia_secs,
                        ram_max_mb: zeruel::parsear_memoria(
                            cfg.zeruel.ram_max.as_deref().unwrap_or("0"),
                        )
                        .map_err(|e| anyhow::anyhow!("{e}"))?,
                        config_origen: cfg.origen().map(|p| p.to_path_buf()),
                    },
                )
            }
        },
        Cmd::Status { state, run } => cmd_status(state, run, json),
        Cmd::WasmRun {
            wasm,
            param,
            fuel,
            mem_mb,
        } => cmd_wasm_run(wasm, param, fuel, mem_mb, json),
        Cmd::Mcp { state } => mcp::run(state),
        Cmd::Web {
            state,
            pipelines,
            port,
            bind,
            max,
            token,
        } => {
            let cfg = config::Config::cargar(None)?;
            // La web arma su lista corriendo cada binario con `--describe`, y
            // esos subprocesos heredan el entorno de ESTE proceso. Sin aplicar
            // `[entorno]`, un pipeline que decide que publicar segun una
            // variable -por ejemplo, cuales DAGs excluir- le contesta una cosa
            // al scheduler y otra a la web: el panel termina listando DAGs que
            // el scheduler no va a ejecutar nunca.
            cfg.aplicar_entorno();
            // Se saca antes de que `cfg` se desarme abajo al mover el token.
            let origen_config = cfg.origen().map(|p| p.to_path_buf());
            web::run(
                state,
                pipelines,
                config::elegir(None, config::de_entorno("ZERUEL_PORT"), cfg.web.port, port),
                max,
                &config::elegir(
                    None,
                    std::env::var("ZERUEL_BIND").ok(),
                    cfg.web.bind.clone(),
                    bind,
                ),
                token
                    .or_else(|| std::env::var("ZERUEL_TOKEN").ok())
                    .or(cfg.web.token),
                origen_config,
            )
        }
        Cmd::Puro { root } => puro::run(&root.to_string_lossy()),
        Cmd::SelfTest { n, workers, keep } => deploy::self_test(n, workers, keep),
        Cmd::Deploy {
            dest,
            data,
            state,
            docker,
            zip,
        } => deploy::deploy(deploy::DeployArgs {
            dest,
            data,
            state,
            docker,
            zip,
        }),
    }
}

/// Arma el logging: consola siempre, archivo rotado si hay un `state`.
///
/// `a_stderr` manda la consola a stderr en vez de stdout. Hace falta para
/// `zeruel mcp`, que usa stdout para el protocolo JSON-RPC: cualquier linea de
/// log ahi se mezcla con los mensajes y el cliente no puede parsear nada. Es un
/// fallo silencioso — la herramienta "no anda" y no hay error que lo explique.
fn init_logs(state: Option<&std::path::Path>, a_stderr: bool) {
    use tracing_appender::rolling::{RollingFileAppender, Rotation};
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Sin colores en stderr: quien lee ese canal suele ser un programa.
    let consola = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_ansi(!a_stderr)
        .with_writer(move || -> Box<dyn std::io::Write> {
            if a_stderr {
                Box::new(std::io::stderr())
            } else {
                Box::new(std::io::stdout())
            }
        });

    // El archivo es opcional: si no hay `state`, o si no se puede escribir, se
    // sigue con la consola. Que no se pueda guardar un log no es motivo para no
    // ejecutar el comando (N-56).
    let archivo = state.and_then(|s| {
        let dir = s.join("logs");
        std::fs::create_dir_all(&dir).ok();
        prune_old_logs(&dir, 14);
        RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_prefix("zeruel.log")
            .build(&dir)
            .map_err(|e| eprintln!("no puedo escribir el log en {}: {e}", dir.display()))
            .ok()
    });

    tracing_subscriber::registry()
        .with(filter)
        .with(consola)
        .with(archivo.map(|f| {
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(f)
        }))
        .init();
}

fn prune_old_logs(dir: &std::path::Path, dias: u64) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if !name.starts_with("zeruel.log.") {
                continue;
            }
            let age = e
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if age >= dias * 86_400 {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
}

fn cmd_wasm_run(wasm: PathBuf, param: String, fuel: u64, mem_mb: u32, json: bool) -> Result<()> {
    let task = zeruel_core::WasmTask::new("wasm", &wasm, fuel, mem_mb)?;
    let ctx = zeruel_sdk::TaskContext {
        run_id: Uuid::new_v4(),
        param: param.clone(),
        cancel: None,
    };
    let t0 = std::time::Instant::now();
    let out = task.run(&ctx)?;
    let secs = t0.elapsed().as_secs_f64();
    if json {
        println!(
            "{}",
            serde_json::json!({
                "ok": true, "rows": out.rows, "mensaje": out.message, "segundos": secs,
                "fuel": fuel, "mem_mb": mem_mb, "wasm": wasm.to_string_lossy().to_string(),
            })
        );
    } else {
        println!("wasm OK: rows={} en {secs:.2}s — {}", out.rows, out.message);
    }
    Ok(())
}

fn cmd_gen(root: PathBuf, n: u32, json: bool) -> Result<()> {
    let env = DataEnv::new(&root);
    let ds = dataset::Dataset::con_transacciones(n);
    let t0 = Instant::now();
    let total = ds.generate(&env)?;
    let secs = t0.elapsed().as_secs_f64();
    if json {
        println!(
            "{}",
            serde_json::json!({
                "filas": total, "segundos": secs, "root": root.to_string_lossy()
            })
        );
    } else {
        println!(
            "dataset generado: {} filas en {secs:.2}s (root: {})",
            total,
            root.display()
        );
    }
    Ok(())
}

fn cmd_run(
    root: PathBuf,
    state: PathBuf,
    param: String,
    workers: usize,
    fresh: bool,
    opts: RunOpts,
    json: bool,
) -> Result<()> {
    let env = DataEnv::new(&root);
    if fresh && state.exists() {
        std::fs::remove_dir_all(&state)?;
    }
    let store = Arc::new(Store::open(&state)?);
    let wasm_spec: Option<zeruel_sdk::WasmSpec> =
        opts.wasm.as_ref().map(|p| zeruel_sdk::WasmSpec {
            path: p.clone(),
            fuel: opts.fuel,
            mem_mb: opts.mem_mb,
        });
    let sql_spec = build_sql_spec(&env, &opts)?;
    let t0 = Instant::now();
    let report = pipeline::run(
        &env,
        &store,
        workers,
        &param,
        wasm_spec.as_ref(),
        sql_spec.as_ref(),
    )?;
    let secs = t0.elapsed().as_secs_f64();
    if json {
        println!(
            "{}",
            serde_json::json!({
                "run_id": report.run_id.to_string(),
                "ok": report.success,
                "segundos": secs,
                "filas": report.total_rows,
                "skipped": report.skipped,
                "wasm": opts.wasm.as_ref().map(|p| p.to_string_lossy().to_string()),
                "sql": sql_spec.as_ref().map(|s| s.sql.clone()),
            })
        );
    } else {
        let mut extra = String::new();
        if wasm_spec.is_some() {
            extra.push_str(" +wasm");
        }
        if sql_spec.is_some() {
            extra.push_str(" +sql");
        }
        println!(
            "run {} => {} ({} s, {} filas, {} skip){}",
            report.run_id,
            if report.success { "OK" } else { "FAIL" },
            secs,
            report.total_rows,
            report.skipped,
            extra
        );
    }
    if !report.success {
        std::process::exit(1);
    }
    Ok(())
}

/// Opciones opt-in de tareas extra en `zeruel run` (B.1 wasm + B.2 sql).
/// Agrupa los args para no inflar la firma de `cmd_run` (deuda B-01).
struct RunOpts {
    wasm: Option<PathBuf>,
    fuel: u64,
    mem_mb: u32,
    sql_file: Option<PathBuf>,
    sql_tables: Option<String>,
    sql_out: Option<PathBuf>,
}

/// Construye la spec de la tarea SQL opt-in desde los args de la CLI.
/// `sql_file` = la query; `sql_tables` = nombre=ruta,... (relativas al root
/// si no son absolutas); `sql_out` = salida Parquet (default output/sql_result.parquet).
fn build_sql_spec(env: &DataEnv, opts: &RunOpts) -> Result<Option<zeruel_sdk::SqlSpec>> {
    let (Some(sql_file), Some(tables_spec)) = (&opts.sql_file, &opts.sql_tables) else {
        // sin --sql-file ni --sql-tables: no hay tarea SQL (opt-in)
        return Ok(None);
    };
    let sql = std::fs::read_to_string(sql_file)
        .with_context(|| format!("no puedo leer la query SQL: {}", sql_file.display()))?;
    let out = match &opts.sql_out {
        Some(p) if p.is_absolute() => p.clone(),
        Some(p) => env.base.join(p),
        None => env.base.join("output/sql_result.parquet"),
    };
    let mut tables = std::collections::HashMap::new();
    for (nombre, ruta) in pipeline::parse_sql_tables(tables_spec)? {
        let abs = if ruta.is_absolute() {
            ruta
        } else {
            env.base.join(&ruta)
        };
        tables.insert(nombre, abs);
    }
    Ok(Some(zeruel_sdk::SqlSpec {
        sql,
        tables,
        output: out,
    }))
}

fn run_json(r: &Run) -> serde_json::Value {
    serde_json::json!({
        "id": r.id.to_string(),
        "dag": r.dag,
        "param": r.param,
        "state": format!("{:?}", r.state),
        "started_at": r.started_at,
        "finished_at": r.finished_at,
    })
}

fn cmd_list(state: PathBuf, limit: usize, offset: usize, json: bool) -> Result<()> {
    let store = Store::open(&state)?;
    let runs = store.recent_runs(limit, offset)?;
    if json {
        println!(
            "{}",
            serde_json::json!({ "runs": runs.iter().map(run_json).collect::<Vec<_>>() })
        );
        return Ok(());
    }
    if runs.is_empty() {
        println!("sin runs (state: {}, offset={})", state.display(), offset);
        return Ok(());
    }
    for r in runs {
        println!(
            "{}  {:<20} {:<8}  start={}",
            r.id,
            r.dag,
            r.state,
            chrono::DateTime::<chrono::Utc>::from_timestamp(r.started_at, 0)
                .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_default()
        );
    }
    Ok(())
}

fn cmd_enqueue(
    state: PathBuf,
    dag: String,
    param: String,
    priority: i64,
    json: bool,
) -> Result<()> {
    let store = Store::open(&state)?;
    let r = store.enqueue(&dag, &param, priority)?;
    if json {
        println!("{}", serde_json::json!({ "ok": r.ok, "motivo": r.motivo }));
    } else if r.ok {
        println!("encolado: dag={} param={} prio={}", dag, param, priority);
    } else {
        println!("no encolado: {}", r.motivo);
    }
    Ok(())
}

fn cmd_backfill(
    state: PathBuf,
    dag: String,
    desde: String,
    hasta: String,
    priority: i64,
    json: bool,
) -> Result<()> {
    let store = Store::open(&state)?;
    let rep = ops::backfill(&store, &dag, &desde, &hasta, priority)?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "dag": rep.dag, "desde": rep.desde, "hasta": rep.hasta,
                "encolados": rep.encolados, "totales": rep.totales, "skipped": rep.skipped,
            })
        );
    } else {
        println!(
            "backfill {}: {}/{} encolados ({}..{}, prio={priority})",
            rep.dag, rep.encolados, rep.totales, rep.desde, rep.hasta
        );
        for s in &rep.skipped {
            println!("  skip: {s}");
        }
    }
    Ok(())
}

/// Muestra / fija el tope de items en cola (cola acotada). Sin args: muestra
/// el tope actual y el total en cola. `--max N` fija el tope (0 lo quita).
/// El tope vive en `meta queue.max_size`; `enqueue` lo respeta.
fn cmd_cola(state: PathBuf, max: Option<usize>, json: bool) -> Result<()> {
    let store = Store::open(&state)?;
    if let Some(n) = max {
        ops::set_queue_max(&store, n)?;
        if json {
            println!("{}", serde_json::json!({ "max_size": n }));
        } else {
            println!(
                "tope de cola fijado a {n} items en cola{}",
                if n == 0 { " (sin tope)" } else { "" }
            );
        }
        return Ok(());
    }
    let tope = ops::get_queue_max(&store);
    let total = store.queue_items()?.len() as i64;
    if json {
        println!(
            "{}",
            serde_json::json!({ "max_size": tope, "en_cola": total })
        );
    } else {
        let tope_s = if tope == 0 {
            "sin tope".to_string()
        } else {
            format!("{tope} items")
        };
        println!("tope de cola: {tope_s}");
        println!("en cola ahora: {total}");
    }
    Ok(())
}

fn cmd_sla(state: PathBuf, dag: String, max_wait: i64, json: bool) -> Result<()> {
    let store = Store::open(&state)?;
    let tope = ops::set_sla(&store, &dag, max_wait)?;
    if json {
        println!(
            "{}",
            serde_json::json!({ "dag": dag, "max_wait_seg": tope })
        );
    } else {
        match tope {
            Some(n) => println!("SLA {dag}: un run no puede esperar mas de {n}s en cola"),
            None => println!("SLA {dag}: tope borrado (sin limite de espera)"),
        }
    }
    Ok(())
}

/// Items de la cola que exceden su tope de espera (SLA) configurado por DAG.
/// Un item viola si `sla:<dag>` existe y su espera actual es mayor al tope.
fn sla_violaciones(store: &Store, items: &[QueueItem]) -> Vec<(QueueItem, i64, i64)> {
    let mut out = Vec::new();
    for it in items {
        if let Some(max) = ops::get_sla(store, &it.dag) {
            let espera = ms_cola(it.enqueued_at);
            if espera > max {
                out.push((it.clone(), espera, max));
            }
        }
    }
    out
}

fn cmd_gc(state: PathBuf, dry_run: bool, json: bool) -> Result<()> {
    let store = Store::open(&state)?;
    let rep = store.gc(dry_run)?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "dry_run": dry_run,
                "runs_borrados": rep.runs_borrados,
                "task_runs_borrados": rep.task_runs_borrados,
                "meses_agregados": rep.meses_agregados,
                "db_bytes": rep.db_size_bytes,
            })
        );
    } else {
        let modo = if dry_run { "dry-run" } else { "ejecutado" };
        println!(
            "GC ({modo}): runs_borrados={} task_runs_borrados={} meses={} db={:.1} MB",
            rep.runs_borrados,
            rep.task_runs_borrados,
            rep.meses_agregados,
            rep.db_size_bytes as f64 / 1_048_576.0
        );
    }
    Ok(())
}

fn hygiene_json(h: &Hygiene) -> serde_json::Value {
    serde_json::json!({
        "db_bytes": h.db_size_bytes,
        "runs": h.runs,
        "task_runs": h.task_runs,
        "meses": h.meses,
        "ultima_gc": h.ultima_gc,
        "db_mb_warning": h.db_mb_warning,
    })
}

fn queue_json(store: &Store, q: &[QueueItem]) -> serde_json::Value {
    let viol = sla_violaciones(store, q);
    serde_json::json!({
        "items": q.iter().map(|i| serde_json::json!({
            "id": i.id, "dag": i.dag, "param": i.param,
            "priority": i.priority, "state": i.state,
            "espera_seg": ms_cola(i.enqueued_at),
        })).collect::<Vec<_>>(),
        "sla_violaciones": viol.iter().map(|(i, esp, max)| serde_json::json!({
            "id": i.id, "dag": i.dag, "param": i.param,
            "espera_seg": esp, "max_seg": max,
        })).collect::<Vec<_>>(),
    })
}

fn print_hygiene(store: &Store) -> Result<()> {
    let h = store.hygiene()?;
    let mb = h.db_size_bytes as f64 / 1_048_576.0;
    let warn = if h.db_mb_warning {
        "  [!] DB > 512MB: corre zeruel gc"
    } else {
        ""
    };
    println!(
        "higiene: db={:.1}MB runs={} task_runs={} meses={} ultima_gc={}{}",
        mb,
        h.runs,
        h.task_runs,
        h.meses,
        h.ultima_gc
            .and_then(|t| chrono::DateTime::<chrono::Utc>::from_timestamp(t, 0)
                .map(|d| d.format("%Y-%m-%d %H:%M").to_string()))
            .unwrap_or_else(|| "nunca".into()),
        warn
    );
    Ok(())
}

fn print_cola(store: &Store) -> Result<()> {
    let q = store.queue_items()?;
    if q.is_empty() {
        return Ok(());
    }
    println!();
    println!("cola ({} items):", q.len());
    for item in &q {
        let wait = ms_cola(item.enqueued_at);
        println!(
            "  #{:<6} {:<18} {:<10} prio={:<4} state={:<8} espera={}s",
            item.id, item.dag, item.param, item.priority, item.state, wait
        );
    }
    let viol = sla_violaciones(store, &q);
    if !viol.is_empty() {
        println!();
        println!(
            "  [SLA] {} item(s) superan su maximo de espera:",
            viol.len()
        );
        for (item, espera, max) in &viol {
            println!(
                "    #{} {}: lleva {}s en cola (tope {}s)",
                item.id, item.param, espera, max
            );
        }
    }
    Ok(())
}

fn ms_cola(enq: i64) -> i64 {
    (chrono::Utc::now().timestamp() - enq).max(0)
}

fn cmd_status(state: PathBuf, run: String, json: bool) -> Result<()> {
    let store = Store::open(&state)?;
    // El run que se pide es el que se muestra. Antes las tasks salian del id
    // pedido pero la cabecera del JSON salia SIEMPRE del run mas reciente
    // (N-03), asi que `status --run <viejo>` devolvia datos cruzados; y el
    // `.unwrap()` de esa linea panicaba con la tabla de runs vacia.
    let r: Run = if run == "latest" {
        store
            .recent_runs(1, 0)?
            .into_iter()
            .next()
            .context("no hay runs todavia (corre `zeruel run` primero)")?
    } else {
        let id: Uuid = run
            .parse()
            .with_context(|| format!("--run tiene que ser un UUID o 'latest' (vino {run:?})"))?;
        store
            .run_by_id(id)?
            .with_context(|| format!("no existe el run {id}"))?
    };
    let id = r.id;
    let tasks = store.task_runs(id)?;
    if json {
        let h = store.hygiene()?;
        let q = store.queue_items()?;
        println!(
            "{}",
            serde_json::json!({
                "run": run_json(&r),
                "tasks": tasks.iter().map(|t| serde_json::json!({
                    "task": t.task,
                    "state": state_str(&t.state),
                    "rows": t.rows,
                    "ram_peak_mb": t.ram_peak_mb,
                    "cpu_secs": t.cpu_secs,
                    "mensaje": t.message,
                })).collect::<Vec<_>>(),
                "higiene": hygiene_json(&h),
                "cola": queue_json(&store, &q),
            })
        );
        return Ok(());
    }
    let mut ts: Vec<_> = tasks;
    ts.sort_by_key(|t| std::cmp::Reverse(t.ram_peak_mb));
    println!(
        "{:<20} {:<8} {:>9} {:>8} {:>8}  mensaje",
        "tarea", "estado", "filas", "ramMB", "cpuS"
    );
    for t in &ts {
        println!(
            "{:<20} {:<8} {:>9} {:>8} {:>8.0}  {}",
            t.task,
            state_str(&t.state),
            t.rows,
            t.ram_peak_mb,
            t.cpu_secs,
            t.message
        );
    }
    println!();
    print_hygiene(&store)?;
    print_cola(&store)?;
    Ok(())
}

fn state_str(s: &zeruel_sdk::TaskState) -> &'static str {
    match s {
        zeruel_sdk::TaskState::Pending => "Pendiente",
        zeruel_sdk::TaskState::Running => "Corriendo",
        zeruel_sdk::TaskState::Success => "OK",
        zeruel_sdk::TaskState::Failed { .. } => "Fallo",
    }
}
