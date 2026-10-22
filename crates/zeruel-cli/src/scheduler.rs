//! Scheduler de zeruel: `zeruel serve` consume la cola global.
//!
//! **No ejecuta pipelines en su proceso: los lanza como subprocesos.** Un
//! pipeline es un binario suelto en el directorio de pipelines (ver
//! [`crate::pipelines`]), asi que:
//!
//! - sumar uno es copiar un archivo — el scheduler lo detecta sin reiniciarse;
//! - un panic o un OOM se lleva puesto ese run y nada mas;
//! - varios runs corren de verdad en paralelo, cada uno en su proceso;
//! - la medicion de RAM pasa a ser exacta, porque el proceso **es** el run.
//!
//! La coordinacion es el store SQLite, que ya funciona entre procesos.
//!
//! Politica de la cola (la resuelve `Store::pick_next`):
//! - prioridad efectiva = priority base + aging (no hay hambre)
//! - FIFO dentro de la misma prioridad efectiva
//! - conflicto: si un run del mismo DAG+param ya esta Running, el de la cola espera
//! - admission control: si la RAM estimada del DAG + 20% no entra en la
//!   disponible, se posterga — pero **con plazo**. Pasados 10 minutos se lanza
//!   igual: postergar para siempre no protege nada, y quien cuida la maquina de
//!   verdad es `ram_max`, que corta el run mientras corre.
//!
//! `--workers` es el paralelismo DENTRO de un run (tareas a la vez);
//! `--paralelo`, cuantos runs a la vez.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use zeruel_core::{RunState, Store};

use crate::pipelines::{self, EnCurso, Pipeline, Registro};

#[derive(Clone)]
pub struct ServeArgs {
    pub state: PathBuf,
    /// Directorio con los binarios de pipeline.
    pub pipelines: PathBuf,
    /// Tareas en paralelo DENTRO de un run.
    pub workers: usize,
    /// Runs en paralelo (subprocesos a la vez).
    pub paralelo: usize,
    /// Sale despues de N runs terminados (0 = daemon).
    pub max: usize,
    pub tick_ms: u64,
    /// Corta un run que pase de estos segundos (0 = sin tope).
    pub timeout_secs: u64,
    /// Al apagar, cuanto se espera a los runs en curso antes de cortarlos.
    /// systemd manda SIGKILL a los 90s por defecto, asi que pasarse de ahi
    /// no sirve: el proceso muere igual y sin poder reencolar nada.
    pub gracia_secs: u64,
    /// Dias que se guarda el detalle por tarea.
    pub retencion_detalle_dias: i64,
    /// Dias que se guarda el listado de corridas.
    pub retencion_runs_dias: i64,
    /// Techo de RAM por run, en MB (0 = sin techo).
    ///
    /// Se le pasa a cada subproceso. Como cada run es un proceso propio, el
    /// techo es por run de verdad — no un promedio de varios mezclados.
    pub ram_max_mb: u64,
    /// De que archivo salio la configuracion. `None` = de fabrica.
    ///
    /// Se arrastra hasta aca solo para poder decirlo al arrancar: un scheduler
    /// que quedo con los valores por defecto porque no encontro su archivo se
    /// ve, desde afuera, exactamente igual que uno bien configurado.
    pub config_origen: Option<PathBuf>,
}

fn ram_disponible_mb() -> u64 {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_memory();
    (sys.available_memory() / (1024 * 1024)).max(64)
}

/// Segundos transcurridos desde un timestamp unix.
/// (Se llamaba `ms_desde` y devolvia milisegundos, pero el log que la usa
/// imprime "en_cola={}s": el numero salia 1000 veces mas grande.)
fn segs_desde(t: i64) -> i64 {
    (chrono::Utc::now().timestamp() - t).max(0)
}

/// Disparo por cron: si un pipeline declara un `schedule`, cada tick evalua el
/// spec contra el momento actual. Si coincide, encola un run.
///
/// El `param` es el **momento del disparo** (UTC, al minuto), no el dia. Eso
/// importa mucho mas de lo que parece: el checkpoint de durabilidad se indexa
/// por `(dag, param, tarea)` (`engine.rs:290`), asi que dos runs con el mismo
/// param **no vuelven a ejecutar las tareas que ya salieron bien**.
///
/// Con `param = fecha`, un DAG `*/30 * * * *` hacia trabajo de verdad una sola
/// vez por dia: las otras 47 corridas terminaban en 0,3 s saltando todo y
/// reportando Success. Con el momento del disparo, cada corrida es su propia
/// particion y la durabilidad sigue haciendo lo suyo donde corresponde —
/// reintentar EL MISMO disparo (mismo param) no repite lo que ya funciono.
///
/// Anti-doble-disparo: la clave `cron:<dag>` en `meta` guarda el minuto en que
/// se disparo; si ese minuto ya estuvo, no dispara de nuevo (ni si `serve` se
/// corto y se re-levanto).
fn tick_cron(store: &Store, disponibles: &[Pipeline]) -> Result<bool> {
    let ahora = chrono::Utc::now();
    let mut disparo = false;

    // Los pipelines descubiertos, mas los compilados en el binario (compat).
    let mut schedules: Vec<(String, String)> = disponibles
        .iter()
        .filter_map(|p| p.desc.schedule.clone().map(|s| (p.desc.name.clone(), s)))
        .collect();
    for name in crate::dags::names() {
        if schedules.iter().any(|(n, _)| n == name) {
            continue; // un binario desplegado gana sobre el compilado
        }
        if let Some(v) = crate::dags::dag_cron(name) {
            schedules.push(v);
        }
    }

    // Se piden una vez y no uno por uno: con 56 DAGs serian 56 consultas por
    // tick, y el tick corre cada segundo.
    let pausados = store.dags_pausados().unwrap_or_default();

    for (dag, spec) in schedules {
        if pausados.contains(&dag.to_string()) {
            continue; // pausado: no se agenda. Lo encolado y lo que corre siguen.
        }
        if !crate::cron::matches(&spec, &ahora) {
            continue;
        }
        let minuto = ahora.timestamp() / 60 * 60;
        let clave = format!("cron:{dag}");
        if store.meta_get(&clave)?.unwrap_or_default() == minuto.to_string() {
            continue; // ya disparo en este minuto
        }
        store.meta_set(&clave, &minuto.to_string())?;
        let param = ahora.format("%Y-%m-%dT%H:%M").to_string();
        store.enqueue(&dag, &param, 0)?;
        tracing::info!("cron: disparo dag={dag} param={param} (spec={spec})");
        disparo = true;
    }
    Ok(disparo)
}

/// Cada cuanto se revisa la cola por items y runs colgados.
///
/// Un minuto: es una consulta barata y el problema que arregla tarda horas en
/// aparecer, asi que no hace falta mirarlo en cada tick.
const HIGIENE_CADA: Duration = Duration::from_secs(60);

/// Cierra lo que quedo colgado: runs en `Running` que nadie ejecuta e items de
/// la cola tomados por un proceso que ya no existe.
///
/// Esto corria **una sola vez, al arrancar** (N-08/N-41). Alcanzaba para el
/// caso que motivo aquello —el daemon se cayo y vuelve—, pero no para el que
/// se ve en produccion: un item se orfana con el scheduler andando y queda
/// tomado hasta el proximo reinicio. En una instalacion en produccion habia items en
/// `running` desde hacia **56 horas**, con su run ya terminado.
///
/// El orden importa. Primero se cierran los runs muertos y despues se rescata
/// la cola, porque `requeue_orphans` no toca un item mientras exista un run
/// `Running` de ese dag+param: al reves, la fila muerta protegeria al item y
/// los dos quedarian trabados para siempre.
fn higiene_de_cola(
    store: &Arc<Store>,
    curso: &[EnCurso],
    timeout: u64,
    ultima: &mut std::time::Instant,
) {
    if ultima.elapsed() < HIGIENE_CADA {
        return;
    }
    *ultima = std::time::Instant::now();

    // Los que este proceso tiene en la mano quedan afuera: de esos ya se ocupa
    // el tope de `recoger`, que ademas mata el subproceso.
    let vivos: Vec<uuid::Uuid> = curso.iter().map(|e| e.run_id).collect();
    match store.reap_stale_runs(timeout as i64, &vivos) {
        Ok(v) if !v.is_empty() => {
            for (id, dag) in &v {
                tracing::error!(
                    run = %id,
                    "{dag}: quedo en Running mas de {timeout}s sin nadie ejecutandolo. \
                     Se marca fallido y se libera la cola."
                );
            }
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("no pude revisar runs colgados: {e:#}"),
    }

    match store.requeue_orphans() {
        Ok(0) => {}
        Ok(n) => tracing::warn!("cola: {n} item(s) huerfanos devueltos a la cola"),
        Err(e) => tracing::warn!("no pude revisar la cola por huerfanos: {e:#}"),
    }
}

/// ¿Paso un dia desde la ultima GC? La marca vive en `meta:last_gc`, que es lo
/// que escribe `Store::gc`, asi que sobrevive a reiniciar el daemon.
fn toca_gc(store: &Store) -> bool {
    let ultima: Option<i64> = store
        .meta_get("last_gc")
        .ok()
        .flatten()
        .and_then(|v| v.parse().ok());
    match ultima {
        Some(t) => chrono::Utc::now().timestamp() - t > 86_400,
        None => true, // nunca corrio: que corra ahora y deje la marca
    }
}

/// Recoge los subprocesos que terminaron y cierra su item de cola.
///
/// Devuelve cuantos terminaron. Nada de lo que pase aca puede propagar un error
/// que mate al daemon: un run que revienta es un run, no el scheduler (N-08).
fn recoger(store: &Arc<Store>, curso: &mut Vec<EnCurso>, timeout: u64) -> usize {
    let mut terminados = 0;

    curso.retain_mut(|e| {
        // ¿Pidieron cancelarlo? La señal viaja por la DB, asi que puede venir
        // de `zeruel web`, de otra terminal o de una IA por el MCP.
        if !e.matado && store.cancel_requested(e.run_id) {
            tracing::warn!(run = %e.run_id, "cancelacion pedida: matando el subproceso");
            let _ = e.child.kill();
            e.matado = true;
        }
        // ¿Se paso del tope de tiempo?
        if !e.matado && timeout > 0 && e.desde.elapsed().as_secs() > timeout {
            tracing::error!(
                run = %e.run_id,
                "el run supero el tope de {timeout}s: lo corto"
            );
            let _ = store.request_cancel(e.run_id);
            let _ = e.child.kill();
            e.matado = true;
        }

        match e.child.try_wait() {
            Ok(None) => true, // sigue corriendo
            Ok(Some(estado)) => {
                terminados += 1;
                let segs = e.desde.elapsed().as_secs_f64();
                let cancelado = e.matado || estado.code() == Some(zeruel::EXIT_CANCELADO);

                // El estado del run lo escribio el propio pipeline. Solo se
                // corrige si el proceso murio sin poder hacerlo (kill, panic).
                if let Ok(Some(r)) = store.run_by_id(e.run_id) {
                    if !r.state.is_final() {
                        let final_ = if cancelado {
                            RunState::Cancelled
                        } else {
                            RunState::Failed
                        };
                        tracing::warn!(
                            run = %e.run_id,
                            "el subproceso murio sin cerrar su run: lo marco {final_}"
                        );
                        let _ = store.set_run_state(e.run_id, final_);
                    }
                }
                store.clear_cancel(e.run_id);

                let desenlace = if cancelado {
                    "CANCELADO"
                } else if estado.success() {
                    "OK"
                } else {
                    "FALLO"
                };
                tracing::info!(
                    "<<< {desenlace}: dag={} param={} en {segs:.1}s (log: {})",
                    e.dag,
                    e.param,
                    e.log.display()
                );

                if let Err(err) = store.complete_queue(e.queue_id) {
                    tracing::error!("no pude sacar el item {} de la cola: {err:#}", e.queue_id);
                }
                false // fuera de la lista
            }
            Err(err) => {
                tracing::error!(run = %e.run_id, "no pude consultar el subproceso: {err:#}");
                let _ = store.complete_queue(e.queue_id);
                false
            }
        }
    });

    terminados
}

/// Bandera de apagado, encendida por SIGTERM/SIGINT/SIGHUP (o Ctrl+C y el
/// cierre de consola en Windows).
///
/// Sin esto, `systemctl stop` o un `docker stop` matan el proceso de golpe y
/// los pipelines hijos quedan huerfanos: en Linux los adopta init y siguen
/// corriendo solos, escribiendo en la misma base que el scheduler que ya no
/// existe. Sus runs quedan en `Running` hasta que el arranque siguiente los
/// rescata — o sea, la base miente todo el tiempo que el servicio este parado.
fn instalar_apagado() -> Arc<AtomicBool> {
    let bandera = Arc::new(AtomicBool::new(false));
    let copia = Arc::clone(&bandera);
    if let Err(e) = ctrlc::set_handler(move || {
        // La segunda señal no espera: si alguien insiste con Ctrl+C es porque
        // no quiere seguir esperando, y un apagado que ignora eso es peor que
        // no tenerlo.
        if copia.swap(true, Ordering::SeqCst) {
            eprintln!("segunda señal: salgo ya, sin esperar a los runs");
            std::process::exit(130);
        }
        eprintln!("apagando: no lanzo nada nuevo y espero a lo que esta corriendo");
    }) {
        // Que no se pueda instalar el manejador no justifica no arrancar: el
        // scheduler sirve igual, solo pierde el apagado ordenado.
        tracing::warn!("no pude instalar el manejador de señales: {e}. Sin apagado ordenado.");
    }
    bandera
}

/// Apaga sin dejar restos: espera a los runs en curso, y lo que no llegue se
/// corta y **vuelve a la cola**.
///
/// Volver a encolarlos es la diferencia entre "el operador paro el servicio" y
/// "alguien cancelo este trabajo". Son dos cosas distintas y solo la segunda
/// justifica perder el trabajo: al reiniciar, lo cortado tiene que estar ahi.
fn apagar(store: &Arc<Store>, curso: &mut Vec<EnCurso>, timeout: u64, gracia: u64) {
    if curso.is_empty() {
        tracing::info!("apagado: nada corriendo");
        return;
    }
    tracing::info!(
        "apagado: espero hasta {gracia}s a {} run(s) en curso",
        curso.len()
    );

    let t0 = std::time::Instant::now();
    while !curso.is_empty() && t0.elapsed().as_secs() < gracia {
        recoger(store, curso, timeout);
        std::thread::sleep(std::time::Duration::from_millis(250));
    }

    if curso.is_empty() {
        tracing::info!("apagado: todos los runs terminaron solos");
        return;
    }

    // Hay que quedarse con dag y param ANTES de cortar: `recoger` saca el item
    // de la cola al procesar la salida, y despues ya no se sabe que devolver.
    let cortados: Vec<(String, String)> = curso
        .iter()
        .map(|e| (e.dag.clone(), e.param.clone()))
        .collect();

    tracing::warn!(
        "apagado: {} run(s) no terminaron en {gracia}s, los corto y los devuelvo a la cola",
        curso.len()
    );
    for e in curso.iter_mut() {
        let _ = store.request_cancel(e.run_id);
        let _ = e.child.kill();
        e.matado = true;
    }

    // Se deja que `recoger` los procese por el camino de siempre (marcar el
    // run, limpiar la cancelacion, sacar de la cola) en vez de repetir esa
    // logica aca, que es donde se irian desincronizando.
    let limite = std::time::Instant::now();
    while !curso.is_empty() && limite.elapsed().as_secs() < 10 {
        recoger(store, curso, timeout);
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    if !curso.is_empty() {
        tracing::error!(
            "apagado: {} subproceso(s) no murieron ni con kill",
            curso.len()
        );
    }

    for (dag, param) in cortados {
        match store.enqueue(&dag, &param, 0) {
            Ok(_) => tracing::info!("apagado: {dag} [{param}] vuelve a la cola"),
            Err(e) => tracing::error!("apagado: no pude reencolar {dag} [{param}]: {e:#}"),
        }
    }
}

/// Bucle principal de `zeruel serve`. Con `max > 0` sale cuando terminaron `max`
/// runs; sin max corre para siempre (daemon).
/// `zeruel serve` desde la terminal: instala el manejador de señales y arranca.
pub fn serve(args: ServeArgs) -> Result<()> {
    let apagando = instalar_apagado();
    serve_con_bandera(args, apagando)
}

/// El bucle, con la bandera de apagado inyectada.
///
/// El servicio de Windows no puede usar `serve` directamente: su orden de
/// parada no llega como señal sino como una llamada del Administrador de
/// servicios, asi que necesita encender la bandera desde afuera. Separarlo aca
/// evita tener dos copias del bucle, que es donde se irian desincronizando.
/// Clave en `meta` donde el scheduler deja su configuración efectiva.
pub const CLAVE_EFECTIVO: &str = "scheduler.efectivo";

/// Publica con qué valores está corriendo realmente este scheduler.
///
/// Incluye `config_origen` porque es el dato que distingue "el operador eligió
/// esto" de "no encontré el archivo y usé los valores de fábrica" — y desde
/// afuera esas dos situaciones se ven idénticas.
///
/// Si falla, se avisa y se sigue: no poder publicar el estado no es motivo para
/// no ejecutar pipelines.
fn publicar_efectivo(store: &Store, args: &ServeArgs, paralelo: usize) {
    let v = serde_json::json!({
        "paralelo": paralelo,
        "workers": args.workers,
        "ram_max_mb": args.ram_max_mb,
        "timeout_secs": args.timeout_secs,
        "gracia_secs": args.gracia_secs,
        "tick_ms": args.tick_ms,
        "retencion_detalle_dias": args.retencion_detalle_dias,
        "retencion_runs_dias": args.retencion_runs_dias,
        "config_origen": args.config_origen.as_ref().map(|p| p.display().to_string()),
        "version": env!("CARGO_PKG_VERSION"),
        "pid": std::process::id(),
        "desde": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    });
    if let Err(e) = store.meta_set(CLAVE_EFECTIVO, &v.to_string()) {
        tracing::warn!("no pude publicar la configuracion efectiva: {e}");
    }
}

pub fn serve_con_bandera(args: ServeArgs, apagando: Arc<AtomicBool>) -> Result<()> {
    let store = Arc::new(Store::open(&args.state)?);
    let mut registro = Registro::new();
    let mut curso: Vec<EnCurso> = Vec::new();
    // Arranca en el pasado para que la primera pasada sea inmediata: lo que
    // quedo colgado del arranque anterior se cierra ya, no en un minuto.
    let mut ultima_higiene = std::time::Instant::now() - HIGIENE_CADA;
    let mut terminados = 0usize;
    let paralelo = args.paralelo.max(1);
    /// Cada cuanto se repite el aviso de "la cola esta parada por RAM".
    const AVISO_CADA: Duration = Duration::from_secs(60);
    /// Cuanto puede postergarse un item por falta de memoria antes de lanzarlo
    /// igual.
    ///
    /// Diez minutos alcanza para que termine un run pesado y libere lugar, que
    /// es el caso legitimo que el control de admision quiere aprovechar. Pasado
    /// eso, seguir esperando no protege nada: el item no corre, no falla y no
    /// avisa. Se lanza, y si de verdad se pasa de `ram_max` se corta con un
    /// diagnostico — que es informacion, al reves que el silencio.
    const ESPERA_MAX_POR_RAM: i64 = 600;
    // Arranca en el pasado para que el primer aviso salga enseguida y no al
    // minuto: si la cola ya esta trabada, hay que enterarse ahora.
    let mut aviso_ram = std::time::Instant::now() - AVISO_CADA;

    // Decir de DONDE salio la configuracion, no solo cual es.
    //
    // La linea anterior decia `paralelo=2 techo=sin tope` y era correcta, pero
    // no alcanzaba: nada distinguia "el operador eligio 2" de "no encontre el
    // archivo y use el default". Esa diferencia costo dos dias con la cola
    // creciendo hasta 1.400 items.
    match &args.config_origen {
        Some(p) => tracing::info!("zeruel serve: configuracion de {}", p.display()),
        None => tracing::error!(
            "zeruel serve: SIN ARCHIVO DE CONFIGURACION. Todo lo de abajo son valores \
             de fabrica, no los que hayas escrito en un zeruel.toml."
        ),
    }
    tracing::info!(
        "  paralelo={paralelo} (runs a la vez)  workers={} (tareas por run)",
        args.workers
    );

    // Dejar por escrito con qué está corriendo, para que el panel lo pueda ver.
    //
    // El log de arriba sólo sirve si alguien va a buscarlo al archivo. Un
    // scheduler que quedó con los valores de fábrica —porque su servicio se
    // registró sin `--config`— se ve desde el panel exactamente igual que uno
    // bien configurado, y esa ceguera costó dos días con la cola creciendo.
    //
    // Va a la tabla `meta` y no a un archivo porque la base ya es el canal
    // entre los dos procesos, y porque el servicio web tiene permiso de
    // escritura sobre `state` pero deliberadamente NO sobre la configuración.
    publicar_efectivo(&store, &args, paralelo);
    tracing::info!(
        "  techo de RAM por run: {}",
        if args.ram_max_mb > 0 {
            format!("{} MB", args.ram_max_mb)
        } else {
            "SIN TOPE — un pipeline que se desmadre se lleva la maquina".to_string()
        }
    );
    tracing::info!(
        "  tope por corrida: {}",
        if args.timeout_secs > 0 {
            format!("{} s", args.timeout_secs)
        } else {
            "SIN TOPE — una corrida colgada ocupa un lugar para siempre".to_string()
        }
    );
    tracing::info!(
        "  pipelines={} (max={})",
        args.pipelines.display(),
        args.max
    );

    // La cuenta que nadie hacia: cuanto trabajo piden los cron contra cuanto
    // puede hacer la maquina. Con paralelo=2 y 85 h/dia agendadas la cola no
    // puede drenar NUNCA, y eso no se ve como un error sino como una cola que
    // crece — dos dias despues, y buscando la causa en otro lado.
    {
        let disponibles = registro.escanear(&args.pipelines);
        crate::capacidad::reportar(&crate::capacidad::calcular(&store, &disponibles, paralelo));
    }

    // Al arrancar, rescatar los items que quedaron tomados por un proceso que
    // ya no existe (N-08/N-41): sin esto quedaban en 'running' para siempre,
    // invisibles para `pick_next`.
    match store.requeue_orphans() {
        Ok(0) => {}
        Ok(n) => tracing::warn!("cola: {n} item(s) huerfanos devueltos a la cola"),
        Err(e) => tracing::warn!("no pude revisar la cola por huerfanos: {e:#}"),
    }

    loop {
        // 1. Recoger lo que termino (antes de decidir que lanzar).
        terminados += recoger(&store, &mut curso, args.timeout_secs);

        // Llego SIGTERM (o Ctrl+C): se corta aca, antes de lanzar nada nuevo.
        // Salir del loop y no del proceso es lo que permite el apagado
        // ordenado de abajo.
        if apagando.load(Ordering::SeqCst) {
            break;
        }
        if args.max > 0 && terminados >= args.max {
            tracing::info!("max={} alcanzado, sale zeruel serve", args.max);
            break;
        }

        // 2. Re-escanear el directorio: un binario nuevo aparece sin reiniciar.
        let disponibles = registro.escanear(&args.pipelines);

        // 3. Cron, higiene de la cola y GC.
        tick_cron(&store, &disponibles)?;
        higiene_de_cola(&store, &curso, args.timeout_secs, &mut ultima_higiene);
        if toca_gc(&store) {
            match store.gc_con_retencion(
                false,
                args.retencion_detalle_dias,
                args.retencion_runs_dias,
            ) {
                Ok(rep) => tracing::info!(
                    "GC automatica: runs={} task_runs={} meses={} db={:.1}MB",
                    rep.runs_borrados,
                    rep.task_runs_borrados,
                    rep.meses_agregados,
                    rep.db_size_bytes as f64 / 1_048_576.0
                ),
                Err(e) => tracing::error!("GC automatica fallo: {e:#}"),
            }
        }

        // 4. Llenar los cupos libres.
        let mut lance_algo = false;
        while curso.len() < paralelo {
            let running_keys = store.running_keys()?;
            let ram = ram_disponible_mb();
            let pick = store.pick_next(&running_keys, ram, 300, 20, ESPERA_MAX_POR_RAM)?;

            let Some(q) = pick.lanzado else {
                // Una cola con items y cupo libre que no lanza nada es un
                // problema, no una situacion normal. Antes esto era `debug!` y
                // produccion corre en INFO: ocho items estuvieron 1,5 horas
                // parados y el log no dijo una palabra. El scheduler SABIA por
                // que -lo contaba- y lo escribia donde nadie lo iba a ver.
                //
                // Se avisa una vez por minuto como mucho: el tick es de un
                // segundo y un aviso por vuelta seria ruido, que termina siendo
                // lo mismo que el silencio.
                if let Some((dag, necesita, hay)) = &pick.primero_sin_ram {
                    if aviso_ram.elapsed() >= AVISO_CADA {
                        aviso_ram = std::time::Instant::now();
                        tracing::warn!(
                            "cola parada por RAM: '{dag}' pide {necesita} MB (pico historico + 20%) \
                             y hay {hay} MB. {} item(s) esperando por esto. Si ese pico no es \
                             creible, mira el historial del DAG: una medicion mala lo bloquea \
                             hasta que corra de nuevo.",
                            pick.no_caben_ram
                        );
                    }
                } else if pick.bloqueados_conflicto > 0 {
                    tracing::debug!(
                        "cola: {} bloqueado(s) por conflicto de dag+param",
                        pick.bloqueados_conflicto
                    );
                }
                break;
            };

            let Some(p) = disponibles.iter().find(|p| p.desc.name == q.dag) else {
                tracing::error!(
                    "no hay binario para el pipeline '{}' en {}. Lo saco de la cola: \
                     compilalo y copialo ahi, o encola un pipeline que exista.",
                    q.dag,
                    args.pipelines.display()
                );
                store.complete_queue(q.id)?;
                continue;
            };

            // El run lo crea el SCHEDULER, no el subproceso: asi tiene el id
            // desde el momento cero y puede cancelarlo aunque el hijo todavia
            // no haya arrancado.
            let run_id = store.create_run(&q.dag, &q.param)?;
            match pipelines::lanzar(
                p,
                &q.param,
                &args.state,
                run_id,
                q.id,
                args.workers,
                args.ram_max_mb,
            ) {
                Ok(e) => {
                    tracing::info!(
                        ">>> lanzo: dag={} param={} prio={} en_cola={}s (ram_disp={}MB, {}/{} en curso)",
                        q.dag,
                        q.param,
                        q.priority,
                        segs_desde(q.enqueued_at),
                        ram,
                        curso.len() + 1,
                        paralelo
                    );
                    // Si se lanzo pese a que la estimacion no entraba, se dice.
                    // Sin esto, un run cortado por memoria mas tarde parece
                    // arbitrario: asi queda escrito que se decidio intentarlo.
                    if let Some((pedia, habia, esperando)) = pick.forzado_por_espera {
                        tracing::warn!(
                            "  lo lance igual: llevaba {}min esperando por RAM (estimaba {pedia} MB \
                             y habia {habia} MB). Si se pasa del techo, se corta y queda en \
                             Alertas — esperar mas no arreglaba nada.",
                            esperando / 60
                        );
                    }
                    curso.push(e);
                    lance_algo = true;
                }
                Err(err) => {
                    tracing::error!("no pude lanzar {}: {err:#}", p.bin.display());
                    let _ = store.set_run_state(run_id, RunState::Failed);
                    store.complete_queue(q.id)?;
                }
            }
        }

        // 5. Si no hay nada que hacer, esperar. Si hay runs en curso, el tick es
        //    mas corto para recoger rapido y reaccionar a una cancelacion.
        if !lance_algo {
            let espera = if curso.is_empty() {
                args.tick_ms
            } else {
                args.tick_ms.min(250)
            };
            std::thread::sleep(Duration::from_millis(espera));
        }
    }

    // Al salir por `--max`, no dejar hijos sueltos.
    for e in &mut curso {
        let _ = e.child.kill();
    }
    // Al salir —por señal o por `--max`— no se abandona a los hijos: se les da
    // tiempo, se corta lo que no llegue, y eso vuelve a la cola.
    apagar(&store, &mut curso, args.timeout_secs, args.gracia_secs);
    Ok(())
}
#[cfg(test)]
mod tests_apagado {
    use super::*;
    use zeruel_core::Store;

    /// Un proceso que no termina solo, para poder observar que el apagado lo
    /// corta de verdad en vez de esperar a que se muera por su cuenta.
    fn hijo_eterno() -> std::process::Child {
        let mut c = if cfg!(windows) {
            let mut c = std::process::Command::new("cmd");
            c.args(["/c", "ping", "127.0.0.1", "-n", "300"]);
            c
        } else {
            let mut c = std::process::Command::new("sleep");
            c.arg("300");
            c
        };
        c.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("no pude lanzar el proceso de prueba")
    }

    fn armar(dir: &std::path::Path) -> (Arc<Store>, Vec<EnCurso>, uuid::Uuid) {
        let store = Arc::new(Store::open(dir).unwrap());
        store.enqueue("lento", "hoy", 0).unwrap();
        let q = store
            .pick_next(&[], 100_000, 300, 20, 0)
            .unwrap()
            .lanzado
            .expect("la cola tendria que haber entregado el item");
        let run_id = store.create_run("lento", "hoy").unwrap();
        let curso = vec![EnCurso {
            child: hijo_eterno(),
            queue_id: q.id,
            run_id,
            dag: "lento".into(),
            param: "hoy".into(),
            desde: std::time::Instant::now(),
            log: dir.join("logs/x.log"),
            matado: false,
        }];
        (store, curso, run_id)
    }

    #[test]
    fn apagar_corta_lo_que_no_termina_y_lo_devuelve_a_la_cola() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, mut curso, run_id) = armar(tmp.path());

        // gracia 0: no hay nada que esperar, se corta ya.
        apagar(&store, &mut curso, 0, 0);

        assert!(curso.is_empty(), "el subproceso tendria que estar recogido");

        // El run se marca Cancelled, no Failed: no fallo nada, lo paro el
        // operador. Confundirlos hace que la tasa de fallos mienta.
        let r = store
            .run_by_id(run_id)
            .unwrap()
            .expect("el run tiene que existir");
        assert_eq!(
            r.state,
            RunState::Cancelled,
            "un run cortado por el apagado es cancelado, no fallido"
        );

        // Y sobre todo: el trabajo no se pierde. Al reiniciar tiene que estar.
        let cola = store.queue_items().unwrap();
        assert!(
            cola.iter().any(|i| i.dag == "lento" && i.param == "hoy"),
            "lo cortado por el apagado tiene que volver a la cola, y la cola quedo: {cola:?}"
        );
    }

    #[test]
    fn apagar_sin_nada_en_curso_no_toca_la_cola() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(tmp.path()).unwrap());
        store.enqueue("otro", "hoy", 0).unwrap();
        let antes = store.queue_items().unwrap().len();

        let mut vacio: Vec<EnCurso> = Vec::new();
        apagar(&store, &mut vacio, 0, 0);

        // Apagar con el scheduler ocioso no puede inventar ni duplicar items:
        // reencolar es solo para lo que se corto.
        assert_eq!(store.queue_items().unwrap().len(), antes);
    }

    #[test]
    fn la_bandera_de_apagado_arranca_apagada() {
        // instalar_apagado() no puede dejar el scheduler creyendo que ya le
        // pidieron parar: saldria del loop en la primera vuelta.
        let b = instalar_apagado();
        assert!(!b.load(Ordering::SeqCst));
    }
}

#[cfg(test)]
mod tests_pausa {
    use super::*;
    use crate::pipelines::Describe;

    /// Un pipeline con un cron que dispara todos los minutos, para no depender
    /// de la hora a la que corra el test.
    fn cada_minuto(nombre: &str) -> Pipeline {
        Pipeline {
            bin: PathBuf::from("no-importa"),
            desc: Describe {
                name: nombre.to_string(),
                schedule: Some("* * * * *".into()),
                max_retries: 0,
                tasks: vec![],
            },
            necesita_dag: false,
        }
    }

    #[test]
    fn un_dag_pausado_no_se_agenda() {
        let store = Store::open_in_memory().unwrap();
        let ps = vec![cada_minuto("activo"), cada_minuto("dormido")];

        store.pausar_dag("dormido").unwrap();
        tick_cron(&store, &ps).unwrap();

        let cola: Vec<String> = store
            .queue_items()
            .unwrap()
            .into_iter()
            .map(|i| i.dag)
            .collect();
        assert!(
            cola.contains(&"activo".to_string()),
            "el activo tiene que entrar"
        );
        assert!(
            !cola.contains(&"dormido".to_string()),
            "el pausado NO puede agendarse, y la cola quedo: {cola:?}"
        );
    }

    #[test]
    fn al_activarlo_vuelve_a_agendarse_con_su_horario() {
        // Pausar no borra el cron: vive en el --describe del binario. Esto fija
        // que activar no necesite que nadie reescriba el horario.
        let store = Store::open_in_memory().unwrap();
        let ps = vec![cada_minuto("d")];

        store.pausar_dag("d").unwrap();
        tick_cron(&store, &ps).unwrap();
        assert!(store.queue_items().unwrap().is_empty());

        store.activar_dag("d").unwrap();
        tick_cron(&store, &ps).unwrap();
        assert_eq!(store.queue_items().unwrap().len(), 1);
    }
}
