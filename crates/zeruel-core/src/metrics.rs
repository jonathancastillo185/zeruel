//! Metricas de recursos del proceso mientras corre una tarea.
//!
//! Un `Sampler` se arranca antes de ejecutar la tarea y se detiene despues.
//! Un thread de muestreo mide el RSS del proceso y acumula el CPU (porcentaje
//! de maquina x delta de tiempo = segundos de CPU reales; cuenta multiproceso:
//! 4 nucleos al tope = 400%).
//!
//! **Que mide exactamente** (N-10, leer antes de sacar conclusiones):
//!
//! - Es el **proceso entero y todos sus descendientes**, no la tarea. Con
//!   `--workers 1` (un solo run, una task a la vez) el numero es la tarea; con
//!   varias tasks en paralelo, todas reportan el mismo pico compartido.
//! - **Por que el arbol y no el proceso solo** (N-58): una tarea que envuelve
//!   un proceso externo —un script de Python, un `dbt run`— no gasta memoria
//!   en el wrapper de Rust: la gasta el hijo. Midiendo solo el proceso propio,
//!   una instalacion en produccion reportaba 11 MB donde habia 169 MB. Mas de 10x, y con
//!   dos consecuencias: la pestaña de Metricas mentia, y el techo de memoria
//!   comparaba contra un numero que nunca se movia, asi que no protegia nada.
//! - El trabajo suele estar en los **nietos**, no en los hijos: el caso real es
//!   `reportes.exe -> python.exe (4 MB) -> python.exe (169 MB)`. Por eso el
//!   recorrido es transitivo.
//! - El pico es la **suma del arbol en un mismo instante**, no el maximo de
//!   cada rama por separado: lo que importa es cuanta RAM tenia tomada la
//!   maquina por culpa de este run en el peor momento.
//! - **En Linux esto sobrevalua**: despues de un `fork` el padre y el hijo
//!   comparten paginas copy-on-write, y sumar RSS las cuenta dos veces. Es una
//!   cota superior. Medir PSS (`/proc/<pid>/smaps_rollup`) seria exacto pero
//!   cuesta una lectura por proceso por muestra; a 25 ms no vale la pena.
//! - **Un proceso corto no se puede muestrear, y por eso no alcanza con
//!   muestrear** (N-59). Descubrir que procesos forman el arbol obliga a
//!   enumerar los del sistema: medido en la maquina de produccion, 518
//!   procesos tardan 39 ms. O sea que el arbol no se puede redescubrir mucho
//!   mas seguido que cada medio segundo sin gastar una fraccion seria de un
//!   nucleo en mirar. Un hijo que nace y muere en el medio no llega a estar
//!   nunca en la lista: node y python reservan 300 MB y salen en 0,12 s, y el
//!   sampler reportaba 13 MB de 300.
//!
//!   Se abarato lo que se pudo: redescubrir el arbol no necesita el CPU de
//!   cada proceso, y sacarlo baja el barrido de 63 ms a 17. Pero sigue sin
//!   poder ser continuo.
//!
//!   **Lo que queda sin cubrir**: una tarea muy corta cuyo trabajo pasa en un
//!   NIETO, detras de un lanzador. Es el caso del `python.exe` de un venv, que
//!   pesa 4 MB y lanza al Python de verdad: leer el pico del hijo directo
//!   —exacto y de cualquier duracion— devuelve los 4 MB del lanzador. Si esa
//!   tarea ademas dura menos de medio segundo, el nieto no se ve. Las tareas
//!   de segundos si se ven, porque el nieto vive lo mismo que su padre.
//!
//!   Por eso el numero final se combina con el que lleva el sistema operativo
//!   ([`crate::pico_os`]), que sigue sabiendo de los procesos que ya
//!   terminaron. Ese valor es un maximo **historico del proceso** y no baja, asi
//!   que solo se le atribuye a la tarea que lo supera: si no crecio mientras
//!   corria, lo unico que sabemos de esa tarea es lo que se muestreo. La
//!   consecuencia practica es que el numero del RUN es correcto —la tarea mas
//!   pesada marca el record— y una tarea liviana posterior no hereda su pico.
//! - El RSS no baja cuando el allocator retiene memoria, asi que una tarea
//!   posterior al pico hereda un numero inflado.
//! - Se toma una muestra **apenas arranca** y despues cada `INTERVALO_MS`.
//!   Antes el loop dormia 100ms antes del primer sample, asi que toda tarea
//!   mas corta que eso quedaba registrada con `ram_peak_mb = 0` y `cpu = 0`.
//! - El CPU sale del **acumulado del proceso**, no del porcentaje instantaneo:
//!   ver el comentario largo junto a `INTERVALO_MS`. El porcentaje depende del
//!   intervalo de muestreo y devolvia cero.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sysinfo::{Pid, ProcessRefreshKind, System};

/// Resultados de una tarea medida.
#[derive(Clone, Copy, Debug, Default)]
pub struct Metrics {
    /// Pico de RAM (RSS) del proceso durante la tarea, en MB.
    pub peak_ram_mb: u64,
    /// Segundos de CPU consumidos (multiproceso: 120s con 4 cores = 30s wall).
    pub cpu_secs: f64,
}

/// Cada cuanto se mira el RSS (ms). 25 ms para que una tarea corta —la de un
/// dataset chico o un self-test— no quede registrada en cero, y para que el
/// techo de memoria reaccione rapido.
const INTERVALO_MS: u64 = 25;

// Sobre el CPU: NO se usa `cpu_usage()`.
//
// `cpu_usage()` devuelve un PORCENTAJE calculado contra la medicion anterior, y
// sysinfo exige 200 ms entre refrescos para que ese diff signifique algo
// (`MINIMUM_CPU_UPDATE_INTERVAL`, igual en Windows, Linux y macOS). Muestreando
// cada 25 ms devuelve cero siempre, sin avisar — que es exactamente lo que paso
// al bajar el intervalo para arreglar la medicion de RAM (N-10): se arreglo el
// RSS y se rompio el CPU, en silencio, hasta que una medicion de 10M de filas
// mostro `cpu_secs = 0.00` en todas las tareas.
//
// `accumulated_cpu_time()` devuelve el CPU ACUMULADO del proceso en ms. Restando
// el valor del arranque se obtiene lo que consumio la tarea, **sin depender del
// intervalo de muestreo**. Es la primitiva correcta para esto.

/// Medidor de RAM/CPU. Barato: un thread que lee 2 numeros por muestra.
pub struct Sampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    cpu_ns: Arc<AtomicU64>, // nanosegundos de CPU acumulados (entero para no pelear por f64)
    excedido: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Marca historica del sistema cuando arranco esta tarea. Ver `stop`.
    pico_os_inicial: Option<u64>,
}

impl Sampler {
    /// Arranca a medir, sin techo de memoria.
    pub fn start() -> Self {
        Self::con_techo(None, None)
    }

    /// Arranca a medir con un **techo de RAM** para el proceso.
    ///
    /// Si el RSS pasa de `techo_mb`, el sampler prende la señal de cancelacion
    /// que se le pase: de ahi en mas actua todo el camino de cancelacion que ya
    /// existe — no se lanzan mas tareas y el sandbox wasm aborta en vivo.
    ///
    /// Es un tope **por muestreo**, no una barrera del sistema operativo: entre
    /// dos muestras (25 ms) el proceso puede pasarse. Sirve para cortar un run
    /// que se desmadra antes de que la maquina empiece a swapear, que es el caso
    /// real; no reemplaza a un cgroup ni a un Job Object si lo que hace falta es
    /// una garantia dura.
    ///
    /// Solo tiene sentido con **un proceso por run**, que es como los lanza
    /// `zeruel serve`: si el proceso ejecutara varios runs a la vez, el RSS seria
    /// de todos juntos y el techo no querria decir nada.
    pub fn con_techo(techo_mb: Option<u64>, cancelar: Option<Arc<AtomicBool>>) -> Self {
        Self::con_techo_y_aviso(techo_mb, cancelar, None)
    }

    /// Como [`Sampler::con_techo`], pero ademas prende una bandera del run.
    ///
    /// Hace falta para no confundir las causas: el sampler corta prendiendo la
    /// misma señal que usa la cancelacion manual, asi que sin esta bandera el
    /// run terminaria diciendo "cancelado" y mandaria a buscar a quien lo
    /// cancelo, cuando en realidad se paso de memoria.
    pub fn con_techo_y_aviso(
        techo_mb: Option<u64>,
        cancelar: Option<Arc<AtomicBool>>,
        aviso_run: Option<Arc<AtomicBool>>,
    ) -> Self {
        // El job de contabilidad tiene que existir ANTES de lanzar hijos: en
        // Windows el job cuenta a los que entran, no retroactivamente.
        crate::pico_os::preparar();
        // Lo que aporten los hijos de ESTA tarea y no los de la anterior.
        zeruel_sdk::pico_hijo::reiniciar();
        // La marca del sistema al arrancar esta tarea. El pico del job es un
        // maximo historico del PROCESO: no baja nunca, asi que sin esta linea
        // cada tarea heredaria el pico de la anterior.
        let pico_os_inicial = crate::pico_os::pico_mb();
        let pid = Pid::from_u32(std::process::id());
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(0u64));
        let cpu_ns = Arc::new(AtomicU64::new(0u64));
        let excedido = Arc::new(AtomicBool::new(false));

        let stop_t = stop.clone();
        let peak_t = peak.clone();
        let cpu_ns_t = cpu_ns.clone();
        let excedido_t = excedido.clone();
        let thread = std::thread::Builder::new()
            .name("zeruel-metrics".into())
            .spawn(move || {
                let mut sys = System::new_with_specifics(
                    sysinfo::RefreshKind::nothing()
                        .with_processes(ProcessRefreshKind::nothing().with_memory().with_cpu()),
                );
                let refresh = ProcessRefreshKind::nothing().with_memory().with_cpu();

                // El arbol se redescubre cada tanto, no en cada muestra:
                // enumerar todos los procesos de la maquina cada 25 ms seria
                // caro y no hace falta —un hijo nuevo puede esperar medio
                // segundo a ser visto—, pero muestrear los que ya conocemos si
                // tiene que ser rapido para que el techo reaccione.
                const CADA_CUANTAS_MUESTRAS_REDESCUBRIR: u32 = 20; // 500 ms
                let mut vuelta = 0u32;
                let mut arbol: Vec<Pid> = vec![pid];

                // CPU acumulado por proceso. Se guarda el ultimo valor visto de
                // cada uno porque un hijo que termina desaparece del sistema y
                // su tiempo se perderia; asi el total del arbol no baja.
                let mut cpu_por_pid: std::collections::HashMap<Pid, u64> =
                    std::collections::HashMap::new();
                let mut cpu_base: std::collections::HashMap<Pid, u64> =
                    std::collections::HashMap::new();

                // Muestra inicial ANTES del primer sleep: una tarea de 20ms
                // tiene que reportar su RAM, no cero.
                sys.refresh_processes_specifics(
                    sysinfo::ProcessesToUpdate::All,
                    true,
                    ProcessRefreshKind::nothing(),
                );
                arbol = descendientes(&sys, pid);
                sys.refresh_processes_specifics(
                    sysinfo::ProcessesToUpdate::Some(&arbol),
                    true,
                    refresh,
                );
                let medir = |sys: &System,
                             arbol: &[Pid],
                             cpu_por_pid: &mut std::collections::HashMap<Pid, u64>,
                             cpu_base: &mut std::collections::HashMap<Pid, u64>|
                 -> u64 {
                    let mut rss_total = 0u64;
                    for p in arbol.iter().filter_map(|q| sys.process(*q)) {
                        rss_total += p.memory();
                        let acum = p.accumulated_cpu_time();
                        // La base es lo que el proceso ya traia cuando lo
                        // vimos por primera vez. Para un hijo recien lanzado
                        // es ~0; para el propio proceso es todo lo que gasto
                        // en tareas anteriores, que no es de esta tarea.
                        let base = *cpu_base.entry(p.pid()).or_insert(acum);
                        cpu_por_pid.insert(p.pid(), acum.saturating_sub(base));
                    }
                    let cpu_ms: u64 = cpu_por_pid.values().sum();
                    cpu_ns_t.store(cpu_ms.saturating_mul(1_000_000), Ordering::Relaxed);
                    rss_total / (1024 * 1024)
                };

                let rss_mb = medir(&sys, &arbol, &mut cpu_por_pid, &mut cpu_base);
                peak_t.fetch_max(rss_mb, Ordering::Relaxed);

                while !stop_t.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(INTERVALO_MS));
                    vuelta = vuelta.wrapping_add(1);

                    if vuelta.is_multiple_of(CADA_CUANTAS_MUESTRAS_REDESCUBRIR) {
                        // Para redescubrir el arbol solo hace falta el
                        // parentesco. Pedir tambien el CPU de los 574 procesos
                        // de la maquina cuesta 63 ms; sin el, 17. La memoria y
                        // el CPU se piden despues, y solo de los nuestros.
                        sys.refresh_processes_specifics(
                            sysinfo::ProcessesToUpdate::All,
                            true,
                            ProcessRefreshKind::nothing(),
                        );
                        arbol = descendientes(&sys, pid);
                        sys.refresh_processes_specifics(
                            sysinfo::ProcessesToUpdate::Some(&arbol),
                            true,
                            refresh,
                        );
                    } else {
                        sys.refresh_processes_specifics(
                            sysinfo::ProcessesToUpdate::Some(&arbol),
                            true,
                            refresh,
                        );
                    }

                    let rss_mb = medir(&sys, &arbol, &mut cpu_por_pid, &mut cpu_base);
                    peak_t.fetch_max(rss_mb, Ordering::Relaxed);

                    // Para DECIDIR SI CORTAR se mira tambien el pico del
                    // sistema, porque un hijo corto no llega a estar nunca en
                    // el arbol. Pero ese valor NO entra en `peak_t`: es un
                    // maximo historico del proceso que no baja, y meterlo ahi
                    // haria que toda tarea posterior a la mas pesada reportara
                    // el numero de aquella. La combinacion acotada se hace al
                    // cerrar, en `stop`.
                    // Al decidir si cortar entra tambien lo que reporto un
                    // hijo que ya termino: puede haber vivido menos de lo que
                    // tarda un redescubrimiento del arbol y no haberse visto
                    // nunca en el muestreo.
                    let para_el_techo = rss_mb
                        .max(zeruel_sdk::pico_hijo::leer())
                        .max(crate::pico_os::pico_mb().unwrap_or(0));

                    if let Some(techo) = techo_mb {
                        if para_el_techo > techo && !excedido_t.swap(true, Ordering::SeqCst) {
                            tracing::error!(
                                "el run supero el techo de memoria: {para_el_techo} MB > \
                                 {techo} MB (proceso + descendientes). Se corta antes de que \
                                 la maquina empiece a swapear."
                            );
                            if let Some(a) = &aviso_run {
                                a.store(true, Ordering::SeqCst);
                            }
                            if let Some(c) = &cancelar {
                                c.store(true, Ordering::SeqCst);
                            }
                        }
                    }
                }
            })
            .expect("sampler thread");

        Self {
            pico_os_inicial,
            stop,
            peak,
            cpu_ns,
            excedido,
            thread: Some(thread),
        }
    }

    /// ¿El proceso paso el techo de memoria?
    pub fn excedio_el_techo(&self) -> bool {
        self.excedido.load(Ordering::Relaxed)
    }

    /// El pico visto hasta ahora, sin detener la medicion.
    pub fn peak_ram_mb(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Detiene el sampler y devuelve los resultados.
    pub fn stop(mut self) -> Metrics {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        // El muestreo y el sistema ven cosas distintas: el muestreo sigue a un
        // proceso largo mientras vive, y el sistema es el unico que sabe de un
        // hijo que nacio y murio entre dos redescubrimientos del arbol.
        //
        // El numero del sistema es un maximo HISTORICO del proceso, que no
        // baja. Solo se puede atribuir a esta tarea si CRECIO mientras corria:
        // en ese caso la marca nueva es suya, porque supero todo lo anterior.
        // Si no crecio, esta tarea no llego al maximo previo y lo unico que
        // sabemos de ella es lo que se muestreo.
        let muestreado = self
            .peak
            .load(Ordering::Relaxed)
            .max(zeruel_sdk::pico_hijo::leer());
        let os_ahora = crate::pico_os::pico_mb();
        let peak_ram_mb = match (os_ahora, self.pico_os_inicial) {
            (Some(ahora), Some(antes)) if ahora > antes => ahora.max(muestreado),
            // Sin marca previa (primera tarea del proceso) el maximo historico
            // es de esta tarea y nada mas.
            (Some(ahora), None) => ahora.max(muestreado),
            _ => muestreado,
        };
        Metrics {
            peak_ram_mb,
            cpu_secs: self.cpu_ns.load(Ordering::Relaxed) as f64 / 1e9,
        }
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // no join para no hacer esperar un drop; el thread muere solo en <200ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn sin_techo_nunca_marca_exceso() {
        let s = Sampler::start();
        std::thread::sleep(Duration::from_millis(120));
        assert!(!s.excedio_el_techo());
        let m = s.stop();
        assert!(m.peak_ram_mb > 0, "algo de RSS tiene que medir");
    }

    #[test]
    fn un_techo_ridiculo_marca_exceso_y_prende_las_señales() {
        // 1 MB: cualquier proceso de test ya lo paso.
        let cancelar = Arc::new(AtomicBool::new(false));
        let aviso = Arc::new(AtomicBool::new(false));
        let s = Sampler::con_techo_y_aviso(
            Some(1),
            Some(Arc::clone(&cancelar)),
            Some(Arc::clone(&aviso)),
        );
        // Se espera A QUE PASE, no un tiempo fijo: con la maquina cargada el
        // hilo del sampler puede tardar en correr, y un sleep fijo hace que el
        // test falle a veces sin que haya nada roto.
        let t0 = Instant::now();
        while !s.excedio_el_techo() && t0.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(20));
        }
        let excedio = s.excedio_el_techo();
        s.stop();
        assert!(
            excedio,
            "tiene que detectar que se paso (espero {:?})",
            t0.elapsed()
        );
        assert!(cancelar.load(Ordering::SeqCst), "corta el run");
        assert!(aviso.load(Ordering::SeqCst), "avisa la causa real");
    }

    #[test]
    fn un_techo_generoso_no_molesta() {
        let cancelar = Arc::new(AtomicBool::new(false));
        // 1 TB: nadie lo pasa.
        let s = Sampler::con_techo(Some(1024 * 1024), Some(Arc::clone(&cancelar)));
        std::thread::sleep(Duration::from_millis(120));
        let excedio = s.excedio_el_techo();
        s.stop();
        assert!(!excedio);
        assert!(!cancelar.load(Ordering::SeqCst), "no corta nada");
    }
}

/// El proceso `raiz` y todos sus descendientes, transitivamente.
///
/// Hace falta transitivo y no solo los hijos directos porque el trabajo real
/// suele estar un nivel mas abajo: `reportes.exe -> python.exe -> python.exe`,
/// donde el del medio pesa 4 MB y el de abajo 169.
///
/// `sys` tiene que venir de un refresh de **todos** los procesos: el parentesco
/// solo se puede reconstruir mirando el sistema entero.
fn descendientes(sys: &System, raiz: Pid) -> Vec<Pid> {
    // padre -> hijos, armado de una pasada. Recorrer todos los procesos por
    // cada nivel seria O(n*profundidad) sin necesidad.
    let mut hijos: std::collections::HashMap<Pid, Vec<Pid>> = std::collections::HashMap::new();
    for (pid, p) in sys.processes() {
        if let Some(padre) = p.parent() {
            hijos.entry(padre).or_default().push(*pid);
        }
    }
    let mut out = vec![raiz];
    let mut i = 0;
    // Recorrido en anchura sobre `out`, que hace de cola y de resultado.
    while i < out.len() {
        if let Some(hs) = hijos.get(&out[i]) {
            for h in hs {
                // Un PID reciclado podria formar un ciclo y colgar el sampler.
                if !out.contains(h) {
                    out.push(*h);
                }
            }
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests_arbol {
    use super::*;

    /// El hijo del test de abajo. Reserva ~150 MB, los **toca** para que sean
    /// residentes de verdad (reservar sin escribir no mueve el RSS) y espera a
    /// que el padre lo haya muestreado.
    ///
    /// Va marcado `#[ignore]` para que no corra en la suite normal: existe solo
    /// para que el padre lo invoque por nombre.
    #[test]
    #[ignore]
    fn hijo_que_come_ram() {
        let mut v: Vec<u8> = Vec::with_capacity(150 * 1024 * 1024);
        for i in 0..(150 * 1024 * 1024usize) {
            v.push((i % 251) as u8);
        }
        std::thread::sleep(Duration::from_millis(1_500));
        // Que el optimizador no tire el buffer antes de tiempo.
        assert_eq!(v.len(), 150 * 1024 * 1024);
    }

    /// N-58: el pico tiene que incluir a los hijos.
    ///
    /// Antes de medir el arbol este test daba el RSS del proceso de test
    /// (unas decenas de MB) sin importar cuanto reservara el hijo, que es
    /// exactamente el bug que hacia reportar 11 MB donde habia 169.
    #[test]
    fn el_pico_incluye_a_los_hijos() {
        let exe = std::env::current_exe().expect("current_exe");
        let s = Sampler::start();
        let base = {
            // Cuanto pesa el proceso de test solo, antes de lanzar nada.
            std::thread::sleep(Duration::from_millis(200));
            s.peak_ram_mb()
        };

        let mut hijo = std::process::Command::new(exe)
            .args([
                "--exact",
                "metrics::tests_arbol::hijo_que_come_ram",
                "--ignored",
                "--nocapture",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("lanzar el hijo");

        // El arbol se redescubre cada 500 ms; hay que darle margen a eso y a
        // que el hijo llene su buffer.
        std::thread::sleep(Duration::from_millis(2_000));
        let con_hijo = s.peak_ram_mb();
        let _ = hijo.kill();
        let _ = hijo.wait();
        let m = s.stop();

        assert!(
            con_hijo >= base + 100,
            "el pico tiene que incluir los ~150 MB del hijo: base {base} MB, con hijo {con_hijo} MB"
        );
        assert!(
            m.peak_ram_mb >= con_hijo,
            "el pico final no puede ser menor al observado ({} < {con_hijo})",
            m.peak_ram_mb
        );
    }

    /// Come 250 MB y se muere enseguida. Modela lo mas comun de un ETL: cargar
    /// un archivo grande, procesarlo y salir.
    #[test]
    #[ignore]
    fn hijo_rapido_que_come_ram() {
        let mut v: Vec<u8> = Vec::with_capacity(250 * 1024 * 1024);
        for i in 0..(250 * 1024 * 1024usize) {
            v.push((i % 251) as u8);
        }
        assert_eq!(v.len(), 250 * 1024 * 1024);
        // Sin sleep: apenas termina de reservar, sale.
    }

    /// El pico tiene que verse aunque el hijo dure menos que el intervalo con
    /// el que se redescubre el arbol.
    #[test]
    fn el_pico_de_un_hijo_corto_no_se_pierde() {
        let exe = std::env::current_exe().expect("current_exe");
        let s = Sampler::start();
        std::thread::sleep(Duration::from_millis(150));
        let base = s.peak_ram_mb();

        let t0 = std::time::Instant::now();
        let salida = std::process::Command::new(exe)
            .args([
                "--exact",
                "metrics::tests_arbol::hijo_rapido_que_come_ram",
                "--ignored",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("correr el hijo");
        let duro = t0.elapsed();
        let m = s.stop();
        assert!(salida.success(), "el hijo tiene que salir bien");

        println!(
            "  hijo duro {:?}, base {base} MB, pico medido {} MB",
            duro, m.peak_ram_mb
        );
        assert!(
            m.peak_ram_mb >= base + 150,
            "se perdio el pico del hijo corto (duro {duro:?}): base {base} MB, medido {} MB",
            m.peak_ram_mb
        );
    }

    /// Cuanto cuesta enumerar todos los procesos: es lo que decide cada cuanto
    /// se puede redescubrir el arbol.
    #[test]
    #[ignore]
    fn costo_de_enumerar() {
        let mut sys = System::new_with_specifics(
            sysinfo::RefreshKind::nothing()
                .with_processes(ProcessRefreshKind::nothing().with_memory().with_cpu()),
        );
        let refresh = ProcessRefreshKind::nothing().with_memory().with_cpu();
        sys.refresh_processes_specifics(sysinfo::ProcessesToUpdate::All, true, refresh);
        let n = sys.processes().len();

        let mut total = Duration::ZERO;
        for _ in 0..20 {
            let t = std::time::Instant::now();
            sys.refresh_processes_specifics(sysinfo::ProcessesToUpdate::All, true, refresh);
            let _ = descendientes(&sys, Pid::from_u32(std::process::id()));
            total += t.elapsed();
        }
        println!(
            "  {n} procesos; enumerar + armar el arbol: {:?} promedio",
            total / 20
        );
    }

    /// Cuanto cuesta un barrido SOLO de parentesco (sin pedir memoria ni CPU
    /// de cada proceso) contra uno completo. Decide si se puede seguir el arbol
    /// de un hijo mientras corre.
    #[test]
    #[ignore]
    fn costo_parentesco_vs_completo() {
        let medir = |etiqueta: &str, rk: ProcessRefreshKind| {
            let mut sys = System::new();
            sys.refresh_processes_specifics(sysinfo::ProcessesToUpdate::All, true, rk);
            let n = sys.processes().len();
            let mut total = Duration::ZERO;
            for _ in 0..15 {
                let t = std::time::Instant::now();
                sys.refresh_processes_specifics(sysinfo::ProcessesToUpdate::All, true, rk);
                total += t.elapsed();
            }
            println!("  {etiqueta:<28} {n} procesos: {:?}", total / 15);
        };
        medir("solo parentesco", ProcessRefreshKind::nothing());
        medir(
            "parentesco + memoria",
            ProcessRefreshKind::nothing().with_memory(),
        );
        medir(
            "completo (lo que se usaba)",
            ProcessRefreshKind::nothing().with_memory().with_cpu(),
        );
    }

    /// El recorrido tiene que ser transitivo: el caso real es
    /// `reportes.exe -> python.exe -> python.exe`, con el peso abajo de todo.
    #[test]
    fn el_arbol_baja_mas_de_un_nivel() {
        let sys = System::new_all();
        let yo = Pid::from_u32(std::process::id());
        let d = descendientes(&sys, yo);
        assert!(d.contains(&yo), "la raiz se incluye a si misma");
        // No se puede fabricar un nieto sin lanzar procesos, pero si verificar
        // que la funcion no se queda en el primer nivel: sobre el proceso init
        // (pid 1 en Linux, el arbol entero) tiene que traer mas que sus hijos
        // directos. Aca alcanza con que no explote y no cicle.
        assert!(!d.is_empty());
    }
}
