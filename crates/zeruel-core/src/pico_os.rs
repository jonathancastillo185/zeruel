//! Pico de memoria del proceso **y sus descendientes**, según el sistema
//! operativo.
//!
//! # Por qué existe este módulo
//!
//! El muestreo de [`crate::metrics`] mira el RSS de un árbol de procesos cada
//! 25 ms. Para saber qué procesos forman el árbol hay que enumerar los del
//! sistema, y eso **no es barato**: medido en la máquina de producción, 518
//! procesos tardan **39 ms** en enumerarse. O sea que el árbol no se puede
//! redescubrir mucho más seguido que cada medio segundo sin gastar una fracción
//! seria de un núcleo en mirar.
//!
//! Eso deja un agujero que el muestreo **no puede cerrar por más fino que se
//! lo ponga**: un proceso hijo que nace y muere entre dos redescubrimientos no
//! llega a estar nunca en la lista, así que su memoria no se cuenta. Y no es un
//! caso raro: medido acá, `node` y `python` reservan 300 MB y terminan en
//! **0,12 s**. Cargar un archivo, procesarlo y salir es la forma normal de una
//! tarea de ETL. En esa medición el sampler reportaba **13 MB de 300**.
//!
//! # Qué hace en cambio
//!
//! Le pregunta al sistema operativo, que lleva la cuenta él mismo y **sigue
//! sabiendo de los procesos que ya terminaron**. Es O(1) y no depende del
//! lenguaje del pipeline: lo único que zeruel sabe es que lanza procesos.
//!
//! Eso alcanza en Unix. En Windows no hay una medida equivalente que sea de
//! RSS, así que ahí el pico lo aporta quien lanza el proceso —que tiene su
//! handle— por [`zeruel_sdk::pico_hijo`]. Ver el módulo `imp` de más abajo.
//!
//! | Sistema | Mecanismo | Qué mide |
//! |---|---|---|
//! | Windows | nada acá; ver el módulo `imp` | (lo cubre `zeruel_sdk::pico_hijo`) |
//! | Unix | `getrusage(RUSAGE_CHILDREN).ru_maxrss` | **RSS** máximo de un hijo esperado |
//!
//! # Lo que mide y lo que no
//!
//! - En **Windows** este módulo no aporta nada: la única medida barata que hay
//!   —el `PeakJobMemoryUsed` de un Job Object— cuenta memoria comprometida y no
//!   residente, y midiendo con el Python real da **401 MB donde el RSS es 77**.
//!   Sobrevalorar cinco veces no es el lado seguro cuando ese número alimenta
//!   un techo: corta corridas que nunca se pasaron. Ahí el pico lo aporta quien
//!   lanza el proceso, que tiene su handle (`zeruel_sdk::pico_hijo`).
//! - En **Unix** `ru_maxrss` es el pico de UN hijo, no la suma de todos: si dos
//!   hijos usan 1 GB cada uno a la vez, devuelve 1 GB, no 2. También es la
//!   dirección segura para no inventar consumo que no hubo.
//! - Sólo cuenta lo que **ya terminó**. Por eso no reemplaza al muestreo, que
//!   es el que sigue a un proceso largo mientras vive: se combinan con un
//!   máximo, y cada uno cubre lo que al otro se le escapa.

/// Pico de memoria del árbol, en MB, si el sistema puede decirlo.
///
/// `None` cuando la plataforma no lo soporta o la llamada falla: nunca es un
/// error: el muestreo sigue dando su número.
pub fn pico_mb() -> Option<u64> {
    imp::pico_mb()
}

/// Prepara lo que el sistema necesite para poder contestar después.
///
/// Hoy no hace falta nada en ninguna plataforma. Se deja porque la preparación
/// es parte del contrato —una implementación futura puede necesitarla— y quitar
/// la llamada obligaría a acordarse de volver a ponerla. Es idempotente y no
/// falla nunca.
pub fn preparar() {
    imp::preparar();
}

#[cfg(windows)]
mod imp {
    //! En Windows **no hay** una medida barata y exacta del pico de RSS de un
    //! arbol de procesos, y conviene decirlo antes de que alguien intente
    //! resucitar la que estaba.
    //!
    //! Se probo con un Job Object y su `PeakJobMemoryUsed`. Cuenta memoria
    //! **comprometida**, no residente, y la diferencia no es un detalle:
    //! medido con el Python del proyecto real, `import pandas` da 77 MB de RSS
    //! y **401 MB de commit**. Cinco veces.
    //!
    //! Sobrevalorar no es el lado seguro cuando ese numero alimenta un techo
    //! de memoria: con el job puesto, el techo de 2500 MB corto corridas que
    //! nunca habian pasado de unos cientos de MB reales, y quedaron en la base
    //! como fallidas con "3170 MB".
    //!
    //! Lo que si funciona es leer el pico del hijo desde su handle
    //! (`GetProcessMemoryInfo` → `PeakWorkingSetSize`, que es RSS y sigue
    //! disponible despues de que el proceso murio). Eso se hace donde se tiene
    //! el handle, en `zeruel-tasks`, y llega hasta aca por
    //! `zeruel_sdk::pico_hijo`.

    pub fn preparar() {}

    pub fn pico_mb() -> Option<u64> {
        None
    }
}

#[cfg(unix)]
mod imp {
    pub fn preparar() {}

    pub fn pico_mb() -> Option<u64> {
        unsafe {
            let mut ru: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_CHILDREN, &mut ru) != 0 {
                return None;
            }
            // `ru_maxrss` viene en KB en Linux y en BYTES en macOS/BSD. Elegir
            // mal es un factor 1024 silencioso en la unidad que se le muestra
            // al usuario.
            let bytes = if cfg!(target_os = "macos") {
                ru.ru_maxrss as u64
            } else {
                (ru.ru_maxrss as u64).saturating_mul(1024)
            };
            Some(bytes / (1024 * 1024))
        }
    }
}

#[cfg(not(any(windows, unix)))]
mod imp {
    pub fn preparar() {}
    pub fn pico_mb() -> Option<u64> {
        None
    }
}
