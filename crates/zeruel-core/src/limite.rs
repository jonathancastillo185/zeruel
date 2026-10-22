//! Techo de memoria **impuesto por el sistema operativo**.
//!
//! El techo del sampler ([`crate::metrics::Sampler::con_techo`]) mira el RSS
//! cada 25 ms y corta el run cuando se pasó. Es un buen diagnóstico —dice qué
//! tarea, cuánto usó y cuál era el límite— pero tiene un hueco: entre dos
//! muestras el proceso puede reservar de golpe todo lo que quiera. Una tarea que
//! hace `vec![0u8; 8 GB]` en una sola instrucción ya se llevó los 8 GB antes de
//! que nadie mire.
//!
//! Este módulo cierra ese hueco pidiéndole al kernel que **impida** la reserva.
//!
//! ## Los dos juntos, no uno u otro
//!
//! El tope duro se aplica un poco **por encima** del blando:
//!
//! ```text
//!   0 ──────────────── techo blando ──── techo duro ────►
//!                      (2048 MB)         (2304 MB)
//!                      corta con         el kernel
//!                      diagnóstico       rechaza la reserva
//! ```
//!
//! Así, en el caso normal salta primero el blando y te llevás el mensaje que
//! dice qué pasó. El duro queda de red para la reserva repentina, donde no hay
//! forma de avisar: cuando el kernel rechaza el `malloc`, Rust aborta el
//! proceso. No hay error recuperable, no hay mensaje. El scheduler ve un
//! proceso que murió y lo reporta como fallido — seguro, pero mudo. Por eso el
//! orden importa.
//!
//! ## Soporte por plataforma
//!
//! | Sistema | Mecanismo | Estado |
//! |---|---|---|
//! | Windows | Job Object con `ProcessMemoryLimit` | implementado |
//! | Linux, macOS, BSD | `setrlimit(RLIMIT_DATA)` | implementado |
//!
//! ### Por qué `RLIMIT_DATA` y no las otras dos opciones de Unix
//!
//! **`RLIMIT_AS` no sirve**, aunque sea la que primero se busca: limita el
//! *espacio de direcciones*, no la memoria usada. Polars mapea archivos grandes
//! en memoria y reserva muchísimo más espacio del que llega a tocar, así que el
//! límite saltaría con el proceso usando una fracción de lo permitido — y el
//! diagnóstico diría "se pasó de 2 GB" con 200 MB de RSS.
//!
//! **cgroups v2 sería lo más fiel**, pero escribir en `/sys/fs/cgroup` necesita
//! permisos delegados que dependen de cómo esté desplegado el proceso. Hacerlo
//! mal deja el límite sin aplicar *en silencio*, que es peor que no tenerlo.
//!
//! **`RLIMIT_DATA` es la que corresponde.** Desde Linux 4.7 cubre el heap más
//! los mapeos anónimos privados —o sea, lo que el programa reserva— y **no**
//! cuenta los mapeos respaldados por archivo. Justo la distinción que hace
//! falta: un parquet de 10 GB mapeado no consume del techo, un `Vec` de 10 GB
//! sí. No necesita privilegios y se aplica al proceso actual, que es como se
//! usa este módulo.
//!
//! Donde no está implementado, [`aplicar`] devuelve `false` y el run sigue con
//! el techo blando. Nunca falla por esto: el techo duro es una mejora, no un
//! requisito.

/// Aplica un techo de memoria al **proceso actual**, impuesto por el sistema.
///
/// Devuelve `true` si se pudo aplicar. `mb` en 0 no hace nada.
///
/// Solo tiene sentido con un proceso por run, que es como los lanza
/// `zeruel serve`: el límite es del proceso entero.
pub fn aplicar(mb: u64) -> bool {
    if mb == 0 {
        return false;
    }
    let bytes = (mb as usize).saturating_mul(1024 * 1024);
    aplicar_impl(bytes, mb)
}

/// Cuánto margen se le da al techo duro por encima del blando.
///
/// El blando tiene que saltar primero para que el fallo traiga diagnóstico. Un
/// 12% con un piso de 128 MB: suficiente para que el muestreo llegue a
/// reaccionar, poco para que el duro siga sirviendo de red.
pub fn con_margen(techo_blando_mb: u64) -> u64 {
    if techo_blando_mb == 0 {
        return 0;
    }
    let margen = (techo_blando_mb / 8).max(128);
    techo_blando_mb.saturating_add(margen)
}

#[cfg(windows)]
fn aplicar_impl(bytes: usize, mb: u64) -> bool {
    use std::sync::OnceLock;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOB_OBJECT_LIMIT_PROCESS_MEMORY,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    // El Job Object tiene que seguir vivo para que el límite valga: si se
    // cierra el último handle, el objeto se destruye y el tope desaparece.
    // Queda en un estático para que dure lo que dure el proceso.
    static JOB: OnceLock<usize> = OnceLock::new();
    if JOB.get().is_some() {
        tracing::warn!("el techo duro ya estaba aplicado: no se cambia");
        return true;
    }

    unsafe {
        let job: HANDLE = CreateJobObjectW(std::ptr::null(), std::ptr::null());
        if job.is_null() {
            tracing::warn!("no pude crear el Job Object: sigo solo con el techo por muestreo");
            return false;
        }

        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        // KILL_ON_JOB_CLOSE: cuando este proceso muere —termina, o el scheduler
        // lo mata por timeout— se cierra el último handle del job y Windows
        // termina a todos los procesos de adentro, que son sus descendientes
        // (heredan la membresía). Sin esto, matar al pipeline dejaba huérfanos
        // a sus hijos: el 29-09-2026 `financiacion` quedó "Cancelled" a las 3 h
        // y su Python siguió escribiendo en la base horas después, pisándose
        // con la corrida siguiente.
        info.BasicLimitInformation.LimitFlags =
            JOB_OBJECT_LIMIT_PROCESS_MEMORY | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        info.ProcessMemoryLimit = bytes;

        let ok = SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            &info as *const _ as *const std::ffi::c_void,
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        );
        if ok == 0 {
            tracing::warn!(
                "no pude fijar el límite del Job Object: sigo solo con el techo por muestreo"
            );
            return false;
        }

        if AssignProcessToJobObject(job, GetCurrentProcess()) == 0 {
            // Pasa si el proceso ya está en otro job que no permite anidar
            // (algunos contenedores y sandboxes de CI hacen esto).
            tracing::warn!(
                "no pude sumar el proceso al Job Object — puede que ya esté en otro. \
                 Sigo solo con el techo por muestreo."
            );
            return false;
        }

        let _ = JOB.set(job as usize);
        tracing::info!("techo duro de {mb} MB aplicado por el sistema operativo");
        true
    }
}

#[cfg(unix)]
fn aplicar_impl(bytes: usize, mb: u64) -> bool {
    // Ver el comentario del módulo: RLIMIT_DATA cubre heap y mapeos anónimos,
    // pero no los respaldados por archivo. Es la única de las tres opciones de
    // Unix que mide lo que este techo quiere medir.
    let lim = libc::rlimit {
        rlim_cur: bytes as libc::rlim_t,
        rlim_max: bytes as libc::rlim_t,
    };

    // SAFETY: `lim` es un rlimit válido y vivo durante la llamada, y
    // RLIMIT_DATA es un recurso válido. setrlimit no guarda el puntero.
    let r = unsafe { libc::setrlimit(libc::RLIMIT_DATA, &lim) };
    if r != 0 {
        // Pasa si el límite duro heredado ya es menor que el pedido: un proceso
        // sin privilegios solo puede bajarlo, nunca subirlo.
        let e = std::io::Error::last_os_error();
        tracing::warn!(
            "no pude fijar RLIMIT_DATA en {mb} MB ({e}): sigo solo con el techo por muestreo"
        );
        return false;
    }
    tracing::info!("techo duro de {mb} MB aplicado por el sistema operativo");
    true
}

#[cfg(not(any(windows, unix)))]
fn aplicar_impl(_bytes: usize, _mb: u64) -> bool {
    tracing::debug!("techo duro no implementado en esta plataforma; queda el techo por muestreo");
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cero_no_hace_nada() {
        assert!(!aplicar(0));
        assert_eq!(con_margen(0), 0);
    }

    #[test]
    fn el_margen_deja_saltar_primero_al_techo_blando() {
        // Siempre por encima, nunca por debajo: si el duro saltara antes, se
        // perderia el diagnostico que da el blando.
        for blando in [256u64, 512, 1024, 2048, 8192] {
            let duro = con_margen(blando);
            assert!(duro > blando, "{duro} tiene que ser mayor que {blando}");
        }
    }

    #[test]
    fn el_margen_tiene_un_piso_para_techos_chicos() {
        // Con 256 MB, un 12% son 32 MB: muy poco para que el muestreo de 25 ms
        // llegue a reaccionar. El piso de 128 MB lo cubre.
        assert_eq!(con_margen(256), 256 + 128);
        // Con techos grandes manda el porcentaje.
        assert_eq!(con_margen(8192), 8192 + 1024);
    }

    #[test]
    fn aplicar_un_techo_generoso_no_rompe_nada() {
        // 64 GB: nadie lo alcanza, asi que el proceso de test sigue vivo.
        // En Windows tiene que aplicar; en el resto, devolver false sin fallar.
        let r = aplicar(64 * 1024);
        if cfg!(windows) || cfg!(unix) {
            assert!(r, "en Windows y en Unix tiene que poder aplicarse");
        } else {
            assert!(!r, "en el resto de plataformas devuelve false sin fallar");
        }
    }
}
