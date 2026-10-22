//! `zeruel init`: crea el `zeruel.toml` de una instalación nueva.
//!
//! # Por qué existe
//!
//! Los límites de operación —cuánto se guarda, cuánto puede durar una corrida,
//! cuánta RAM puede tomar— venían **apagados por defecto**. Un `timeout_secs`
//! sin poner significa "sin tope", y nadie descubre que le falta hasta que un
//! DAG queda corriendo dos días. Preguntarlos al instalar convierte tres
//! decisiones que hoy se toman por omisión en tres decisiones tomadas.
//!
//! # Los valores por defecto, y por qué esos
//!
//! Se ofrecen contestando `Enter`, así que son lo que va a tener la mayoría:
//! tienen que ser defendibles, no cómodos.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Lo que se le pregunta al usuario, con su valor sugerido.
struct Respuestas {
    state: String,
    pipelines: String,
    paralelo: usize,
    workers: usize,
    ram_max: String,
    timeout_horas: f64,
    retencion_detalle: i64,
    retencion_runs: i64,
    puerto: u16,
}

impl Respuestas {
    /// Sugerencias calculadas contra la máquina donde se está instalando.
    fn sugeridas(dir: &Path) -> Self {
        let nucleos = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        let ram_total_mb = {
            let mut sys = sysinfo::System::new_all();
            sys.refresh_memory();
            sys.total_memory() / (1024 * 1024)
        };

        // Runs en paralelo: la mitad de los núcleos, mínimo 1. Cada run es un
        // proceso, así que copar todos los núcleos deja la máquina sin aire
        // para el scheduler y para el sistema.
        let paralelo = (nucleos / 2).max(1);
        // Techo por run: la RAM total menos un cuarto para el sistema, dividida
        // entre los runs que pueden correr juntos. Es lo que hace que N runs
        // simultáneos en el peor caso no lleven la máquina al swap.
        let ram_por_run = (ram_total_mb * 3 / 4) / paralelo as u64;

        Self {
            state: dir.join("state").display().to_string().replace('\\', "/"),
            pipelines: dir
                .join("pipelines")
                .display()
                .to_string()
                .replace('\\', "/"),
            paralelo,
            // Tareas en paralelo dentro de un run: pocas, porque ya hay varios
            // runs a la vez y el paralelismo real sale de ahí.
            workers: nucleos.clamp(1, 4),
            ram_max: format!("{}M", ram_por_run.max(256)),
            timeout_horas: 3.0,
            retencion_detalle: zeruel_core::store::RETENCION_DETALLE_DIAS,
            retencion_runs: zeruel_core::store::RETENCION_RUNS_DIAS,
            puerto: 8080,
        }
    }
}

/// Crea el archivo. Con `si_a_todo` no pregunta nada y usa las sugerencias.
pub fn ejecutar(ruta: Option<PathBuf>, si_a_todo: bool, forzar: bool) -> Result<()> {
    ejecutar_con_ruido(ruta, si_a_todo, forzar, true)
}

/// Igual, pero pudiendo callar el resumen.
///
/// El instalador ya lleva su propio hilo de "[ok] este paso": intercalar el
/// resumen completo de `init` en el medio hace que no se lea ninguno de los
/// dos.
pub fn ejecutar_con_ruido(
    ruta: Option<PathBuf>,
    si_a_todo: bool,
    forzar: bool,
    contar: bool,
) -> Result<()> {
    let destino = ruta.unwrap_or_else(|| PathBuf::from("zeruel.toml"));
    // Pisar la configuración de una instalación que ya anda es de las peores
    // cosas que puede hacer una herramienta: los valores de producción no están
    // en ningún otro lado.
    if destino.exists() && !forzar {
        anyhow::bail!(
            "ya existe {}. No lo piso: sus valores no estan en ningun otro lado.\n\
             Si de verdad querés reemplazarlo, `zeruel init --forzar` (hacé una copia antes).",
            destino.display()
        );
    }
    let dir = destino
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    let sug = Respuestas::sugeridas(&dir);
    let r = if si_a_todo { sug } else { preguntar(sug)? };

    let toml = plantilla(&r);
    std::fs::write(&destino, &toml)
        .with_context(|| format!("no pude escribir {}", destino.display()))?;

    if !contar {
        return Ok(());
    }
    println!("\nListo: {}\n", destino.display());
    println!("  estado        {}", r.state);
    println!("  pipelines     {}", r.pipelines);
    println!("  runs a la vez {}", r.paralelo);
    println!("  RAM por run   {}", r.ram_max);
    println!(
        "  corte por tiempo {}",
        if r.timeout_horas > 0.0 {
            format!("{} h", r.timeout_horas)
        } else {
            "SIN TOPE".into()
        }
    );
    println!(
        "  se guarda     {} dias de detalle, {} dias de corridas",
        r.retencion_detalle, r.retencion_runs
    );
    println!(
        "\nPara arrancar:\n  zeruel serve --config {}",
        destino.display()
    );
    Ok(())
}

/// Una pregunta con valor sugerido. `Enter` acepta la sugerencia.
fn pedir(texto: &str, sugerido: &str, ayuda: &str) -> Result<String> {
    println!("\n{ayuda}");
    print!("{texto} [{sugerido}]: ");
    std::io::stdout().flush().ok();
    let mut linea = String::new();
    // Sin terminal (un instalador desatendido) `read_line` devuelve 0 bytes:
    // eso es aceptar la sugerencia, no quedarse colgado ni fallar.
    if std::io::stdin().read_line(&mut linea).unwrap_or(0) == 0 {
        println!("{sugerido}");
        return Ok(sugerido.to_string());
    }
    let v = linea.trim();
    Ok(if v.is_empty() {
        sugerido.to_string()
    } else {
        v.to_string()
    })
}

fn pedir_num<T: std::str::FromStr + std::fmt::Display + Copy>(
    texto: &str,
    sugerido: T,
    ayuda: &str,
) -> Result<T> {
    loop {
        let v = pedir(texto, &sugerido.to_string(), ayuda)?;
        match v.parse::<T>() {
            Ok(n) => return Ok(n),
            // Repreguntar en vez de abortar: quien se equivoca tipeando no
            // tiene por qué volver a empezar toda la configuración.
            Err(_) => println!("  '{v}' no es un numero. Probá de nuevo."),
        }
    }
}

fn preguntar(s: Respuestas) -> Result<Respuestas> {
    println!("Configuracion de zeruel. Enter acepta el valor entre corchetes.");

    let state = pedir(
        "Directorio de estado",
        &s.state,
        "Donde vive la base y los logs. Tiene que sobrevivir a los despliegues.",
    )?;
    let pipelines = pedir(
        "Directorio de pipelines",
        &s.pipelines,
        "Donde se copian los binarios de pipeline. Se re-escanea solo: sumar uno\n\
         es copiar un archivo, sin reiniciar nada.",
    )?;
    let paralelo = pedir_num(
        "Runs en paralelo",
        s.paralelo,
        "Cuantas corridas pueden ejecutarse a la vez. Cada una es un proceso\n\
         propio, asi que este numero multiplica el consumo de RAM.",
    )?;
    let workers = pedir_num(
        "Tareas en paralelo dentro de un run",
        s.workers,
        "El paralelismo de adentro de una corrida. Es distinto del anterior.",
    )?;
    let ram_max = pedir(
        "Techo de RAM por run",
        &s.ram_max,
        "Cuanta memoria puede tomar UNA corrida (con sus procesos hijos) antes\n\
         de que se la corte. `0` la deja sin techo, que es como quedarse sin red\n\
         cuando un pipeline se desmadra.",
    )?;
    let timeout_horas = pedir_num(
        "Horas maximas por corrida",
        s.timeout_horas,
        "Pasado ese tiempo la corrida se corta y queda como fallida. Sin esto un\n\
         DAG que se cuelga sigue ahi para siempre, ocupando un lugar de los\n\
         `runs en paralelo`. `0` es sin tope.",
    )?;
    let retencion_detalle = pedir_num(
        "Dias de detalle por tarea",
        s.retencion_detalle,
        "El detalle tarea por tarea es lo que mas pesa en la base. Pasada la\n\
         primera semana casi no se mira; el resumen de la corrida queda igual.",
    )?;
    let retencion_runs = pedir_num(
        "Dias de historial de corridas",
        s.retencion_runs,
        "Cuanto se guarda el listado de corridas. Al borrarse, su volumen sigue\n\
         estando en los agregados mensuales, que no se purgan nunca.",
    )?;
    let puerto = pedir_num(
        "Puerto de la interfaz web",
        s.puerto,
        "Escucha solo en 127.0.0.1: para abrirla a la red hay que decirlo aparte,\n\
         porque no tiene usuarios ni contraseñas.",
    )?;

    Ok(Respuestas {
        state,
        pipelines,
        paralelo,
        workers,
        ram_max,
        timeout_horas,
        retencion_detalle,
        retencion_runs,
        puerto,
    })
}

/// El archivo, con los porqués al lado de cada valor: el que lo abra dentro de
/// seis meses tiene que poder cambiarlo sin buscar documentación.
fn plantilla(r: &Respuestas) -> String {
    let timeout_secs = (r.timeout_horas * 3600.0).round() as i64;
    format!(
        r#"# Configuracion de zeruel, generada por `zeruel init`.
#
# Los flags de la linea de comandos pisan lo que diga este archivo.

[zeruel]
# Donde vive la base y los logs. Tiene que sobrevivir a los despliegues.
state     = "{state}"
# Los binarios de pipeline. Se re-escanea solo: sumar uno es copiar un archivo.
pipelines = "{pipelines}"

# Cuantas corridas a la vez. Cada una es un proceso propio, asi que este numero
# multiplica el consumo de RAM de la maquina.
paralelo  = {paralelo}
# Tareas en paralelo DENTRO de una corrida. Es otra cosa que `paralelo`.
workers   = {workers}

# Techo de memoria de UNA corrida, contando sus procesos hijos. Al pasarse, se
# corta. Sin esto, un pipeline que se desmadra se lleva puesta la maquina.
ram_max   = "{ram_max}"

# Corte por tiempo, en segundos ({horas} h). Pasado eso la corrida se corta y
# queda fallida.
#
# Cubre dos casos distintos: al que el scheduler tiene en la mano lo mata, y al
# que quedo huerfano —porque el scheduler se reinicio— lo marca fallido y libera
# su lugar en la cola. Sin tope, un DAG colgado ocupa un lugar de `paralelo`
# para siempre.
timeout_secs = {timeout_secs}

# Al apagar, cuanto se espera a las corridas en curso antes de cortarlas.
# systemd manda SIGKILL a los 90 s, asi que pasarse de ahi no sirve.
gracia_secs  = 60

# Cuanto se guarda. La limpieza corre sola una vez por dia.
#  - detalle: las filas tarea por tarea. Es lo que mas pesa.
#  - runs: el listado de corridas. Al borrarse, su volumen queda igual en los
#    agregados mensuales, que no se purgan nunca.
retencion_detalle_dias = {ret_detalle}
retencion_runs_dias    = {ret_runs}

[web]
# Solo local. Abrirla a la red tiene que ser una decision explicita
# (`bind = "0.0.0.0"`), porque la interfaz no tiene usuarios ni contraseñas y
# desde ella se encolan y se cancelan corridas.
bind  = "127.0.0.1"
port  = {puerto}
# token = "poner-algo-largo-aca"   # obligatorio si se cambia el bind

# Variables de entorno que reciben los pipelines al ser lanzados.
# [entorno]
# MI_VARIABLE = "valor"
"#,
        state = r.state,
        pipelines = r.pipelines,
        paralelo = r.paralelo,
        workers = r.workers,
        ram_max = r.ram_max,
        horas = r.timeout_horas,
        timeout_secs = timeout_secs,
        ret_detalle = r.retencion_detalle,
        ret_runs = r.retencion_runs,
        puerto = r.puerto,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sug() -> Respuestas {
        Respuestas::sugeridas(Path::new("/opt/zeruel"))
    }

    /// Lo generado tiene que poder leerlo el propio parser de config: si no,
    /// `init` produce un archivo que `serve` rechaza.
    #[test]
    fn lo_que_genera_se_puede_volver_a_leer() {
        let t = plantilla(&sug());
        let cfg: crate::config::Config =
            toml::from_str(&t).expect("el toml generado tiene que parsear");
        assert!(cfg.zeruel.timeout_secs.unwrap() > 0, "con tope de tiempo");
        assert!(cfg.zeruel.ram_max.is_some(), "con techo de RAM");
        assert!(cfg.zeruel.retencion_detalle_dias.unwrap() > 0);
        assert!(cfg.zeruel.retencion_runs_dias.unwrap() > 0);
    }

    /// El punto del comando: los limites vienen PUESTOS. Un archivo generado
    /// con los topes en cero seria el mismo problema que se quiso arreglar.
    #[test]
    fn los_limites_vienen_puestos_no_en_cero() {
        let s = sug();
        assert!(s.timeout_horas > 0.0, "corte por tiempo activo");
        assert!(
            s.ram_max != "0" && !s.ram_max.starts_with('0'),
            "techo de RAM activo"
        );
        assert!(s.paralelo >= 1);
        assert!(
            s.retencion_detalle <= s.retencion_runs,
            "el detalle no sobrevive a su run"
        );
    }

    /// El techo por run tiene que ser tal que todos los runs juntos entren en la
    /// maquina: si no, el tope no protege de nada.
    #[test]
    fn el_techo_por_run_por_los_runs_a_la_vez_entra_en_la_maquina() {
        let s = sug();
        let mb: u64 = s.ram_max.trim_end_matches('M').parse().unwrap();
        let mut sys = sysinfo::System::new_all();
        sys.refresh_memory();
        let total = sys.total_memory() / (1024 * 1024);
        assert!(
            mb * s.paralelo as u64 <= total,
            "{} MB x {} runs no entra en {} MB",
            mb,
            s.paralelo,
            total
        );
    }
}
