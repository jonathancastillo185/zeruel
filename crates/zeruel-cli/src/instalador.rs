//! Instalar y actualizar zeruel como un programa normal.
//!
//! # Por qué el propio binario es el instalador
//!
//! Instalar zeruel requería cinco comandos en orden, con rutas absolutas, en
//! una consola de administrador. En una instalación real se equivocaron **los
//! cinco**: se escribió `zeruel` sin la ruta (no está en el PATH), el
//! `desinstalar` dejó los servicios marcados para eliminación, se instaló sin
//! `--config` —dos días con la cola creciendo— y nadie concedió permisos sobre
//! los intérpretes.
//!
//! Ninguno es un error del usuario: es un procedimiento que pide demasiado.
//!
//! No hace falta MSI ni WiX. zeruel ya es un ejecutable único y autocontenido,
//! que es justamente la condición que hace posible lo más simple: **el
//! instalador y el programa son el mismo archivo**, así que no pueden estar en
//! versiones distintas. Es lo que hacen `rustup`, `deno` y `bun`.
//!
//! # La regla que ordena todo esto
//!
//! **Cada paso verifica que pasó, y si no, aborta diciendo qué quedó a medias.**
//! El antipatrón a evitar es el que ya nos mordió dos veces: un script que
//! imprime "listo" cuando el comando de abajo falló.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Dónde se instala por defecto.
///
/// No es `Program Files`: ahí escribir necesita privilegios en cada operación,
/// y zeruel escribe su base y sus logs todo el tiempo. Una carpeta propia en la
/// raíz es lo que usan las herramientas de línea de comandos que además
/// guardan estado.
pub const DESTINO_DEFECTO: &str = r"C:\zeruel";

/// Los dos servicios de una instalación.
///
/// Separados a propósito: reiniciar la web no puede interrumpir un ETL de dos
/// horas que está a la mitad.
fn nombres(base: &str) -> (String, String) {
    (base.to_string(), format!("{base}-web"))
}

/// Qué versión hay instalada en un destino, si hay alguna.
pub fn version_instalada(destino: &Path) -> Option<String> {
    let exe = destino.join("zeruel.exe");
    if !exe.is_file() {
        return None;
    }
    let salida = std::process::Command::new(&exe)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .output()
        .ok()?;
    let texto = String::from_utf8_lossy(&salida.stdout);
    // `clap` imprime "zeruel 0.1.0".
    texto.split_whitespace().nth(1).map(str::to_string)
}

/// La versión de este binario.
pub fn version_propia() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Copia este ejecutable al destino, sorteando que pueda estar en uso.
///
/// En Windows un `.exe` que está corriendo **no se puede sobreescribir**, pero
/// **sí se puede renombrar**: el proceso vivo se queda con su handle al archivo
/// viejo y termina tranquilo, mientras el nombre queda libre para el nuevo.
///
/// Sin esto, actualizar obligaría a parar todo y, si algo fallaba a mitad de
/// camino, a quedarse sin binario.
fn copiar_binario(destino: &Path) -> Result<PathBuf> {
    let origen = std::env::current_exe().context("no pude ubicar el ejecutable")?;
    let final_ = destino.join("zeruel.exe");

    if final_.exists() {
        // Si es el mismo archivo, no hay nada que copiar: pasa cuando se
        // ejecuta el instalador desde el propio destino.
        if same_file(&origen, &final_) {
            return Ok(final_);
        }
        let previo = destino.join("zeruel.exe.anterior");
        let _ = std::fs::remove_file(&previo);
        std::fs::rename(&final_, &previo).with_context(|| {
            format!(
                "no pude apartar el binario anterior. ¿Hay un zeruel corriendo desde {}?",
                destino.display()
            )
        })?;
    }
    std::fs::copy(&origen, &final_)
        .with_context(|| format!("no pude copiar a {}", final_.display()))?;
    Ok(final_)
}

/// ¿Son el mismo archivo? Se compara por metadatos y no por ruta: `C:\z\a.exe`
/// y `c:/z/./a.exe` son el mismo y se escriben distinto.
fn same_file(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

/// Lo que hay que hacer, y en qué estado quedó.
///
/// Se lleva registro para poder decir **qué quedó a medias** si algo falla, en
/// vez de dejar al usuario adivinando desde dónde retomar.
pub struct Progreso {
    pasos: Vec<(String, bool)>,
}

impl Progreso {
    fn nuevo() -> Self {
        Self { pasos: Vec::new() }
    }

    fn ok(&mut self, que: &str) {
        println!("  [ok]   {que}");
        self.pasos.push((que.to_string(), true));
    }

    fn falla(&mut self, que: &str) {
        println!("  [FALLA] {que}");
        self.pasos.push((que.to_string(), false));
    }

    /// Devuelve si quedó algo sin hacer.
    ///
    /// Quien llama tiene que decidir con eso y no seguir de largo: la primera
    /// versión imprimía el resumen con tres fallas y después anunciaba
    /// "listo. El panel está en http://localhost:9876" igual, con los dos
    /// servicios detenidos.
    fn resumen(&self) -> bool {
        let hechos = self.pasos.iter().filter(|(_, ok)| *ok).count();
        println!("\n{hechos} de {} pasos completados.", self.pasos.len());
        let fallidos: Vec<&String> = self
            .pasos
            .iter()
            .filter(|(_, ok)| !*ok)
            .map(|(que, _)| que)
            .collect();
        for que in &fallidos {
            println!("  quedó sin hacer: {que}");
        }
        !fallidos.is_empty()
    }
}

/// Prepara el destino: carpetas y configuración.
///
/// La configuración **no se pisa si ya existe**. Es la peor cosa que puede
/// hacer un instalador: los valores de producción no están en ningún otro lado.
fn preparar_destino(destino: &Path, p: &mut Progreso) -> Result<PathBuf> {
    for sub in ["", "state", "pipelines"] {
        let d = destino.join(sub);
        std::fs::create_dir_all(&d).with_context(|| format!("no pude crear {}", d.display()))?;
    }
    p.ok(&format!("carpetas en {}", destino.display()));

    let cfg = destino.join(crate::config::NOMBRE_DEFAULT);
    if cfg.is_file() {
        p.ok(&format!(
            "configuración existente, no se toca: {}",
            cfg.display()
        ));
    } else {
        crate::init::ejecutar_con_ruido(Some(cfg.clone()), true, false, false)
            .context("no pude crear la configuración")?;
        p.ok(&format!("configuración creada: {}", cfg.display()));
    }
    Ok(cfg)
}

/// Deja el destino en forma absoluta, y **lo dice** si hubo que resolverlo.
///
/// # Por qué esto existe
///
/// `--destino C:alerta` no es `C:\alerta`: en Windows una ruta con unidad y sin
/// barra es **relativa al directorio actual de esa unidad**. Escrito desde
/// `C:\alerta`, resuelve a `C:\alerta\alerta`.
///
/// Eso ya pasó, y el resultado fue una instalación anidada completa —binario de
/// 98 MB, configuración nueva, carpetas— creada en silencio dentro de la
/// instalación buena, más los servicios re-registrados apuntando ahí.
///
/// El daño no fue el error de tipeo: fue que nadie lo dijo.
///
/// Una ruta relativa común (`--destino .`, `--destino sub/dir`) es razonable: se
/// resuelve contra el directorio actual y **se muestra** dónde va a quedar. Pero
/// la forma con unidad y sin barra se **rechaza**, porque no hay lectura buena:
/// resolverla como manda Windows da `C:\alerta\alerta`, que es exactamente la
/// sorpresa que queremos evitar, y tratarla como `C:\alerta` sería inventar una
/// semántica distinta a la del sistema operativo.
fn resolver_destino(destino: PathBuf) -> Result<PathBuf> {
    if destino.is_absolute() {
        return Ok(destino);
    }

    // Con unidad pero sin barra: `C:alerta`. `Path::join` NO la arregla —una
    // ruta con prefijo descarta la base y vuelve igual—, así que sin este corte
    // seguiría de largo tal cual llegó.
    if es_relativa_a_unidad(&destino) {
        let d = destino.display();
        let sugerida = destino.to_string_lossy().replacen(':', r":\", 1);
        anyhow::bail!(
            "'{d}' lleva unidad pero no barra, y eso en Windows NO es la raíz de \
             la unidad: es relativo al directorio actual de esa unidad. Escrito \
             desde C:\\alerta, '{d}' resuelve a C:\\alerta\\alerta y te deja una \
             instalación anidada.\n\n¿Quisiste decir '{sugerida}'?"
        );
    }

    let base = std::env::current_dir().context("no pude ver el directorio actual")?;
    let absoluta = base.join(&destino);
    println!(
        "'{}' es una ruta relativa: la instalación va a {}\n",
        destino.display(),
        absoluta.display()
    );
    Ok(absoluta)
}

/// ¿Es de la forma `C:algo`? Trae prefijo de unidad pero no arranca en la raíz.
fn es_relativa_a_unidad(p: &Path) -> bool {
    matches!(p.components().next(), Some(std::path::Component::Prefix(_))) && !p.has_root()
}

/// Instala o actualiza en `destino`.
pub fn instalar(destino: Option<PathBuf>, nombre: &str, puerto: u16) -> Result<()> {
    let destino = resolver_destino(destino.unwrap_or_else(|| PathBuf::from(DESTINO_DEFECTO)))?;
    let previa = version_instalada(&destino);
    let (svc, svc_web) = nombres(nombre);

    match &previa {
        // "Ya está instalada" tiene que significar **instalada y andando**, no
        // sólo que el binario tenga el número de versión correcto.
        //
        // Con el binario al día pero los servicios rotos —detenidos, borrados,
        // o corriendo con arranque `Disabled`— volver a instalar es justamente
        // lo que corresponde. La primera versión miraba sólo la versión y se
        // iba diciendo "no hay nada que hacer" ante una instalación que no
        // funcionaba: el usuario hace lo correcto y el programa no hace nada.
        Some(v)
            if v == version_propia()
                && crate::servicio::sano(&svc)
                && crate::servicio::sano(&svc_web) =>
        {
            println!(
                "ya está instalada la versión {v} en {} y los dos servicios están \
                 corriendo.",
                destino.display()
            );
            // El contexto para IA sí se refresca, aunque no haya nada más que
            // hacer. Son archivos derivados y baratos, no necesitan permisos, y
            // sin esto una instalación sana nunca los recibiría: quedarían sólo
            // en las máquinas que además tuvieron algo roto.
            match crate::ia::escribir(&destino, nombre, puerto) {
                Ok(fs) => println!("  contexto para IA al día: {}", fs.join(", ")),
                Err(e) => println!("  no pude escribir el contexto para IA: {e}"),
            }
            return Ok(());
        }
        Some(v) if v == version_propia() => println!(
            "la versión {v} ya está en {}, pero los servicios no están sanos. \
             Reinstalando.\n",
            destino.display()
        ),
        Some(v) => println!(
            "actualizando {} : {v} → {}\n",
            destino.display(),
            version_propia()
        ),
        None => println!(
            "instalando zeruel {} en {}\n",
            version_propia(),
            destino.display()
        ),
    }

    // Elevarse UNA vez, antes de empezar.
    //
    // La primera versión dejaba que cada registro de servicio se elevara por su
    // cuenta, y el resultado en la máquina real fue: dos diálogos de UAC, y
    // todo lo que NO eleva —tocar el PATH, arrancar los servicios— corriendo
    // sin permisos y fallando. Terminó con "5 de 8 pasos" y los dos servicios
    // detenidos.
    if !crate::servicio::soy_administrador() {
        println!("hace falta ser administrador: te lo va a pedir una sola vez.\n");
        return crate::servicio::relanzar_elevado(
            &["instalar"],
            &[
                ("--destino", destino.to_string_lossy().into_owned()),
                ("--nombre", nombre.to_string()),
                ("--puerto", puerto.to_string()),
            ],
        );
    }

    let mut p = Progreso::nuevo();
    let cfg = preparar_destino(&destino, &mut p)?;

    // Los servicios se paran ANTES de tocar el binario: si están corriendo, el
    // archivo está tomado, y además una actualización no puede cortar un run a
    // la mitad. `parar` espera a la gracia configurada.
    if previa.is_some() {
        for s in [&svc, &svc_web] {
            match crate::servicio::parar_y_esperar(s) {
                Ok(true) => p.ok(&format!("servicio '{s}' detenido")),
                Ok(false) => {}
                Err(e) => p.falla(&format!("no pude detener '{s}': {e}")),
            }
        }
    }

    let exe = copiar_binario(&destino)?;
    p.ok(&format!("binario en {}", exe.display()));

    // Reinstalar los servicios y no sólo reemplazar el binario: los argumentos
    // con los que arranca un servicio quedan grabados en su registro, así que
    // un `--config` nuevo no llega hasta que se vuelven a crear. Fue
    // exactamente lo que dejó una instalación dos días con los valores de
    // fábrica.
    for (s, modo) in [
        (&svc, crate::servicio::Modo::Scheduler),
        (&svc_web, crate::servicio::Modo::Web),
    ] {
        let _ = crate::servicio::desinstalar_silencioso(s);
        match crate::servicio::instalar(
            s,
            modo,
            &destino.join("state"),
            &destino.join("pipelines"),
            60,
            puerto,
            Some(&cfg),
        ) {
            Ok(()) => p.ok(&format!("servicio '{s}' registrado")),
            Err(e) => {
                p.falla(&format!("servicio '{s}': {e}"));
                p.resumen();
                return Err(e);
            }
        }
    }

    match agregar_al_path(&destino) {
        Ok(true) => p.ok("agregado al PATH de la máquina"),
        Ok(false) => p.ok("ya estaba en el PATH"),
        Err(e) => p.falla(&format!("no pude tocar el PATH: {e}")),
    }

    // Dejar la carpeta lista para que un agente de IA la entienda sola.
    //
    // zeruel expone once herramientas por MCP y no las usaba nadie, porque no
    // estaban declaradas en ningún lado. Declararlo a mano en cada máquina se
    // olvida igual que instalar a mano, así que lo escribe el instalador.
    match crate::ia::escribir(&destino, nombre, puerto) {
        Ok(fs) => p.ok(&format!("contexto para IA: {}", fs.join(", "))),
        Err(e) => p.falla(&format!("no pude escribir el contexto para IA: {e}")),
    }

    for s in [&svc, &svc_web] {
        match crate::servicio::arrancar_y_verificar(s) {
            Ok(()) => p.ok(&format!("servicio '{s}' corriendo")),
            Err(e) => p.falla(&format!("'{s}' no arrancó: {e}")),
        }
    }

    let hubo_fallas = p.resumen();

    // El chequeo de herramientas al final y no al principio: recién ahora la
    // configuración está en su lugar definitivo.
    println!("\nprobando las herramientas de [entorno]...");
    let conf = crate::config::Config::cargar(Some(&cfg))?;
    crate::dependencias::chequear_y_reportar(&conf.entorno);

    // Decir "listo" con pasos fallidos arriba es la mentira que este proyecto
    // ya pagó tres veces. Si algo no salió, la instalación NO está lista, y hay
    // que terminar con error para que quien la haya llamado se entere.
    if hubo_fallas {
        anyhow::bail!(
            "la instalación quedó incompleta: mirá los pasos marcados arriba. \
             Nada de lo que quedó a medias se arregla solo."
        );
    }
    println!("\nlisto. El panel está en http://localhost:{puerto}");
    Ok(())
}

/// Agrega el destino al PATH de máquina. Devuelve si hizo falta cambiarlo.
///
/// Sin esto, `zeruel` sólo funciona con la ruta absoluta — que fue literalmente
/// el primer error al intentar instalar a mano.
///
/// # Por qué no `setx /M` y no `std::env::var("PATH")`
///
/// La primera versión hacía las dos cosas, y era destructiva de dos maneras
/// distintas. En la máquina donde se probó: PATH de máquina 768 caracteres,
/// PATH de usuario 1870, PATH del proceso 2639.
///
/// 1. `std::env::var("PATH")` devuelve máquina **+ usuario ya fusionados**.
///    Escribir eso como PATH de máquina le publica a todos los usuarios las
///    rutas privadas del que instaló.
/// 2. **`setx` trunca en 1024 caracteres**, con un aviso en stdout y código de
///    salida 0 — o sea que ni siquiera se detecta como error. Con 2639
///    caracteres el PATH de máquina quedaba cortado a la mitad de una entrada.
///
/// Sólo se salvó porque no tenía permisos y falló antes de escribir.
///
/// `[Environment]::SetEnvironmentVariable` no tiene el límite, escribe en el
/// ámbito que se le pide y avisa al resto del sistema (WM_SETTINGCHANGE). El
/// valor viaja por el entorno y no dentro del script: un PATH con comillas no
/// tiene por qué convertirse en código.
#[cfg(windows)]
fn agregar_al_path(destino: &Path) -> Result<bool> {
    let d = destino.to_string_lossy().to_string();
    let actual = leer_path_de_maquina()?;
    if ya_esta_en_el_path(&actual, &d) {
        return Ok(false);
    }

    let nuevo = concatenar_al_path(&actual, &d);
    let salida = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            "[Environment]::SetEnvironmentVariable('Path', $env:ZERUEL_PATH_NUEVO, 'Machine')",
        ])
        .env("ZERUEL_PATH_NUEVO", &nuevo)
        .output()
        .context("no pude escribir el PATH de la máquina")?;
    if !salida.status.success() {
        anyhow::bail!("{}", String::from_utf8_lossy(&salida.stderr).trim());
    }

    // Verificar y no confiar: es la misma regla que el resto del instalador, y
    // acá importa el doble porque un PATH mal escrito se nota mucho después.
    let quedo = leer_path_de_maquina().unwrap_or_default();
    if !ya_esta_en_el_path(&quedo, &d) {
        anyhow::bail!("escribí el PATH pero {d} no quedó adentro");
    }
    Ok(true)
}

/// Windows no distingue mayúsculas en las rutas y tolera la barra final, así
/// que `C:\zeruel`, `c:\zeruel\` y ` C:\ZERUEL ` son la misma carpeta. Sin
/// normalizar, reinstalar agrega una entrada repetida cada vez.
#[cfg(windows)]
fn normalizada(p: &str) -> String {
    p.trim().trim_end_matches(['\\', '/']).to_ascii_lowercase()
}

/// ¿El destino ya figura en este PATH?
#[cfg(windows)]
fn ya_esta_en_el_path(path: &str, destino: &str) -> bool {
    let d = normalizada(destino);
    !d.is_empty() && path.split(';').any(|p| normalizada(p) == d)
}

/// Agrega el destino al final, sin dejar un `;;` ni comerse una entrada.
///
/// Un PATH que ya termina en `;` es común y válido; concatenar sin mirarlo deja
/// una entrada vacía que algunas herramientas leen como "el directorio actual".
#[cfg(windows)]
fn concatenar_al_path(actual: &str, destino: &str) -> String {
    let base = actual.trim_end_matches(';');
    if base.is_empty() {
        destino.to_string()
    } else {
        format!("{base};{destino}")
    }
}

/// El PATH **de máquina**, leído del registro y sin fusionar con el del usuario.
#[cfg(windows)]
fn leer_path_de_maquina() -> Result<String> {
    let salida = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            "[Environment]::GetEnvironmentVariable('Path','Machine')",
        ])
        .output()
        .context("no pude leer el PATH de la máquina")?;
    if !salida.status.success() {
        anyhow::bail!("{}", String::from_utf8_lossy(&salida.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&salida.stdout)
        .trim_end()
        .to_string())
}

#[cfg(not(windows))]
fn agregar_al_path(_destino: &Path) -> Result<bool> {
    // En Unix el equivalente es un symlink en /usr/local/bin, y lo resuelve el
    // paquete de la distribución. No hay nada que hacer acá.
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn la_version_propia_es_la_del_paquete() {
        assert_eq!(version_propia(), env!("CARGO_PKG_VERSION"));
        assert!(!version_propia().is_empty());
    }

    /// Un destino sin instalación previa no puede aparentar tener una.
    #[test]
    fn un_destino_vacio_no_tiene_version() {
        let d = tempfile::tempdir().unwrap();
        assert!(version_instalada(d.path()).is_none());
    }

    /// Comparar rutas como texto diría que `C:\z\a.exe` y `c:/z/./a.exe` son
    /// distintos, y el instalador se copiaría encima de sí mismo.
    #[test]
    fn el_mismo_archivo_se_reconoce_aunque_la_ruta_se_escriba_distinto() {
        let d = tempfile::tempdir().unwrap();
        let a = d.path().join("x.txt");
        std::fs::write(&a, "hola").unwrap();
        let b = d.path().join(".").join("x.txt");
        assert!(same_file(&a, &b));

        let otro = d.path().join("y.txt");
        std::fs::write(&otro, "hola").unwrap();
        assert!(!same_file(&a, &otro), "mismo contenido no es mismo archivo");
    }

    /// `C:alerta` (unidad, sin barra) es relativo al directorio actual de esa
    /// unidad, no a la raíz. Escrito desde `C:\alerta` resuelve a
    /// `C:\alerta\alerta`, y eso creó una instalación anidada entera en
    /// silencio. Tiene que quedar absoluto antes de tocar el disco.
    #[cfg(windows)]
    #[test]
    fn una_ruta_con_unidad_y_sin_barra_no_se_toma_por_absoluta() {
        assert!(
            !Path::new(r"C:alerta").is_absolute(),
            "si esto cambia, resolver_destino deja de protegernos"
        );
        assert!(Path::new(r"C:\alerta").is_absolute());
        assert!(es_relativa_a_unidad(Path::new(r"C:alerta")));
        assert!(!es_relativa_a_unidad(Path::new(r"C:\alerta")));
        assert!(!es_relativa_a_unidad(Path::new("sub/dir")));

        // Se rechaza en vez de resolverse: no hay lectura buena. `Path::join`
        // tampoco la arregla —vuelve igual—, que fue como paso desapercibida.
        assert_eq!(
            Path::new(r"C:\base").join(r"C:alerta"),
            Path::new(r"C:alerta")
        );

        let e = resolver_destino(PathBuf::from(r"C:alerta")).unwrap_err();
        let msg = e.to_string();
        assert!(
            msg.contains(r"C:\alerta\alerta"),
            "tiene que mostrar el daño: {msg}"
        );
        assert!(
            msg.contains(r"C:\alerta"),
            "y sugerir lo que quiso decir: {msg}"
        );
    }

    /// Una ruta ya absoluta se deja intacta: normalizarla de más cambiaría el
    /// destino que el usuario escribió.
    #[test]
    fn una_ruta_absoluta_no_se_toca() {
        let esperada = if cfg!(windows) {
            PathBuf::from(r"C:\alerta")
        } else {
            PathBuf::from("/opt/zeruel")
        };
        assert_eq!(resolver_destino(esperada.clone()).unwrap(), esperada);
    }

    /// El progreso tiene que poder decir qué quedó a medias: es la diferencia
    /// entre "falló algo" y "falló esto, lo demás quedó hecho".
    #[test]
    fn el_progreso_distingue_lo_hecho_de_lo_que_falto() {
        let mut p = Progreso::nuevo();
        p.ok("uno");
        p.falla("dos");
        p.ok("tres");
        assert_eq!(p.pasos.len(), 3);
        assert_eq!(p.pasos.iter().filter(|(_, ok)| *ok).count(), 2);
        assert_eq!(p.pasos.iter().filter(|(_, ok)| !*ok).count(), 1);
    }

    /// Reinstalar no puede ir agregando la misma carpeta una y otra vez. En
    /// Windows la comparación es sin distinguir mayúsculas y tolerando la barra
    /// final, porque las tres formas nombran la misma carpeta.
    #[cfg(windows)]
    #[test]
    fn el_destino_que_ya_esta_en_el_path_se_reconoce_escrito_de_cualquier_forma() {
        let path = r"C:\Windows;C:\zeruel;C:\Windows\System32";
        for escrito in [r"C:\zeruel", r"c:\zeruel", r"C:\ZERUEL\", r"  C:\zeruel  "] {
            assert!(
                ya_esta_en_el_path(path, escrito),
                "{escrito} tendría que reconocerse como ya presente"
            );
        }
        assert!(
            !ya_esta_en_el_path(path, r"C:\zeruel2"),
            "no puede confundir una carpeta distinta con prefijo parecido"
        );
    }

    /// Un PATH que termina en `;` es válido y común. Concatenar sin mirarlo
    /// deja una entrada vacía, que varias herramientas leen como "el directorio
    /// actual" — un cambio de comportamiento que nadie pidió.
    #[cfg(windows)]
    #[test]
    fn concatenar_no_deja_entradas_vacias() {
        assert_eq!(concatenar_al_path(r"C:\a", r"C:\z"), r"C:\a;C:\z");
        assert_eq!(concatenar_al_path(r"C:\a;", r"C:\z"), r"C:\a;C:\z");
        assert_eq!(concatenar_al_path("", r"C:\z"), r"C:\z");
        assert_eq!(concatenar_al_path(";", r"C:\z"), r"C:\z");
    }

    /// El PATH de máquina de una Windows normal ya pasa holgadamente el límite
    /// de 1024 caracteres de `setx`, que trunca **sin fallar** (código de salida
    /// 0 y un aviso en stdout). Este test fija por qué no se usa `setx`: lo que
    /// se arma acá no tiene por qué entrar en ese límite.
    #[cfg(windows)]
    #[test]
    fn el_path_armado_puede_superar_el_limite_de_setx() {
        let largo = std::iter::repeat_n(r"C:\una\carpeta\con\nombre\largo", 40)
            .collect::<Vec<_>>()
            .join(";");
        let nuevo = concatenar_al_path(&largo, r"C:\zeruel");
        assert!(nuevo.len() > 1024, "el caso de prueba tiene que exceder");
        assert!(
            ya_esta_en_el_path(&nuevo, r"C:\zeruel"),
            "el destino tiene que quedar adentro y entero"
        );
    }
}
