//! Ver y cambiar los ajustes sin editar el TOML a mano.
//!
//! # Por qué el CLI y no el panel
//!
//! El servicio web tiene permiso de **sólo lectura** sobre su propia
//! configuración, a propósito: corre con una cuenta virtual aislada, y que un
//! proceso expuesto por HTTP pueda reescribir con qué arranca el scheduler es
//! justo lo que esa cuenta existe para impedir. Además el panel todavía no
//! tiene autenticación que funcione.
//!
//! Así que el panel **muestra** y este comando **cambia**: corre con los
//! permisos de quien lo ejecuta, que es quien debe tenerlos.
//!
//! # Por qué se edita línea por línea y no se reserializa
//!
//! Un `zeruel.toml` de producción tiene más comentarios que valores, y esos
//! comentarios son documentación cara: por qué el token está apagado, por qué
//! ciertos DAGs están excluidos, qué ruta corresponde a qué contenedor.
//! Deserializar y volver a escribir la estructura los borraría todos de una vez.
//!
//! Por eso se toca **sólo la línea del valor**, dejando intacto el resto del
//! archivo — incluidos el alineado y el comentario al final de esa misma línea.

use std::path::Path;

use anyhow::{Context, Result};

/// Qué tipo de valor admite un ajuste, y cómo se valida.
pub enum Tipo {
    /// Entero con rango cerrado. El rango no es decorativo: `paralelo = 0`
    /// deja el scheduler sin ejecutar nada y parece un cuelgue.
    Entero(u64, u64),
    /// Tamaño de memoria: `2500M`, `2G`, o `0` para sin tope.
    Memoria,
    /// Texto libre (una dirección de escucha, por ejemplo).
    Texto,
}

/// Un ajuste editable.
///
/// Esta tabla es la **única** fuente: valida lo que entra por el CLI y es lo
/// que dibuja el panel. Agregar un ajuste en un solo lugar evita que la ayuda
/// diga una cosa y el programa haga otra.
pub struct Ajuste {
    pub clave: &'static str,
    pub seccion: &'static str,
    pub tipo: Tipo,
    pub que_hace: &'static str,
    /// Qué hay que reiniciar para que el cambio tenga efecto.
    pub aplica: &'static str,
}

pub const AJUSTES: &[Ajuste] = &[
    Ajuste {
        clave: "paralelo",
        seccion: "zeruel",
        tipo: Tipo::Entero(1, 64),
        que_hace: "Cuántos runs corren a la vez, cada uno en su proceso.",
        aplica: "scheduler",
    },
    Ajuste {
        clave: "workers",
        seccion: "zeruel",
        tipo: Tipo::Entero(1, 64),
        que_hace: "Cuántas tareas en paralelo DENTRO de un run.",
        aplica: "scheduler",
    },
    Ajuste {
        clave: "ram_max",
        seccion: "zeruel",
        tipo: Tipo::Memoria,
        que_hace: "Techo de memoria por run. 0 = sin tope (un pipeline \
                   desbocado se lleva la máquina).",
        aplica: "scheduler",
    },
    Ajuste {
        clave: "timeout_secs",
        seccion: "zeruel",
        tipo: Tipo::Entero(0, 604_800),
        que_hace: "Corta un run que pase de estos segundos. 0 = sin tope.",
        aplica: "scheduler",
    },
    Ajuste {
        clave: "gracia_secs",
        seccion: "zeruel",
        tipo: Tipo::Entero(0, 600),
        que_hace: "Al apagar, cuánto se espera a los runs en curso antes de \
                   cortarlos.",
        aplica: "scheduler",
    },
    Ajuste {
        clave: "tick_ms",
        seccion: "zeruel",
        tipo: Tipo::Entero(100, 60_000),
        que_hace: "Cada cuánto mira la cola y re-escanea los pipelines.",
        aplica: "scheduler",
    },
    Ajuste {
        clave: "retencion_detalle_dias",
        seccion: "zeruel",
        tipo: Tipo::Entero(0, 3650),
        que_hace: "Días que se guarda el detalle por tarea y los logs. Es lo \
                   que más ocupa.",
        aplica: "scheduler",
    },
    Ajuste {
        clave: "retencion_runs_dias",
        seccion: "zeruel",
        tipo: Tipo::Entero(0, 3650),
        que_hace: "Días que se guarda el listado de corridas. Los agregados \
                   mensuales sobreviven a esta purga.",
        aplica: "scheduler",
    },
    Ajuste {
        clave: "port",
        seccion: "web",
        tipo: Tipo::Entero(1, 65_535),
        que_hace: "Puerto del panel.",
        aplica: "web",
    },
    Ajuste {
        clave: "bind",
        seccion: "web",
        tipo: Tipo::Texto,
        que_hace: "Dirección donde escucha el panel. 127.0.0.1 no sale de la \
                   máquina.",
        aplica: "web",
    },
];

pub fn buscar(clave: &str) -> Option<&'static Ajuste> {
    AJUSTES.iter().find(|a| a.clave == clave)
}

/// Valida un valor contra su ajuste y lo devuelve tal como va escrito en TOML.
///
/// Devolver el texto TOML —y no el valor— es lo que hace que un entero quede sin
/// comillas y un texto con ellas, sin que quien llama tenga que acordarse.
pub fn validar(a: &Ajuste, valor: &str) -> Result<String> {
    match a.tipo {
        Tipo::Entero(min, max) => {
            let n: u64 = valor.trim().parse().map_err(|_| {
                anyhow::anyhow!(
                    "'{valor}' no es un número entero. {} espera entre {min} y {max}.",
                    a.clave
                )
            })?;
            if n < min || n > max {
                anyhow::bail!(
                    "{} = {n} está fuera de rango: tiene que estar entre {min} y {max}.",
                    a.clave
                );
            }
            Ok(n.to_string())
        }
        Tipo::Memoria => {
            // Se valida con el mismo parser que usa el scheduler: si acá pasara
            // algo que allá no, el error saldria recien al arrancar el servicio.
            zeruel::parsear_memoria(valor.trim()).map_err(|e| {
                anyhow::anyhow!("'{valor}' no es un tamaño válido ({e}). Ejemplos: 2500M, 2G, 0.")
            })?;
            Ok(format!("\"{}\"", valor.trim()))
        }
        Tipo::Texto => Ok(format!("\"{}\"", valor.trim())),
    }
}

/// Cambia una clave en el texto de un TOML, tocando **sólo su línea**.
///
/// Devuelve el texto nuevo y el valor anterior si lo había.
pub fn editar_toml(
    texto: &str,
    seccion: &str,
    clave: &str,
    valor_toml: &str,
) -> (String, Option<String>) {
    let mut salida: Vec<String> = Vec::new();
    let mut en_seccion = false;
    let mut escrito = false;
    let mut anterior = None;
    // Dónde termina la sección, por si hay que agregar la clave al final.
    let mut fin_seccion: Option<usize> = None;

    for linea in texto.lines() {
        let limpia = linea.trim_start();

        if limpia.starts_with('[') {
            // Al abrir otra sección se cierra la anterior: acá es donde habría
            // que insertar la clave si no aparecio.
            if en_seccion && !escrito {
                fin_seccion = Some(salida.len());
            }
            en_seccion = limpia.starts_with(&format!("[{seccion}]"));
            salida.push(linea.to_string());
            continue;
        }

        // Un comentario NO es una asignación, por más que mencione la clave. El
        // `zeruel.toml` de produccion documenta varios ajustes en prosa, y
        // pisar uno de esos dejaria el archivo con un valor invisible.
        if en_seccion && !escrito && !limpia.starts_with('#') {
            if let Some((izq, der)) = linea.split_once('=') {
                if izq.trim() == clave {
                    anterior = Some(valor_de(der));
                    // Se preserva el alineado de la izquierda y el comentario
                    // que pueda venir a la derecha.
                    let comentario = der.find('#').map(|i| der[i..].to_string());
                    let nueva = match comentario {
                        Some(c) => format!("{izq}= {valor_toml}  {c}"),
                        None => format!("{izq}= {valor_toml}"),
                    };
                    salida.push(nueva);
                    escrito = true;
                    continue;
                }
            }
        }
        salida.push(linea.to_string());
    }

    if !escrito {
        let linea = format!("{clave} = {valor_toml}");
        match fin_seccion.or(if en_seccion { Some(salida.len()) } else { None }) {
            Some(i) => salida.insert(i, linea),
            None => {
                // La sección no existe: se crea al final.
                salida.push(String::new());
                salida.push(format!("[{seccion}]"));
                salida.push(linea);
            }
        }
    }

    // Respetar el fin de linea del archivo.
    //
    // `lines()` se come el \r y juntar con \n convertiria un archivo CRLF
    // entero a LF. No rompe nada -TOML parsea igual- pero reescribe cada linea:
    // el diff de un cambio de un caracter sale con el archivo completo, y ahi
    // adentro no se puede revisar nada.
    let salto = if texto.contains("\r\n") { "\r\n" } else { "\n" };
    let mut texto = salida.join(salto);
    texto.push_str(salto);
    (texto, anterior)
}

/// El valor de la derecha de un `=`, sin comentario ni comillas.
fn valor_de(der: &str) -> String {
    let sin_comentario = match der.find('#') {
        Some(i) => &der[..i],
        None => der,
    };
    sin_comentario.trim().trim_matches('"').to_string()
}

/// `zeruel config set <clave> <valor>`
pub fn fijar(ruta: &Path, clave: &str, valor: &str) -> Result<()> {
    let a = buscar(clave).ok_or_else(|| {
        let lista: Vec<&str> = AJUSTES.iter().map(|a| a.clave).collect();
        anyhow::anyhow!(
            "no conozco el ajuste '{clave}'. Los que hay: {}",
            lista.join(", ")
        )
    })?;
    let valor_toml = validar(a, valor)?;

    let texto = std::fs::read_to_string(ruta)
        .with_context(|| format!("no pude leer {}", ruta.display()))?;
    let (nuevo, anterior) = editar_toml(&texto, a.seccion, a.clave, &valor_toml);

    // Releer lo que se va a escribir y comprobar que sigue siendo TOML valido
    // ANTES de tocar el archivo. Un config roto no se nota al guardarlo: se
    // nota cuando el servicio no arranca, que es varias horas despues.
    toml::from_str::<toml::Value>(&nuevo)
        .context("el cambio dejaria el archivo invalido, asi que no lo escribi")?;

    std::fs::write(ruta, &nuevo).with_context(|| format!("no pude escribir {}", ruta.display()))?;

    match anterior {
        Some(v) if v == valor.trim() => println!("{clave} ya valía {valor}. No cambié nada."),
        Some(v) => println!("{clave}: {v} → {valor}"),
        None => println!("{clave} = {valor}  (no estaba en el archivo; lo agregué)"),
    }
    println!("\nEsto NO tiene efecto hasta reiniciar el {}.", a.aplica);
    Ok(())
}

/// `zeruel config` — muestra los ajustes y de dónde salen.
pub fn mostrar(ruta: &Path) -> Result<()> {
    let texto = std::fs::read_to_string(ruta)
        .with_context(|| format!("no pude leer {}", ruta.display()))?;
    let doc: toml::Value = toml::from_str(&texto).context("el archivo no es TOML válido")?;

    println!("configuración en {}\n", ruta.display());
    for a in AJUSTES {
        let v = doc
            .get(a.seccion)
            .and_then(|s| s.get(a.clave))
            .map(|v| match v {
                toml::Value::String(s) => s.clone(),
                otro => otro.to_string(),
            });
        match v {
            Some(v) => println!("  {:<24} {}", a.clave, v),
            None => println!("  {:<24} (de fábrica)", a.clave),
        }
    }
    println!("\nPara cambiar uno:  zeruel config set <clave> <valor>");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EJEMPLO: &str = r#"# Un comentario de arriba de todo.
[zeruel]
state     = "C:/alerta/state"
paralelo  = 6   # cuantos a la vez
workers   = 4

# El token esta comentado a proposito:
#     port = 1234
[web]
port  = 9876
"#;

    #[test]
    fn cambiar_un_valor_deja_el_resto_del_archivo_intacto() {
        let (nuevo, antes) = editar_toml(EJEMPLO, "zeruel", "paralelo", "10");
        assert_eq!(antes.as_deref(), Some("6"));
        assert!(nuevo.contains("paralelo  = 10"), "quedo:\n{nuevo}");
        assert!(nuevo.contains("# Un comentario de arriba de todo."));
        assert!(nuevo.contains("# El token esta comentado a proposito:"));
        assert!(nuevo.contains(r#"state     = "C:/alerta/state""#));
        toml::from_str::<toml::Value>(&nuevo).expect("sigue siendo TOML valido");
    }

    /// El comentario al final de la línea explica por qué está ese valor.
    /// Borrarlo al cambiar el número es perder justo la parte que importa.
    #[test]
    fn se_conserva_el_comentario_de_la_misma_linea() {
        let (nuevo, _) = editar_toml(EJEMPLO, "zeruel", "paralelo", "10");
        assert!(nuevo.contains("# cuantos a la vez"), "quedo:\n{nuevo}");
    }

    /// Una línea comentada que menciona la clave NO es una asignación. Pisarla
    /// dejaría el valor escondido adentro de un comentario, o sea sin efecto y
    /// sin explicación.
    #[test]
    fn una_clave_comentada_no_se_confunde_con_la_de_verdad() {
        let (nuevo, antes) = editar_toml(EJEMPLO, "web", "port", "8080");
        assert_eq!(antes.as_deref(), Some("9876"));
        assert!(
            nuevo.contains("#     port = 1234"),
            "no se toca el comentario"
        );
        assert!(nuevo.contains("port  = 8080"));
        let doc: toml::Value = toml::from_str(&nuevo).unwrap();
        assert_eq!(doc["web"]["port"].as_integer(), Some(8080));
    }

    /// La misma clave puede existir en dos secciones. Cambiar la de `[web]` no
    /// puede tocar la de `[zeruel]`.
    #[test]
    fn no_se_cruza_la_misma_clave_de_otra_seccion() {
        let dos = "[zeruel]\nport = 1\n\n[web]\nport = 2\n";
        let (nuevo, antes) = editar_toml(dos, "web", "port", "3");
        assert_eq!(antes.as_deref(), Some("2"));
        let doc: toml::Value = toml::from_str(&nuevo).unwrap();
        assert_eq!(doc["zeruel"]["port"].as_integer(), Some(1), "no se toca");
        assert_eq!(doc["web"]["port"].as_integer(), Some(3));
    }

    #[test]
    fn una_clave_que_falta_se_agrega_en_su_seccion() {
        let (nuevo, antes) = editar_toml(EJEMPLO, "zeruel", "timeout_secs", "3600");
        assert!(antes.is_none());
        let doc: toml::Value = toml::from_str(&nuevo).unwrap();
        assert_eq!(doc["zeruel"]["timeout_secs"].as_integer(), Some(3600));
        assert_eq!(
            doc["web"]["port"].as_integer(),
            Some(9876),
            "no se movio de seccion"
        );
    }

    /// Un archivo con finales CRLF —lo normal en Windows, que es donde corre
    /// esto— no puede volver convertido a LF entero. TOML parsea igual, pero el
    /// diff de un cambio de un carácter sale con el archivo completo adentro, y
    /// así no hay forma de revisar qué se tocó.
    #[test]
    fn no_se_cambian_los_finales_de_linea_del_archivo() {
        let crlf = "[zeruel]\r\nparalelo = 6\r\nworkers = 4\r\n";
        let (nuevo, _) = editar_toml(crlf, "zeruel", "paralelo", "10");
        assert!(nuevo.contains("\r\n"), "perdió los CRLF");
        assert!(
            !nuevo.replace("\r\n", "").contains('\n'),
            "quedó mezclado: {nuevo:?}"
        );
        assert_eq!(nuevo.matches("\r\n").count(), crlf.matches("\r\n").count());

        let lf = "[zeruel]\nparalelo = 6\n";
        let (nuevo, _) = editar_toml(lf, "zeruel", "paralelo", "10");
        assert!(!nuevo.contains('\r'), "no puede inventar CRLF: {nuevo:?}");
    }

    #[test]
    fn una_seccion_que_falta_se_crea() {
        let (nuevo, _) = editar_toml("[zeruel]\nparalelo = 1\n", "web", "port", "9000");
        let doc: toml::Value = toml::from_str(&nuevo).unwrap();
        assert_eq!(doc["web"]["port"].as_integer(), Some(9000));
    }

    /// `paralelo = 0` deja el scheduler sin ejecutar nada, y desde afuera se ve
    /// igual que un cuelgue. El rango existe para que eso no se pueda escribir.
    #[test]
    fn los_rangos_frenan_un_valor_que_romperia_el_scheduler() {
        let a = buscar("paralelo").unwrap();
        assert!(validar(a, "0").is_err(), "paralelo=0 tiene que rechazarse");
        assert!(validar(a, "10").is_ok());
        assert!(validar(a, "1000").is_err());
        assert!(validar(a, "diez").is_err());
    }

    /// El tamaño se valida con el mismo parser que usa el scheduler: si acá
    /// pasara algo que allá no, el error aparecería recién al arrancar.
    #[test]
    fn la_memoria_usa_el_mismo_parser_que_el_scheduler() {
        let a = buscar("ram_max").unwrap();
        assert_eq!(validar(a, "2500M").unwrap(), "\"2500M\"");
        assert!(validar(a, "2G").is_ok());
        assert!(validar(a, "0").is_ok());
        assert!(validar(a, "muchisima").is_err());
    }

    #[test]
    fn un_ajuste_que_no_existe_se_dice_con_la_lista_de_los_que_hay() {
        let d = tempfile::tempdir().unwrap();
        let ruta = d.path().join("zeruel.toml");
        std::fs::write(&ruta, EJEMPLO).unwrap();
        let e = fijar(&ruta, "paralelismo", "10").unwrap_err().to_string();
        assert!(e.contains("paralelo"), "tiene que sugerir los validos: {e}");
    }
}
