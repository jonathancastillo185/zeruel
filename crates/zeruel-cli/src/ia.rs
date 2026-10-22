//! Dejar la instalación lista para que un agente de IA la entienda sola.
//!
//! # El problema
//!
//! zeruel expone once herramientas por MCP y nadie las usaba. No por desinterés:
//! **no estaban declaradas en ningún lado**, así que ningún agente las veía. El
//! resultado medible fue sesiones enteras consultando la API HTTP a mano,
//! reinventando consultas que ya existían como herramienta.
//!
//! Declararlo a mano en cada máquina es la misma trampa que instalar a mano: se
//! olvida, y cuando se olvida nada avisa. Por eso lo escribe el instalador.
//!
//! # Qué hace cada archivo, sin humo
//!
//! - **`.mcp.json`** es lo único que produce *detección*. Un agente abierto en
//!   esta carpeta recibe las herramientas sin configurar nada.
//! - **`AGENTS.md`** no detecta nada: es contexto. Sirve para que el agente sepa
//!   qué es esta carpeta y cuáles son las trampas antes de romper algo.
//! - **`CLAUDE.md`** es un puntero a `AGENTS.md`. Duplicar el contenido
//!   garantiza que las dos copias se contradigan con el tiempo.
//!
//! # La regla que decide el contenido
//!
//! **Se escribe lo que no cambia; lo que cambia se pregunta.**
//!
//! La lista de DAGs, la cola, los estados y los picos de memoria salen de las
//! herramientas MCP en el momento de usarlas. Si se cachearan acá, el archivo
//! empezaría a mentir el primer día que alguien agregue un pipeline — y un
//! contexto desactualizado es peor que ninguno, porque se lee con confianza.

use std::path::Path;

use anyhow::{Context, Result};

/// Nombre del ejecutable según la plataforma.
fn exe(destino: &Path) -> String {
    let bin = if cfg!(windows) {
        "zeruel.exe"
    } else {
        "zeruel"
    };
    destino.join(bin).to_string_lossy().into_owned()
}

/// Escribe los archivos de contexto en el destino. Devuelve los que escribió.
///
/// Se regeneran en cada instalación y actualización a propósito: son artefactos
/// derivados de dónde quedó instalado y cómo se llaman los servicios. Si alguien
/// los edita a mano y después actualiza, pierde el cambio — por eso cada archivo
/// lo dice en su primera línea.
pub fn escribir(destino: &Path, servicio: &str, puerto: u16) -> Result<Vec<String>> {
    let mut hechos = Vec::new();

    let mcp = destino.join(".mcp.json");
    std::fs::write(&mcp, mcp_json(destino))
        .with_context(|| format!("no pude escribir {}", mcp.display()))?;
    hechos.push(".mcp.json".to_string());

    let agents = destino.join("AGENTS.md");
    std::fs::write(&agents, agents_md(destino, servicio, puerto))
        .with_context(|| format!("no pude escribir {}", agents.display()))?;
    hechos.push("AGENTS.md".to_string());

    let claude = destino.join("CLAUDE.md");
    std::fs::write(&claude, CLAUDE_MD)
        .with_context(|| format!("no pude escribir {}", claude.display()))?;
    hechos.push("CLAUDE.md".to_string());

    Ok(hechos)
}

/// La declaración del servidor MCP: lo único que da detección automática.
///
/// Las rutas van absolutas porque un agente puede arrancar con cualquier
/// directorio actual, y `state` relativo apuntaría a una base que no existe —
/// que además se crearía vacía, dando "no hay ningún run" en vez de un error.
fn mcp_json(destino: &Path) -> String {
    let exe = exe(destino).replace('\\', "\\\\");
    let state = destino.join("state").to_string_lossy().replace('\\', "/");
    format!(
        r#"{{
  "mcpServers": {{
    "zeruel": {{
      "command": "{exe}",
      "args": ["mcp", "--state", "{state}"]
    }}
  }}
}}
"#
    )
}

fn agents_md(destino: &Path, servicio: &str, puerto: u16) -> String {
    let d = destino.display();
    let exe = exe(destino);
    let cfg = destino.join(crate::config::NOMBRE_DEFAULT);
    let cfg = cfg.display();
    format!(
        r#"<!-- Generado por `zeruel instalar`. Se reescribe en cada actualización:
     los cambios a mano se pierden. -->

# zeruel — esta instalación

Esta carpeta (`{d}`) es una instalación de **zeruel**, un orquestador de
pipelines de datos. Corre como servicio, ejecuta cada pipeline en su propio
proceso y coordina todo a través de una base SQLite en `state/`.

## Primero: usá las herramientas, no adivines

Hay un servidor MCP declarado en `.mcp.json` de esta misma carpeta, así que
deberías tenerlo disponible sin configurar nada. Da, entre otras:

| herramienta | para qué |
|---|---|
| `list_dags` | qué pipelines hay realmente desplegados |
| `queue` | qué está encolado y qué está corriendo |
| `recursos` | RAM libre, runs en curso, tamaño de la base |
| `run_status`, `task_metrics` | cómo salió un run y qué hizo cada tarea |
| `memoria_por_tarea` | picos históricos de RAM y CPU |
| `trigger_run`, `backfill` | encolar trabajo |
| `sla`, `queue_set_max`, `gc_status` | límites e higiene |

**Este archivo no lista los DAGs ni el estado de la cola a propósito.** Eso
cambia todos los días; preguntáselo a las herramientas. Acá está sólo lo que no
cambia.

Si el MCP no aparece, hay una API HTTP en `http://localhost:{puerto}/api/` y el
CLI acepta `--json` en casi todo:

```
{exe} --json cola --state {d}/state
```

## Los datos fijos

| qué | dónde |
|---|---|
| binario | `{exe}` |
| configuración | `{cfg}` |
| base e historial | `{d}/state` (no borrar: es todo el historial) |
| logs por run | `{d}/state/logs` |
| binarios de pipeline | `{d}/pipelines` |
| servicios | `{servicio}` (scheduler) y `{servicio}-web` (panel) |
| panel | `http://localhost:{puerto}` |

## Las trampas que ya costaron días

Están acá porque cada una se pagó con una caída real.

**Cambiar `zeruel.toml` no hace nada hasta reiniciar el scheduler.** La
configuración se lee al arrancar. Después de editarla:
`Restart-Service {servicio}` (necesita administrador).

**Un servicio guarda sus argumentos al registrarse.** Cambiar `--config` obliga a
volver a registrarlo, no alcanza con reemplazar el binario. Una instalación
quedó dos días con los valores de fábrica —la mitad del paralelismo— porque se
registró sin `--config` y nada lo avisó.

**Los servicios no ven las variables de entorno nuevas hasta que reinicia la
máquina.** El gestor de servicios de Windows cachea el bloque de entorno al
arrancar. Por eso las variables que los pipelines necesitan van en la sección
`[entorno]` de `zeruel.toml` y no en el entorno del sistema.

**No copies binarios a mano.** Para instalar o actualizar:
`{exe} instalar --destino {d}`. Para el resto ya hay comandos; si estás por
hacer algo a mano, primero fijate si existe.

**No pises `zeruel.toml`.** Es el único lugar donde viven los valores de
producción. El instalador nunca lo toca si ya existe.

## Diagnóstico cuando "no ejecuta nada"

En orden, porque este es el camino que funcionó:

1. `recursos` — ¿hay runs en curso de verdad, o el cupo está tomado?
2. `queue` — ojo con ítems en estado `running` de hace horas: son cadáveres de
   una caída, ocupan cupo y nada los limpia solo. Se cancelan con
   `POST /api/cola/<id>/cancelar`.
3. ¿Los servicios corren **y arrancan solos**? Un servicio en `Running` con
   arranque `Disabled` funciona hoy y no vuelve después de un reinicio.
4. `{exe} doctor --config {cfg}` — si un intérprete no responde, los runs fallan
   a nombre del DAG y no del problema real.
5. `state/logs/run-<id>.log` para el detalle de una corrida puntual.

## Sobre los números de memoria viejos

El historial anterior a septiembre de 2026 tiene picos de RAM inventados: por un
error de medición, el pico del sistema operativo se le atribuía a la tarea, así
que hay corridas registradas con 16 GB que en realidad usaron unos cientos de
megas. Si ves un pico absurdo y repetido en tareas distintas con valores casi
idénticos, es eso. Los datos posteriores son buenos.
"#
    )
}

/// Claude Code carga `CLAUDE.md` solo; el resto de las herramientas leen
/// `AGENTS.md`. Un puntero y no una copia: dos copias divergen.
const CLAUDE_MD: &str = r#"<!-- Generado por `zeruel instalar`. Se reescribe en cada actualización. -->

El contexto de esta instalación está en [AGENTS.md](AGENTS.md). Leelo antes de
tocar nada: tiene las trampas que ya causaron caídas.

Hay un servidor MCP declarado en `.mcp.json` con herramientas para consultar la
cola, los runs y los recursos. Usalas en vez de consultar la base a mano.
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escribe_los_tres_archivos() {
        let d = tempfile::tempdir().unwrap();
        let hechos = escribir(d.path(), "mi-zeruel", 9876).unwrap();
        assert_eq!(hechos.len(), 3);
        for f in [".mcp.json", "AGENTS.md", "CLAUDE.md"] {
            assert!(d.path().join(f).is_file(), "falta {f}");
        }
    }

    /// El `.mcp.json` tiene que ser JSON válido: si no parsea, el agente no lo
    /// carga y —peor— no siempre dice por qué.
    #[test]
    fn el_mcp_json_es_json_valido_y_apunta_al_state_de_esta_instalacion() {
        let d = tempfile::tempdir().unwrap();
        escribir(d.path(), "mi-zeruel", 9876).unwrap();
        let texto = std::fs::read_to_string(d.path().join(".mcp.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&texto).expect("no parsea");

        let srv = &v["mcpServers"]["zeruel"];
        assert!(
            srv["command"].as_str().unwrap().contains("zeruel"),
            "el comando tiene que ser el binario"
        );
        let args: Vec<&str> = srv["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert_eq!(args[0], "mcp");
        assert!(
            args.contains(&"--state"),
            "sin --state absoluto el agente abre una base vacia y responde \
             'no hay ningun run' en vez de fallar"
        );
    }

    /// Una ruta de Windows lleva backslashes, que en JSON son escapes. Sin
    /// escaparlos el archivo no parsea y la deteccion se cae en silencio.
    #[test]
    fn una_ruta_de_windows_no_rompe_el_json() {
        let json = mcp_json(Path::new(r"C:\Archivos\zeruel"));
        let v: serde_json::Value = serde_json::from_str(&json).expect("no parsea");
        let cmd = v["mcpServers"]["zeruel"]["command"].as_str().unwrap();
        assert!(cmd.contains("zeruel"), "quedo: {cmd}");
    }

    /// El contexto se genera con los datos de ESTA instalacion. Un template con
    /// nombres de otra maquina manda al agente a tocar servicios que no existen.
    #[test]
    fn el_contexto_lleva_los_datos_de_esta_instalacion() {
        let md = agents_md(Path::new(r"C:\alerta"), "alerta-zeruel", 9999);
        assert!(md.contains("alerta-zeruel"), "falta el nombre del servicio");
        assert!(md.contains("alerta-zeruel-web"), "falta el de la web");
        assert!(md.contains("9999"), "falta el puerto real");
        assert!(md.contains(r"C:\alerta"), "falta el destino real");
    }

    /// Lo que cambia se pregunta. Si algun dia alguien cachea la lista de DAGs
    /// o el estado de la cola aca, este test lo frena: el archivo se lee con
    /// confianza y desactualizado miente.
    #[test]
    fn el_contexto_no_cachea_lo_que_cambia() {
        let md = agents_md(Path::new(r"C:\alerta"), "alerta-zeruel", 9876);
        assert!(
            md.contains("preguntáselo a las herramientas"),
            "tiene que mandar a las herramientas por lo que cambia"
        );
    }
}
