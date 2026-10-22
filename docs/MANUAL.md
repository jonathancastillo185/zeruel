# Manual de zeruel

Orquestador de pipelines de datos. Un binario, sin dependencias externas, sin
base de datos que administrar.

Este manual va del modelo mental a la operación diaria. Si venís de Airflow,
empezá por «El modelo»: la diferencia grande está ahí y explica todo lo demás.

---

## 1. El modelo

**No hay carpeta `dags/`.** Esa es la diferencia con Airflow y es deliberada.

En Airflow dejás un `.py` en `dags/` y el scheduler lo importa dentro de su
propio proceso. En zeruel **un pipeline es un ejecutable**: se escribe en Rust,
se compila, y el binario resultante se copia a `pipelines/`.

```
C:\zeruel\                  (o /opt/zeruel en Linux)
  zeruel.exe                el motor: scheduler, panel y CLI en un archivo
  zeruel.toml               la configuración
  pipelines\                los DAGs, compilados
     ventas.exe
     reportes.exe           un binario puede traer muchos DAGs adentro
  state\
     zeruel.db              historial, cola y métricas (SQLite)
     logs\                  un archivo por corrida
```

### El contrato

El scheduler no sabe nada de tus pipelines: les pregunta.

| llamada | qué devuelve |
|---|---|
| `mi-pipeline --describe` | JSON con nombre, cron y grafo de tareas |
| `mi-pipeline run --dag <n> --param <p> --state <dir>` | ejecuta y escribe checkpoints |

En cada tick el scheduler escanea `pipelines/`, le pregunta `--describe` a cada
ejecutable y cachea la respuesta por fecha de modificación.

### Las tres consecuencias

**Un binario puede contener muchos DAGs.** Por eso los comandos que piden código
o grafo necesitan `--dag` además del binario: sin eso la pregunta es ambigua.

**Desplegar un DAG es copiar un archivo.** No hace falta reiniciar el scheduler:
aparece en el tick siguiente. Y actualizar un pipeline no toca a los demás.

**Un pipeline nunca corre dentro del scheduler.** Se lanza como subproceso. Por
eso un pipeline que revienta no se lleva puesto al scheduler, por eso el techo
de memoria es por corrida de verdad, y por eso la medición de RAM es exacta: el
proceso *es* la corrida.

### Arquitectura

```
  pipelines/                     zeruel serve  (scheduler)
    ventas.exe    ◄── lanza ──   · lee la cola
    reportes.exe                 · decide qué corre (prioridad, RAM, cupo)
         │                       · NO ejecuta nada en su proceso
         │                                   │
         └────────► state/zeruel.db ◄────────┘
                    (SQLite en modo WAL)      ▲
                                              │
                              zeruel web ─────┘  (panel, proceso aparte)
```

La base es el punto de encuentro entre procesos. Cancelar una corrida desde el
panel funciona porque la orden viaja por ahí, no por memoria compartida.

---

## 2. Instalación

### Windows

Un comando. Pide administrador una sola vez.

```powershell
zeruel.exe instalar --destino C:\zeruel --nombre zeruel --puerto 9876
```

Hace, verificando cada paso:

1. Crea `state/` y `pipelines/`.
2. Crea `zeruel.toml` si no existe. **Si existe, no lo toca.**
3. Copia el binario al destino.
4. Registra dos servicios: el scheduler y el panel.
5. Les concede permisos mínimos sobre `state`, `pipelines` y el config.
6. Agrega el destino al PATH de la máquina.
7. Escribe el contexto para agentes de IA (§7).
8. Arranca los servicios **y verifica que quedaron corriendo**.
9. Corre `doctor` sobre las herramientas de `[entorno]`.

Si algo falla, lo dice y termina con error. Nunca imprime «listo» con pasos
fallidos arriba.

Volver a correrlo actualiza: para los servicios esperando a las corridas en
curso, reemplaza el binario y vuelve a arrancar. Si ya está todo al día y sano,
no hace nada.

> **Dos servicios y no uno.** Reiniciar el panel no puede interrumpir un ETL de
> dos horas que está a la mitad.

### Linux

No hay servicios de Windows: el equivalente lo maneja systemd con la unidad de
`packaging/zeruel.service`.

### Desde el código

```bash
cargo build --release          # target/release/zeruel
zeruel self-test               # verifica el binario de punta a punta
```

---

## 3. Escribir un pipeline

Un pipeline es un crate propio que depende de `zeruel`.

```rust
use zeruel::prelude::*;

fn main() -> zeruel::Result<()> {
    let dag = dag!("ventas-diarias",
        "bajar"   => [],
        "limpiar" => ["bajar"],
        "cargar"  => ["limpiar"],
    );
    zeruel::pipeline(dag)
        .cron("0 3 * * *")
        .register(Bajar::new())
        .register(Limpiar::new())
        .register(Cargar::new())
        .start()          // acá falla si falta una implementación
}
```

`start()` provee `--describe` y `run` solo: no escribís CLI.

Compilás, copiás el `.exe` a `pipelines/`, y en el tick siguiente aparece en el
panel con su grafo y su horario.

### Qué va en Rust y qué en TOML

- **Rust**: el grafo, las transformaciones, los tipos. Es el producto.
- **TOML**: sólo lo que cambia entre una máquina y otra — rutas, conexiones,
  horarios, credenciales. **Nada de lógica.**

Una transformación no entra en un archivo de configuración. Meterla ahí obliga a
inventar un mini-lenguaje, que es donde las herramientas «Airflow con YAML» se
arruinan: se pierden el compilador, el autocompletado y la performance.

---

## 4. Configuración

Vive en `zeruel.toml`, al lado del binario.

```toml
[zeruel]
state      = "C:/zeruel/state"
pipelines  = "C:/zeruel/pipelines"
paralelo   = 6         # corridas a la vez
workers    = 4         # tareas en paralelo dentro de una corrida
ram_max    = "2500M"   # techo por corrida
timeout_secs = 10800   # corta una corrida que pase de 3 h
gracia_secs  = 60      # al apagar, cuánto espera a lo que está corriendo
retencion_detalle_dias = 30
retencion_runs_dias    = 90

[web]
bind = "127.0.0.1"
port = 9876

[entorno]
# Variables que reciben los pipelines. Acá y no en el entorno del sistema:
# ver la trampa de §8.
MI_PYTHON = "C:/proyecto/venv/Scripts/python.exe"
```

### Verlos y cambiarlos

```bash
zeruel config                        # todos los ajustes y sus valores
zeruel config set paralelo 10        # cambia uno
```

`config set` toca **sólo la línea del valor**: los comentarios del archivo son
documentación, y reserializar el TOML los borraría todos. Valida rangos antes de
escribir (`paralelo = 0` deja el scheduler sin ejecutar nada y parece un
cuelgue) y comprueba que el archivo siga siendo válido antes de guardarlo.

**Un cambio no tiene efecto hasta reiniciar.** La configuración se lee al
arrancar:

```powershell
Restart-Service zeruel     # necesita administrador
```

### La pestaña Configuración

El panel muestra dos columnas: **lo que dice el archivo** y **lo que el
scheduler está usando de verdad**. No siempre coinciden, y ver esa diferencia es
todo el punto de la pantalla — ver §8.

No se edita desde ahí: el servicio web tiene permiso de sólo lectura sobre su
configuración, a propósito. Un proceso expuesto por HTTP no debería poder
reescribir con qué arranca el scheduler.

---

## 5. Operación

### El panel

`http://localhost:9876`

| pestaña | para qué |
|---|---|
| Panel | estado de cada DAG ahora, con buscador y filtros |
| Corridas | historial con filtros, orden, paginado y línea de tiempo |
| Métricas | volumen, duraciones y RAM, con filtros de fecha y consumo |
| Alertas | fallos agrupados por problema, no por corrida |
| Cola y registros | qué está encolado y los logs |
| Configuración | archivo contra lo que corre (§4) |
| Sistema | RAM por proceso, disco, higiene de la base |

### Comandos

```bash
zeruel enqueue  --dag ventas --param 2026-09-05     # encolar una corrida
zeruel backfill --dag ventas --desde 2026-01-01 --hasta 2026-01-31
zeruel list                                          # corridas recientes
zeruel status --run latest                           # detalle de una
zeruel cola                                          # cuántos hay esperando
zeruel cola --max 500                                # tope de cola
zeruel sla --dag ventas --max-wait 3600              # alerta si espera de más
zeruel gc --dry-run                                  # qué purgaría
zeruel doctor                                        # probar las herramientas
```

Casi todos aceptan `--json`, pensado para scripts y para agentes.

### Limpieza

El scheduler purga solo, según `retencion_detalle_dias` y
`retencion_runs_dias`. El detalle por tarea y los logs son lo que más ocupa; el
listado de corridas pesa poco. Los **agregados mensuales sobreviven a la
purga**, así que las métricas históricas no se pierden al recortar la retención.

---

## 6. Diagnóstico

### «No ejecuta nada»

En este orden, porque es el que funcionó:

1. **¿Hay cupo?** Mirá la cola: ítems en estado `running` desde hace horas son
   cadáveres de una caída. Ocupan cupo y **nada los limpia solo**. Se cancelan
   desde el panel o con `POST /api/cola/<id>/cancelar`.
2. **¿Los servicios corren *y arrancan solos*?** Un servicio en `Running` con
   arranque `Disabled` funciona hoy y no vuelve después de un reinicio.
3. **¿El scheduler tiene su configuración?** Pestaña Configuración. Si dice que
   arrancó sin archivo, está con los valores de fábrica (§8).
4. **`zeruel doctor`** — si un intérprete no responde, las corridas fallan a
   nombre del DAG y no del problema real.
5. **`state/logs/run-<id>.log`** para el detalle de una corrida puntual.

### La cola crece y no baja

No siempre es un bug: puede ser aritmética. Si lo que agendaste demanda más
horas por día de las que tu paralelismo puede entregar, la cola crece para
siempre. El scheduler estima esto al arrancar y lo dice en el log.

Sirve mirar cuánto se lleva cada DAG: a veces un solo pipeline pesado y
frecuente explica la mitad de la carga.

---

## 7. Agentes de IA

`zeruel instalar` deja la carpeta lista para que un agente la entienda sin
configuración:

| archivo | qué hace |
|---|---|
| `.mcp.json` | **la detección**: un agente abierto en la carpeta recibe las herramientas |
| `AGENTS.md` | contexto: datos fijos y las trampas conocidas |
| `CLAUDE.md` | un puntero a `AGENTS.md` |

El servidor MCP expone once herramientas: `list_dags`, `trigger_run`,
`run_status`, `task_metrics`, `recursos`, `memoria_por_tarea`, `gc_status`,
`queue`, `backfill`, `sla`, `queue_set_max`.

Se regeneran en cada instalación, con los datos de esa instalación. **No
cachean lo que cambia**: la lista de DAGs y el estado de la cola salen de las
herramientas en el momento, porque un contexto desactualizado es peor que
ninguno — se lee con la misma confianza.

---

## 8. Trampas conocidas

Cada una salió de una caída real.

**Cambiar `zeruel.toml` no hace nada hasta reiniciar el scheduler.** La
configuración se lee al arrancar.

**Un servicio guarda sus argumentos al registrarse.** Cambiar `--config` obliga
a registrarlo de nuevo; no alcanza con reemplazar el binario. Una instalación
pasó dos días con la mitad del paralelismo porque el servicio se registró sin
`--config`: arrancó con los valores de fábrica, el archivo seguía diciendo otra
cosa, y desde afuera se veía idéntica a una instalación sana. La pestaña
Configuración existe para que eso se vea.

**Los servicios no ven variables de entorno nuevas hasta que reinicia la
máquina.** El gestor de servicios de Windows cachea el bloque de entorno al
arrancar. Por eso las variables de los pipelines van en `[entorno]` y no en el
entorno del sistema.

**Nunca borres un servicio que está corriendo.** Windows lo marca para
eliminación, queda a medio existir, y el siguiente intento de instalarlo falla
con el error 1072. Pará primero, esperá a que esté detenido, después borrá.

**`--destino C:zeruel` no es `C:\zeruel`.** Una ruta con unidad y sin barra es
relativa al directorio actual *de esa unidad*. `zeruel instalar` la rechaza,
porque resolverla en silencio crea una instalación anidada.

**No copies binarios a mano.** `zeruel instalar` sabe que en Windows un `.exe`
en uso no se puede sobreescribir pero sí renombrar, y lo aprovecha para
reemplazarlo sin parar la máquina.

---

## 9. Límites

Lo que zeruel **no** hace, a propósito:

- **No se autoactualiza desde internet.** Un orquestador que se baja código solo
  es una superficie de ataque y una fuente de sorpresas a las 3 de la mañana. Se
  le pasa el binario nuevo; él se encarga del reemplazo seguro.
- **No reescribe tu configuración.** Si un ajuste cambia de nombre, avisa al
  arrancar.
- **El panel no tiene HTTPS, y el token es opcional.** Lo que lo protege es el
  `bind`: `127.0.0.1` no sale de la máquina. Si hace falta verlo desde otra,
  usá un túnel o un proxy con TLS y además un `token` en `[web]`. Ver
  [SECURITY.md](../SECURITY.md).
