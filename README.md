# zeruel

**Orquestador de pipelines de datos escrito en Rust.** Una idea parecida a
Airflow, pero en un solo binario: sin Python, sin Postgres, sin Redis y sin
Docker obligatorio. El estado vive en un SQLite embebido y los datos se procesan
con [Polars](https://pola.rs) nativo.

> **Estado: experimental (0.1.0).** Funciona y está probado (fmt + clippy sin
> warnings + tests), pero la API todavía puede cambiar. Leé
> [Ventajas y desventajas](#ventajas-y-desventajas) antes de adoptarlo.

---

## Índice

- [Qué es](#qué-es)
- [Ventajas y desventajas](#ventajas-y-desventajas)
- [Inicio rápido](#inicio-rápido)
- [Usarlo como plantilla](#usarlo-como-plantilla)
- [Escribir un pipeline](#escribir-un-pipeline)
- [Cómo funciona](#cómo-funciona)
- [Comandos](#comandos)
- [Números](#números)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Estado y hoja de ruta](#estado-y-hoja-de-ruta)
- [Cómo se desarrolló](#cómo-se-desarrolló)
- [Contribuir](#contribuir)
- [Licencia](#licencia)

---

## Qué es

En zeruel **un pipeline es un ejecutable**. Se declara el grafo de tareas en
Rust, se compila, y el binario resultante se copia a la carpeta `pipelines/`. El
scheduler (`zeruel serve`) lo detecta en el siguiente tick, le pregunta qué DAGs
trae y a qué hora corren, y lo lanza como **subproceso** cuando corresponde.

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
        .start()
}
```

Lo que trae sin configurar nada:

- **Durabilidad por checkpoint.** Si el proceso muere a la mitad, la siguiente
  corrida del mismo DAG y parámetro saltea las tareas que ya terminaron bien.
- **Reintentos con backoff**, cron, backfill por rango de fechas y SLA de espera
  en cola.
- **Cola con prioridad y envejecimiento** (nada se queda esperando para
  siempre), control de admisión por la RAM histórica de cada DAG y tope de
  memoria por corrida (Windows).
- **Métricas por tarea**: duración, pico de RAM, CPU y filas, con agregados
  mensuales que sobreviven a la limpieza.
- **Cancelación en vivo** desde el panel, incluso entre procesos. Mata el árbol
  de procesos de la tarea, y una tarea WASM se corta aunque esté en un bucle
  infinito.
- **Panel web** embebido en el binario: corridas, grafo, métricas, alertas,
  cola, registros y configuración.
- **Servidor MCP** para que un agente de IA consulte el estado, encole
  corridas y decida si hay recursos antes de lanzar algo.
- **Higiene por defecto**: retención de 30 días para el detalle y limpieza
  automática diaria; la base no crece sin límite.
- **Conectores** opcionales, que se activan por *feature*: shell, HTTP,
  Postgres (vía `COPY`), Snowflake (API REST), S3/Azure/GCS, SQL sobre Parquet
  y tareas WASM en sandbox.

---

## Ventajas y desventajas

### Ventajas

| | |
|---|---|
| **Liviano en reposo** | Scheduler + panel ocupan **~49 MB de RAM** sin nada corriendo. Un Airflow mínimo (scheduler, webserver, worker, Postgres, Redis) arranca en el orden de 1-2 GB. |
| **Un solo binario** | Se copia y anda. No hay venv, `pip install` ni una base de datos que administrar. |
| **Aislamiento real** | Cada corrida es un proceso aparte: un pipeline que revienta no tumba al scheduler ni a los demás, y el pico de RAM que se mide es exactamente el de esa corrida. |
| **Desplegar = copiar un archivo** | Sumar o actualizar un pipeline no obliga a reiniciar nada ni a recompilar los otros. |
| **Rápido con datos medianos y grandes** | Polars nativo con streaming: 10 M de filas en ~6,5 s con un pico de ~830 MB en el pipeline de ejemplo. |
| **Tipado** | El grafo y las transformaciones los revisa el compilador. Una dependencia con typo o una tarea sin implementar falla al arrancar, no a las 3 de la mañana. |
| **Pipelines chicos** | Un pipeline sin Polars pesa ~3 MB, y se pueden agrupar muchos DAGs en un binario (*bundles*). |

### Desventajas (leelas)

| | |
|---|---|
| **Hay que escribir Rust** | Si tu equipo trabaja en Python, esto es una barrera real. Las tareas *shell* permiten orquestar scripts existentes de cualquier lenguaje, pero el grafo se declara en Rust. |
| **Compilar es lento** | La primera compilación de un pipeline con Polars tarda varios minutos y `target/` ocupa varios GB. Sin Polars, ~80 s. |
| **Un solo nodo** | No hay ejecución distribuida en varias máquinas. La escala es vertical. |
| **Ecosistema chico** | Airflow tiene cientos de *operators* y una comunidad enorme; acá hay un puñado de conectores y un solo autor. |
| **Probado sobre todo en Windows** | Se desarrolló y se usa en Windows 11. Linux y macOS deberían compilar (hay Dockerfile y unidad systemd), pero falta que el CI lo confirme. |
| **Techo de RAM solo en Windows** | El límite duro de memoria usa Job Objects. En Linux falta implementarlo con cgroups. |
| **Panel sin TLS** | El panel escucha en `127.0.0.1` por defecto. Hay un token opcional, pero no hay HTTPS: para exponerlo a la red, ponelo detrás de un proxy con TLS. Ver [SECURITY.md](SECURITY.md). |
| **Postgres sin TLS** | Sirve para un Postgres local o en la misma red, no para uno administrado en la nube que exija TLS. |
| **API inestable** | Versión 0.1: puede cambiar sin aviso entre versiones menores. |

### ¿Cuándo conviene?

- **Sí**: una máquina (VPS, servidor de oficina, PC) que tiene que correr
  muchas automatizaciones programadas (ETL, reportes, descargas, cargas a una
  base, scripts de Python o de lo que sea) con poco hardware, donde Airflow es
  demasiado pesado y un `cron` + scripts se quedó corto (sin reintentos, sin
  historial, sin saber qué falló).
- **Sí**: procesar volúmenes que no entran en pandas, con un binario que se
  lleva a cualquier lado.
- **No**: si necesitás un clúster, cientos de integraciones listas o un equipo
  que solo escribe Python. Ahí Airflow, Dagster o Prefect son mejor opción.

---

## Inicio rápido

### Requisitos

- [Rust](https://rustup.rs) estable (edición 2021).
- Windows 10/11 es la plataforma principal. Linux y macOS: ver la tabla de arriba.
- ~10 GB libres en disco para la compilación con todas las *features*.

### Compilar y probar

```bash
git clone <url-del-repo> zeruel
cd zeruel
cargo build --release                 # 6 a 12 minutos la primera vez
./target/release/zeruel self-test     # valida el binario de punta a punta
```

### Correr el pipeline de ejemplo

El binario trae un ETL de e-commerce de ejemplo (validar → limpiar → joins →
agregados → CSV) y un generador de datos sintéticos reproducible:

```bash
zeruel gen  --root data --n 1000000   # 1 M de transacciones (seed fija)
zeruel run  --root data --state state # corre el pipeline
zeruel list --state state             # corridas recientes
zeruel status --state state           # detalle de la última, por tarea
```

Corré `zeruel run` de nuevo con el mismo parámetro: termina en milisegundos
porque la durabilidad saltea lo que ya está hecho.

### Panel web y scheduler

```bash
zeruel serve --state state --pipelines pipelines   # el scheduler
zeruel web   --state state --pipelines pipelines   # el panel: http://127.0.0.1:8787
```

`zeruel web` no ejecuta nada por sí mismo: encola. Las corridas las lanza
`zeruel serve`.

### Instalarlo como servicio (Windows)

```powershell
zeruel.exe instalar --destino C:\zeruel --nombre zeruel --puerto 9876
```

Crea la estructura, registra el scheduler y el panel como servicios, verifica
que hayan arrancado y deja el contexto para agentes de IA. En Linux se usa la
unidad de [packaging/zeruel.service](packaging/zeruel.service) o la imagen de
[packaging/Dockerfile](packaging/Dockerfile).

**La guía completa está en el [manual](docs/MANUAL.md)** (versión ilustrada y
autocontenida en [docs/MANUAL.html](docs/MANUAL.html)).

---

## Usarlo como plantilla

La idea es que armes **tus propias automatizaciones** sobre zeruel sin tocar
el motor. El camino corto:

1. **Copiá el ejemplo** [examples/pipeline-ventas](examples/pipeline-ventas) a
   una carpeta tuya (fuera de este repo) y cambiá el nombre en su `Cargo.toml`.
2. **Apuntá la dependencia** a zeruel: mientras no esté publicado en
   crates.io, con `zeruel = { git = "<url-de-este-repo>" }` o con un `path` a
   tu copia local.
3. **Declará tu grafo** con `dag!` y escribí una tarea por paso. Si ya tenés
   scripts (Python, Node, PowerShell, `dbt`…), no hace falta reescribirlos:
   activá la feature `shell` y envolvelos con `ShellTask`. Ganan reintentos,
   historial, métricas y cancelación sin cambiar una línea.
4. **Fijá el horario** con `.cron("0 3 * * *")` y probalo suelto:
   `cargo run -- run --param 2026-01-01 --state ./state`.
5. **Desplegalo**: `cargo build --release` y copiá el binario a la carpeta
   `pipelines/` de tu instalación. El scheduler lo toma en el siguiente tick,
   sin reiniciar nada.

Para muchas automatizaciones parecidas (una por cliente, por sucursal, por
fuente de datos), mirá [examples/bundle-demo](examples/bundle-demo): genera
varios DAGs en un bucle y los empaqueta en un solo binario.

Todo lo que cambia de una máquina a otra (rutas, credenciales, conexiones) va
en `zeruel.toml` o en variables de entorno, nunca en el código: así el mismo
pipeline sirve en tu PC y en el servidor.

---

## Escribir un pipeline

Un pipeline es un crate propio que depende de `zeruel`. El ejemplo completo,
listo para copiar, está en [examples/pipeline-ventas](examples/pipeline-ventas):

```bash
cd examples/pipeline-ventas
cargo run -- run --param 2026-08-19 --state ./state   # correrlo suelto
cargo run -- --describe                               # el grafo, como lo ve el scheduler
```

Para ponerlo a correr solo, se compila en release y se copia el binario a la
carpeta `pipelines/` del scheduler.

**Qué va en Rust y qué en TOML.** En Rust: el grafo, las transformaciones y
los tipos. En `zeruel.toml`: solo lo que cambia de una máquina a otra (rutas,
conexiones, horarios, credenciales). La lógica nunca va en la configuración.

*Features* del crate `zeruel` (por defecto no trae ninguna, para que un
pipeline chico siga siendo chico):

| Feature | Qué agrega |
|---|---|
| `shell` | `ShellTask`: correr comandos externos como tareas del grafo |
| `http` | `HttpTask`: bajar de una API o URL |
| `polars` | Tareas de datos listas (`PolarsTask`, `JoinTask`, `ExportCsvTask`) |
| `sql` | Consultas SQL sobre Parquet (polars-sql) |
| `wasm` | Tareas WASM en sandbox con límites de *fuel* y memoria |
| `postgres` | Postgres como origen y destino vía `COPY` |
| `snowflake` | Carga a Snowflake por su API REST |
| `cloud` | Leer y escribir en S3 / MinIO / Azure / GCS |
| `full` | Todo lo anterior |

Más ejemplos en [examples/](examples): `bundle-demo` (muchos DAGs en un
binario), `pipeline-lento` (para probar cancelación y timeouts), `wasm-hola` y
`wasm-bucle` (tareas WASM).

---

## Cómo funciona

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

- **El contrato.** El scheduler no conoce los pipelines: les pregunta.
  `mi-pipeline --describe` devuelve un JSON con nombre, cron y grafo;
  `mi-pipeline run --dag <n> --param <p> --state <dir>` ejecuta y escribe los
  checkpoints.
- **La base es el punto de encuentro.** Scheduler, pipelines y panel son
  procesos distintos que se comunican por SQLite en modo WAL. Por eso cancelar
  desde el panel corta una corrida que está en otro proceso.
- **Dentro de una corrida** las tareas listas se lanzan en tandas, hasta
  `workers` en paralelo, sobre datos Arrow/Parquet.

El detalle de cada módulo está en el [manual](docs/MANUAL.html) y en
[AGENTS.md](AGENTS.md).

---

## Comandos

| Comando | Para qué |
|---|---|
| `zeruel serve` | El scheduler: cola, cron, GC diaria |
| `zeruel web` | El panel web |
| `zeruel mcp` | Servidor MCP por stdio, para agentes de IA |
| `zeruel run` / `gen` / `puro` | El pipeline de ejemplo, su generador de datos y su versión sin orquestación (benchmark) |
| `zeruel enqueue` / `backfill` | Encolar una corrida o un rango de fechas |
| `zeruel list` / `status` | Historial y detalle de corridas |
| `zeruel cola` / `sla` | Tope de la cola y espera máxima por DAG |
| `zeruel gc` | Limpieza por retención (`--dry-run` para ver qué borraría) |
| `zeruel config` | Ver y cambiar `zeruel.toml` sin perder los comentarios |
| `zeruel doctor` | Probar que las herramientas que usan los pipelines respondan |
| `zeruel instalar` / `servicio` | Instalación y servicios de Windows |
| `zeruel deploy` | Carpeta portable (binario + wrappers + docs, opcional zip/Docker) |
| `zeruel self-test` | Verificar el binario de punta a punta |
| `zeruel wasm-run` | Ejecutar un módulo WASM en el sandbox |

`--json` es un flag global (`zeruel --json status ...`), pensado para scripts y
agentes. `zeruel <comando> --help` muestra todas las opciones.

---

## Números

Medidos en Windows 11, binarios release. El detalle y cómo re-medirlos está en
[docs/COSTOS.md](docs/COSTOS.md).

| | |
|---|---|
| RAM en reposo (scheduler + panel) | **49 MB** |
| Binario completo (Polars + wasmtime + SQLite) | 97 MB |
| Un pipeline sin Polars | 3 MB |
| Arranque de un proceso de pipeline | ~170 ms |
| ETL de ejemplo, 10 M de filas | **6,5 s**, pico de 833 MB (mediana de 3) |

> **Sobre la comparación con Python.** Existe un benchmark contra pandas y
> polars-python ([bench/](bench)), pero **todavía no es una comparación justa**:
> los números de Python se tomaron dentro de un contenedor Linux y los de
> zeruel en Windows nativo, midiendo la RAM de formas distintas. Hasta re-medir
> en igualdad de condiciones, no lo cites como prueba de nada. Re-medirlo bien es
> una de las [tareas abiertas](#estado-y-hoja-de-ruta) para quien quiera ayudar.

---

## Estructura del repositorio

```
zeruel/
├── crates/
│   ├── zeruel/          # la librería que usa un pipeline (`cargo add zeruel`)
│   ├── zeruel-sdk/      # API pública: Task, Dag, macro dag!
│   ├── zeruel-core/     # store SQLite, engine, métricas, WASM, SQL, límites
│   ├── zeruel-data/     # tareas Polars (Parquet, joins, export)
│   ├── zeruel-tasks/    # conectores: shell, http, postgres, snowflake
│   └── zeruel-cli/      # el binario `zeruel`: scheduler, panel, MCP, instalador
├── examples/            # pipelines de ejemplo (crates independientes)
├── docs/                # manual, roadmap, costos, historial, irregularidades
├── bench/               # scripts del benchmark contra Python
├── packaging/           # Dockerfile y unidad systemd
├── scripts/             # utilidades (fuentes empotradas del panel)
├── check.ps1            # fmt + clippy + tests: correlo antes de cada commit
└── build_releases.ps1   # empaquetado de releases (manual)
```

---

## Estado y hoja de ruta

Lo que está hecho y lo que falta se sigue en tres documentos:

- [docs/ROADMAP.md](docs/ROADMAP.md): hacia dónde va.
- [docs/IRREGULARIDADES.md](docs/IRREGULARIDADES.md): todo lo que se encontró
  mal, con prioridad y cómo se verificó cada arreglo.
- [CHANGELOG.md](CHANGELOG.md): cambios por versión.

**Abierto, y buen lugar para empezar a contribuir:**

1. **Benchmark justo contra Python**: mismo sistema, misma forma de medir RAM,
   mediana de varias corridas. Ver [bench/README.md](bench/README.md).
2. **CI en Linux y macOS**: el workflow existe, pero falta dejar los tests en
   verde en las tres plataformas.
3. **Techo de RAM en Linux** con cgroups v2 (`RLIMIT_AS` no sirve porque Polars
   mapea archivos grandes).
4. **TLS para Postgres** (`postgres-native-tls` o `rustls`).
5. **Lectura masiva de Snowflake** sin romper el "un solo binario".
6. **Empaquetado**: binarios musl estáticos, releases automáticos, servicio
   systemd probado.
7. **Panel**: HTTPS propio o guía de proxy inverso, y token obligatorio al
   escuchar fuera de `127.0.0.1`.

---

## Cómo se desarrolló

zeruel empezó en agosto de 2026 como **`nodo`**, un benchmark para responder
una pregunta concreta: ¿cuánto se gana orquestando un ETL en Rust en lugar de
hacerlo con Airflow y Python, en una máquina modesta?

El desarrollo avanzó por fases, todas registradas en
[docs/HISTORIAL.md](docs/HISTORIAL.md) con qué se decidió, por qué y qué número
dio cada cambio:

1. **Fundación y MVP**: SDK para declarar DAGs, store en SQLite, engine con
   checkpoints y el pipeline de e-commerce con un dataset sintético reproducible.
2. **Benchmark** contra pandas y polars-python, y el salto de Polars 0.47 a
   0.55.2, que bajó el tiempo de 14,8 s a 6,2 s.
3. **Operación**: scheduler con cola y prioridad, cron, reintentos, backfill,
   SLA, higiene automática, panel web, MCP y sandbox WASM.
4. **Auditoría**: una revisión completa encontró 53 irregularidades (docs que
   no coincidían con el código, warnings que figuraban como resueltos, números
   desactualizados). Se corrigieron 51 y la lista quedó como documento vivo.
5. **De benchmark a producto** ([docs/ROADMAP.md](docs/ROADMAP.md)): un binario
   por pipeline, *features* para no pagar lo que no se usa, conectores, techo
   de memoria, configuración TOML, instalador y servicios. Ahí se renombró a
   **zeruel**.
6. **Uso continuo**: correrlo todos los días con decenas de DAGs sacó a la
   luz los problemas que un benchmark no muestra (configuración que no se
   aplicaba en silencio, permisos de servicios, procesos huérfanos). Las
   lecciones quedaron en la sección "Trampas conocidas" del
   [manual](docs/MANUAL.md) y explican buena parte del diseño actual.

Dos reglas guiaron todo el proceso y siguen vigentes: **nada se marca como
resuelto sin correr el comando que lo prueba**, y **cada número publicado tiene
que poder re-medirse** con lo que hay en el repo.

---

## Contribuir

¡Toda ayuda suma! Issues, documentación, tests, benchmarks o código.

1. Leé [CONTRIBUTING.md](CONTRIBUTING.md): cómo compilar, probar y mandar un PR.
2. Mirá la lista de [tareas abiertas](#estado-y-hoja-de-ruta) o los issues.
3. Si vas a trabajar con un agente de IA, [AGENTS.md](AGENTS.md) tiene el
   contexto que necesita.

Este proyecto sigue un [código de conducta](CODE_OF_CONDUCT.md). Para reportar
una vulnerabilidad, **no abras un issue público**: seguí
[SECURITY.md](SECURITY.md).

---

## Licencia

[MIT](LICENSE) © 2026 Jonathan Castillo.
