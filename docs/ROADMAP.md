# ROADMAP — de benchmark a motor de ETL embebible

> Decidido el 2026-08-18, después de la ronda de correcciones que cerró 51 de las
> 53 irregularidades relevadas. Estado y progreso: el `README.md`. Historia:
> `HISTORIAL.md`.

## Qué es esto y por qué

Hoy `zeruel` es un **benchmark que funciona**: el motor está bien y probado (84
tests, durabilidad por checkpoint, cancelación entre procesos, cola con prioridad
y admission control). Pero no se puede usar desde afuera, no sabe conectarse a
nada, y todos los DAGs viven dentro de un solo binario.

El objetivo es que **cada pipeline ETL sea un crate propio** que hace
`cargo add zeruel`, descargue de distintas conexiones, limpie y cargue, procesando
volúmenes muy superiores a la RAM en un VPS de 2-4 GB — y que sumar, cambiar o
romper un pipeline **no afecte a los demás**.

### Los bloqueadores, medidos

| Hallazgo | Evidencia | Consecuencia |
|---|---|---|
| Los DAGs se compilan adentro del binario | `dags.rs`: `build: fn(&DataEnv) -> Pipeline` | Sumar un pipeline obliga a reconstruir todo y reiniciar el daemon: los demás se pausan |
| No hay API de librería | `zeruel-core` expone 64 items `pub`; 28 funciones devuelven `anyhow::Result` | Imposible de versionar; le impone `anyhow` a quien la use |
| Todo se compila siempre | 389 deps (polars 155, wasmtime 31). Cero `[features]` | 96 MB de binario, ~5 min de build, `target/` de decenas de GB |
| Solo lee y escribe Parquet local | `DataEnv` es una carpeta base | No hay Postgres, ni S3, ni Snowflake, ni APIs |
| `busy_timeout` sin configurar | `store.rs` pone WAL pero no el timeout | Con varios procesos escribiendo, SQLite devuelve "database is locked" en vez de esperar |

---

## La arquitectura: procesos independientes, no un monolito

Un solo binario con todos los DAGs adentro los acopla: recompilás todo para sumar
uno, reiniciás el daemon para desplegarlo, y un panic en cualquiera se lleva
puestos a los demás.

```
  pipelines/                      zeruel serve (scheduler)
    ventas.exe      ◄── lanza ──  · lee la cola
    scraping.exe                  · decide qué corre (prioridad, RAM)
    fichap.exe                    · NO ejecuta nada en su proceso
         │                                    │
         └────────► state/zeruel.db ◄───────────┘
                    (SQLite-WAL: la coordinación)
```

- **`zeruel` es la librería.** Cada pipeline —o grupo de pipelines— es su propio
  binario que depende de ella.
- **`zeruel serve` no ejecuta pipelines: los lanza como subprocesos.**
- **La coordinación es el store SQLite**, que ya funciona entre procesos: está
  probado cancelando desde `zeruel web` un run que corría en `zeruel serve`.
- **Contrato del binario de pipeline** (lo provee la librería; el autor no
  escribe CLI):
  - `mi-etl --describe` → JSON con nombre, cron y grafo. Alimenta al scheduler y a la UI.
  - `mi-etl run --param X --state <dir>` → ejecuta y escribe checkpoints.
- **Descubrimiento**: un directorio `pipelines/`, re-escaneado en cada tick.
  Sumar un pipeline = compilar **solo ese crate** y copiar el binario.

**Lo que se gana:** independencia real (un panic mata un pipeline, no el
scheduler); deploy y rollback por archivo; cada pipeline fija su propia versión de
zeruel y polars; las métricas de RAM pasan a ser exactas (el sampler mide el
proceso, y ahora el proceso *es* el run, con lo que la limitación de fondo de N-10
desaparece sola); y N-22 se simplifica, porque correr varios runs a la vez es
lanzar N subprocesos.

**Lo que cuesta:** 170 ms de arranque por run (medido, release) — irrelevante para
un ETL de minutos; y cada binario embebe su copia de polars (~90 MB), que se
mitiga eligiendo la granularidad (un binario puede tener varios DAGs de un mismo
dominio) y con el feature split de la Fase 1.

---

## Qué va en Rust y qué en TOML

Una transformación no entra en un archivo de configuración. `PolarsTask` recibe
`f: impl Fn(LazyFrame) -> Result<LazyFrame>`; meter eso en TOML obliga a inventar
un mini-lenguaje, que es donde las herramientas tipo "Airflow con YAML" se
arruinan: se pierde el compilador, el autocompletado y la performance.

- **Rust**: el grafo, las transformaciones, los tipos. Es el producto.
- **TOML**: solo lo que cambia entre dev y prod — conexiones, rutas, horarios,
  credenciales. Nada de lógica.

---

## Fase 1 — La librería y el contrato de pipeline

Nadie hace `cargo add` de algo que arrastra 389 crates: el objetivo elegido y el
problema de tamaño son el mismo trabajo.

- **1.0 Medir primero** (cierra N-21 y N-19). Por combinación de features: deps,
  tamaño del binario, tiempo de build limpio, peso de `target/`. Y re-correr el
  benchmark ahora que el pipeline usa streaming real (N-18).
- **1.1 Layout de crates**

  ```
  zeruel-sdk      tipos y traits. Sin deps pesadas (ya está: 4 deps)
  zeruel-core     Engine + Store. default = SIN polars y SIN wasmtime
                features: `sql` (polars) · `wasm` (wasmtime)
  zeruel-connect  NUEVO: fuentes y destinos (Fase 3), un feature por conector
  zeruel-data     transformaciones polars (queda igual, pasa a opcional)
  zeruel          NUEVO: fachada — el `cargo add zeruel`, con el harness que le da
                a un pipeline su `--describe` y su `run`
  zeruel-cli      el binario del scheduler: serve + web + mcp + gc
  ```

- **1.2 Errores tipados.** `NodoError` con `thiserror` en la superficie pública
  (el patrón ya está en `DagError`). Para `Task::run`, un `TaskError` con
  `From<E: Error>` para que quien escriba una tarea siga usando `?` sin agregar
  `anyhow` a su crate.
- **1.3 Una API que no se pueda usar mal.** Hoy `engine.run(&dag, &tasks_hashmap,
  param, workers)` obliga al caller a armar un `HashMap` cuyas claves tienen que
  coincidir con los ids del `Dag`, y si no coinciden falla en runtime. Va un
  builder que valida al construir, más el harness del binario:

  ```rust
  fn main() -> zeruel::Result<()> {
      let dag = dag!("ventas-diarias",
          "bajar" => [], "limpiar" => ["bajar"], "cargar" => ["limpiar"],
      );
      zeruel::pipeline(dag)
          .register(Bajar::new())
          .register(Limpiar::new())
          .register(Cargar::new())
          .start()
  }
  ```

  Además `TaskContext::cancelled() -> bool` en vez de exponer el
  `Arc<AtomicBool>`, y bajar los 64 items públicos de `zeruel-core` al mínimo.
- **1.4 Presupuesto de tamaño chequeado** en `check.ps1`.

**Entregable:** un crate de ejemplo, fuera del workspace, que hace `cargo add
zeruel`, declara un ETL de 3 tareas y corre solo.

## Fase 2 — Scheduler multiproceso

Acá aparece la independencia entre pipelines.

- `busy_timeout` en `store.rs` (N-57). Prerrequisito de todo lo demás.
- `scheduler.rs` deja de llamar a `dags::run_named` y **lanza el binario** del
  pipeline como subproceso, con N a la vez (cierra N-22).
- Descubrimiento por directorio + caché de `--describe` por mtime.
- Supervisión: exit code, stdout/stderr al log del run, timeout, y matar el
  subproceso al cancelar (la señal ya viaja por la DB).

## Fase 3 — Conectores

Un trait `Source`/`Sink` que produzca y consuma **`LazyFrame` en streaming**,
nunca `DataFrame` materializado. Cada conector es un feature.

| Conexión | Camino | Esfuerzo |
|---|---|---|
| Parquet/CSV/JSON local | Ya existe (`zeruel-data`) | — |
| S3 / MinIO / Azure / GCS | **No se escribe conector**: polars 0.55.2 ya trae `cloud`, `aws`, `azure`, `gcp`. `scan_parquet("s3://...")` lee con streaming nativo. Solo cablear credenciales. | Bajo |
| HTTP / REST | Feature `http` de polars; `HttpTask` con `reqwest` para APIs paginadas. | Bajo |
| PostgreSQL / MySQL | `sqlx` o `postgres` leyendo por chunks hacia Arrow. Los drivers buenos son async y el engine es sync: decidirlo de entrada. | Medio |
| Snowflake | Sin driver oficial de Rust. Ver abajo. | Alto |

**Snowflake:** para **cargar** (el caso común), staging en S3 + `COPY INTO` por su
API REST — es lo que Snowflake recomienda para volumen, reusa el conector S3 y no
agrega dependencias nativas. Por acá se empieza. Para **lectura masiva**, ADBC
devuelve Arrow nativo (cero copias hacia polars) pero es una librería nativa por
FFI y rompe "un solo binario": solo si resulta un cuello real.

## Fase 4 — Big data en 2-4 GB

- Streaming punta a punta: que ningún camino materialice el resultado.
- Techo de RAM por run, con spill. Hoy el admission control decide si un run
  *entra*; falta que un run *se contenga* mientras corre. Con un proceso por run,
  el techo se puede aplicar desde afuera (job objects en Windows, cgroups en Linux).
- **Objetivo medible**: 10M+ de filas con menos de 2 GB de pico, publicado y
  reproducible desde `bench/`. Cierra N-19.

## Fase 5 — Operación · CERRADA (2026-08-22)

- **Config TOML + variables de entorno.** Precedencia flag > entorno > toml >
  default, con las claves desconocidas fallando por nombre en vez de ignorarse.
- **Auth en la UI web.** Token; sin él, `zeruel web` escucha solo en `127.0.0.1`
  y no lo pide.
- **Empaquetado.** `packaging/` con unidad de systemd, Dockerfile multi-etapa y
  servicio nativo de Windows (`zeruel servicio instalar`). El README documenta las
  tres, con el número que las atraviesa: el plazo del supervisor tiene que ser
  mayor que `--gracia-secs`.

Dos cosas aparecieron al empaquetar y hubo que resolverlas antes:

- **No había ningún manejo de señales** (N-58). Sin eso, un `systemctl stop`
  mataba el proceso y dejaba los pipelines hijos huérfanos escribiendo en la
  base. Ahora el apagado es ordenado y lo que se corta vuelve a la cola.
- **El techo duro de memoria solo existía en Windows** (N-59). En Linux —el
  sistema donde esto se despliega— la garantía dura no existía. Resuelto con
  `setrlimit(RLIMIT_DATA)`.

Queda pendiente de esta fase una sola cosa, y es opcional: **musl estático**.
El binario actual necesita glibc; para una imagen `scratch` o un servidor sin
glibc compatible habría que cruzar a musl, y polars no compila ahí sin trabajo.
El contenedor de `packaging/` usa `debian-slim`, que resuelve el caso real sin
esa pelea.

---

## Riesgos

- **El nombre `zeruel` en crates.io puede estar tomado.** Chequear antes de
  comprometerse con la fachada.
- **Async vs sync en los drivers de base.** Decidirlo al empezar la Fase 3;
  arrastrar un runtime async a mitad de camino sería el peor resultado.
- **N binarios de ~90 MB**: es el precio de la independencia entre pipelines.
- **La Fase 1 es refactor sin función nueva visible.** Por eso lleva el pipeline
  de ejemplo y los números como entregable.
- **Partir polars de `zeruel-core` puede destapar acoplamientos** (`sql.rs` y
  `metrics.rs` conviven ahí). Si aparece, se mueve `sql` a su propio crate.
