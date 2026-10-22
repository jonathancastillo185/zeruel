# AGENTS.md — zeruel

Contexto para agentes de IA (y para personas nuevas en el repo): qué es el
proyecto, cómo se compila, dónde está cada cosa y qué invariantes no hay que
romper. Para usarlo como usuario, ver el [manual](docs/MANUAL.md).

| Documento | Para qué |
|---|---|
| [README.md](README.md) | Qué es, ventajas y desventajas, inicio rápido |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Cómo preparar el entorno y mandar un cambio |
| [docs/MANUAL.md](docs/MANUAL.md) / [MANUAL.html](docs/MANUAL.html) | El manual. Si difieren, gana el HTML |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Hacia dónde va el proyecto |
| [docs/IRREGULARIDADES.md](docs/IRREGULARIDADES.md) | Lo que se encontró mal, con prioridad, arreglo y verificación |
| [docs/HISTORIAL.md](docs/HISTORIAL.md) | Decisiones tomadas y por qué. Consultalo antes de rediscutir algo |
| [docs/COSTOS.md](docs/COSTOS.md) | RAM, tamaño y tiempos medidos, con cómo re-medirlos |

> El proyecto se llamó `nodo` hasta mediados de 2026. En HISTORIAL e
> IRREGULARIDADES vas a ver ese nombre y rutas viejas
> (`nodo-core/...`, `zeruel/crates/...`): son historia. Hoy todo es `zeruel` y
> el workspace está en la raíz.

## Qué es

Orquestador de pipelines de datos en Rust. Un pipeline es un **ejecutable**
que usa la librería `zeruel`; el scheduler (`zeruel serve`) los descubre en
`pipelines/`, les pregunta su grafo con `--describe` y los lanza como
subprocesos. El estado vive en SQLite (WAL) y los datos se procesan con Polars.

## Workspace

```
crates/
  zeruel/         fachada pública: pipeline(), bundle(), prelude, features
    src/bundle.rs   varios pipelines en un binario (contrato 2)
  zeruel-sdk/     Task, Dag, TaskDef, TaskState, macro dag!
  zeruel-core/
    store.rs        SQLite-WAL: runs, task_runs, queue, rollups_monthly, meta;
                    migraciones, UPSERT, GC, cola + pick_next
    engine.rs       ejecución del DAG: checkpoints, retries, resume, cancelación
    corridas.rs     consultas de historial con filtros (panel)
    metrics.rs      sampler de RAM/CPU del proceso (sysinfo)
    metricas.rs     consultas de la pestaña Métricas
    pico_os.rs      pico de memoria del proceso y sus descendientes, según el SO
    limite.rs       techo duro de memoria (Job Objects, solo Windows)
    wasm.rs         WasmTask: sandbox wasmtime con fuel, memoria y epoch
    sql.rs          SqlTask: polars-sql sobre Parquet
  zeruel-data/    PolarsTask, JoinTask, ExportCsvTask, chequeos de datos
  zeruel-tasks/   ShellTask (mata el árbol de procesos), http, pg, snowflake
  zeruel-cli/     el binario `zeruel`
    main.rs         CLI (clap) + --json global
    scheduler.rs    `serve`: cola, cron, GC diaria, apagado ordenado
    pipelines.rs    descubrimiento de binarios y caché de --describe
    web.rs + ui.html  panel (tiny_http; la UI se empotra con include_str!)
    mcp.rs          servidor MCP por stdio
    ops.rs          backfill / SLA / tope de cola: una implementación para CLI y MCP
    config.rs       zeruel.toml: precedencia flag > entorno > archivo
    configurar.rs   `config set` sin perder comentarios
    instalador.rs, servicio.rs   instalación y servicios de Windows
    dependencias.rs `doctor`
    init.rs         `init`: crea el zeruel.toml de una instalación nueva
    capacidad.rs    ¿alcanza el paralelismo para lo agendado? (aviso al arrancar)
    salud.rs        lo que consume zeruel en reposo (pestaña Sistema)
    ia.rs           genera .mcp.json / AGENTS.md / CLAUDE.md en una instalación
    deploy.rs       `self-test` y `deploy`
    cron.rs         parser cron propio (5 campos, UTC)
    pipeline.rs, dags.rs, dataset.rs, puro.rs   ETL de ejemplo y benchmark
examples/         crates independientes (no son miembros del workspace)
```

Las dependencias se declaran **solo** en el `Cargo.toml` de la raíz; los crates
usan `{ workspace = true }`.

## Comandos

```bash
cargo check --workspace                                # rápido: usalo primero
cargo test --workspace                                 # todos los tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
cargo build --release                                  # 6 a 12 minutos
```

En Windows, `.\check.ps1` corre fmt + clippy + tests de una vez (es lo mismo
que el CI). **Corré siempre todo el workspace**, no solo el crate que tocaste.

## Invariantes (no romper)

1. **Integridad.** Con seed fija, el pipeline de ejemplo da siempre las mismas
   filas (validar descarta ~1,5 % a propósito: NULLs y montos negativos).
   Repetir DAG+param saltea lo ya exitoso y el run reporta `skipped`; las filas
   reutilizadas quedan marcadas y fuera de las estadísticas.
2. **Higiene por defecto.** Retención de 30 días para task_runs, agregados
   mensuales en `rollups_monthly` que nunca se borran, GC diaria dentro de
   `serve` (la marca vive en `meta:last_gc`). Logs rotados por día.
3. **`--fresh` borra todo `state/`**, logs incluidos.
4. **Scheduler.** Prioridad efectiva = priority + aging (1 punto cada 300 s),
   conflicto por DAG+param, admisión por pico de RAM histórico. `--workers` es
   paralelismo **dentro** de una corrida.
5. **Polars.** El pipeline sinkea el LazyFrame directo, sin `collect()` previo:
   el pico de RAM no depende del tamaño del resultado. Las filas se cuentan
   después, desde los metadatos del Parquet. `PlRefPath` no acepta `PathBuf`:
   usar `p.to_string_lossy().as_ref().into()`. `polars::prelude::*` trae un
   trait `PolarsContext` que choca con `anyhow::Context`: usar
   `anyhow::Context::context(...)` con camino completo.
6. **Cron.** En cron el día de la semana 0 es domingo; chrono da
   `number_from_monday()` = 1..7, así que se usa `% 7`. El anti-doble-disparo
   usa la clave `cron:<dag>` en `meta`.
7. **`Store::meta_get` / `meta_set`** es el KV para estado duradero
   (`last_gc`, `cron:<dag>`, `sla:<dag>`, `queue.max_size`).
8. **Cancelación entre procesos.** La orden viaja por la base, no por memoria.
   `ShellTask` mata el árbol de procesos completo, no solo el hijo directo.

## Trampas conocidas

- **wasmtime (store sincrónico):** la cancelación es `set_epoch_deadline(n)` +
  `Engine::increment_epoch()`. `epoch_deadline_async_yield_and_update` es solo
  para stores async. Los tests generan `.wasm` con `wat::parse_str`: no armar
  los bytes a mano.
- **Windows:** un `.exe` en uso no se puede sobrescribir. Para recompilar hay
  que parar el proceso `zeruel` que lo usa.
- **PowerShell 5.1:** no hay `&&` (usar `;`); `2>&1` sobre un ejecutable nativo
  convierte su stderr en errores falsos; los `.ps1` tienen que quedar en ASCII.
- **Verificar el panel en un navegador**, no solo con un HTTP 200: el JS puede
  fallar en tiempo de ejecución.
- **`target/` crece sin límite:** cargo no borra artefactos viejos. Un
  `cargo clean` de vez en cuando.

## Proceso de trabajo

- Un bug encontrado se arregla en el momento si tocás esa área; si no, se anota
  en `docs/IRREGULARIDADES.md` con prioridad (o en un issue).
- **No marques algo como resuelto sin correr el comando que lo prueba.**
- Si cambia un número publicado (RAM, tiempos, tamaño), actualizá
  `docs/COSTOS.md` y el README, y decí cómo lo mediste.
- Si un cambio empeora más de un 10 % el tiempo del pipeline de ejemplo o
  cambia sus filas, revisá `docs/HISTORIAL.md` antes de seguir.
