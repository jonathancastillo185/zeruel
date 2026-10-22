# HISTORIAL — zeruel

> **Nota para quien llega de afuera.** Este documento se escribió durante el
> desarrollo privado, cuando el proyecto se llamaba `nodo` y el workspace
> vivía en una subcarpeta. Los nombres y rutas (`nodo-core/...`,
> `zeruel/crates/...`, `BUGS.md`, `PLAN.md`) son históricos: hoy todo es
> `zeruel` y la estructura es la que describe el [README](../README.md).


Historia del proyecto: que se hizo, que se decidio y por que, y que numero dio.
Es el archivo que hay que leer **antes de rediscutir una decision ya tomada**.

Reune lo que antes vivia en `LOG.md`, `PLAN.md`, `PLAN-PENDIENTES.md`,
`ESTADO.md` y la parte de RESUELTOS de `BUGS.md`. El contenido se conservo tal
cual se escribio; lo unico que se corrigio son los checkboxes del plan que
habian quedado desactualizados (ver "Correcciones" mas abajo).

- **Estado de hoy y como trabajar** → `AGENTS.md`
- **Lo que falta corregir** → `IRREGULARIDADES.md`

---

## Indice

1. [Fases y rondas cerradas](#1-fases-y-rondas-cerradas) — la tabla resumen
2. [Correcciones a este historial](#2-correcciones-a-este-historial-2026-08-17)
3. [Bugs resueltos](#3-bugs-resueltos)
4. [Log de desarrollo](#4-log-de-desarrollo) — cronologico, del mas nuevo al mas viejo
5. [Plan original (Fases 0-8)](#5-plan-original-fases-0-8)

---

## 1. Fases y rondas cerradas

| Fase / Ronda | Que entrego | Fecha |
|---|---|---|
| **Fase 0** — Fundacion | Workspace de 4 crates, arquitectura, README | 2026-08-15 |
| **Fase 1** — SDK | `Task`, `Dag`, `TaskDef`, `TaskId`, `TaskState` | 2026-08-15 |
| **Fase 2** — Core | Store SQLite-WAL, engine, checkpoint/resume, CLI minima | 2026-08-15 |
| **Fase 3** — Datos | Arrow/Parquet, `PolarsTask`, `JoinTask`, `ExportCsvTask` | 2026-08-15 |
| **Hito 1** — MVP | Pipeline de 6 etapas end-to-end + durabilidad verificada con kill | 2026-08-15 |
| **Benchmark 10M** | nodo 6.18s / 2813 MB vs polars-python 6.39s / 4702 MB | 2026-08-15 |
| **Upgrade polars** | 0.47 → 0.55.2: de 14.84s a 6.18s (ZSTD + row groups 1M) | 2026-08-15 |
| **Fase 3.5** — Metricas | Sampler de RAM/CPU por task (sysinfo), persistido en `task_runs` | 2026-08-15 |
| **Fase 3.8** — Higiene | Retenciones, `gc`, rotacion de logs, `status` con pulso, paginado | 2026-08-15 |
| **Fase 4** — Scheduler | `serve`, cola en SQLite, prioridad + aging, admission control por RAM | 2026-08-15 |
| **Fase 5** — WASM | wasmtime 47 embebido, fuel + tope de memoria, `wasm-run` | 2026-08-15 |
| **Fase 6** — UI web | `nodo web`, tiny_http + UI embebida | 2026-08-15 |
| **Fase 7** — Distribucion | `self-test`, `deploy`, wrappers, Dockerfile, zip | 2026-08-16 |
| **Fase 8** — IA-friendly | `AGENTS.md`, `--json` global, `nodo mcp` | 2026-08-15 |
| **Ronda A.1** | Retries con backoff exponencial | 2026-08-16 |
| **Ronda A.2** | Cron propio (5 campos UTC) + `tick_cron` en `serve` | 2026-08-16 |
| **Ronda B.1** | `WasmTask` como tarea del DAG (deps/param/rows/retries) | 2026-08-16 |
| **Ronda B.2** | `SqlTask` sobre Parquet con **polars-sql** (DuckDB descartado) | 2026-08-16 |
| **Ronda B.3** | Macro declarativa `dag!` | 2026-08-16 |
| **Ronda C.1** | `backfill` por rango de fechas | 2026-08-16 |
| **Ronda C.2** | SLA / `max_wait` por DAG | 2026-08-16 |
| **Ronda C.3** | Cola acotada con rechazo explicito | 2026-08-16 |
| **Ronda C.4** | Escala 1M/10M/50M (tabla en README) | 2026-08-16 |
| **Ronda C.5** | `docs/` por concern — cerrada como NO APLICABLE | 2026-08-16 |
| **Ronda D.1** | UI por tabs, estilo verde bosque, `/api/metrics` y `/api/alerts` | 2026-08-16 |
| **Ronda D.2** | Cancelacion dura (epoch interruption de wasmtime) | 2026-08-16 |
| **Ronda D.3** | Landing por DAG (hash routing `#/dag/<nombre>`) | 2026-08-16 |
| **Multi-DAG** | Registry `dags.rs`: base para agregar otros pipelines | 2026-08-16 |
| **Manual** | `docs/MANUAL.md` + `.html` + `.pdf` | 2026-08-16 |
| **Auditoria** | 53 irregularidades relevadas → `IRREGULARIDADES.md` | 2026-08-17 |

Decisiones cerradas que **no** conviene reabrir sin leer el log:

- **polars-sql, no DuckDB** (B.2): 0 deps nuevas, 0 build extra, el binario no
  crece. DuckDB tiene CTEs y QUALIFY, a cambio de +200MB y +15min de build.
  Queda como feature flag si el ETL lo pide.
- **`macro_rules! dag!`, no proc-macro** (B.3): misma meta, cero crate nuevo.
- **No hay "pausar" un run** (D.2): no tiene sentido en un DAG. Cancelar si.
- **`docs/` por concern: no** (C.5): la condicion era "si AGENTS se vuelve
  largo". Se reevaluo el 2026-08-17 y la respuesta fue la contraria a dividir:
  se **unificaron** los 9 `.md` en 4.

---

## 2. Correcciones a este historial (2026-08-17)

Al unificar los documentos aparecieron afirmaciones que no coincidian con el
codigo. Se corrigieron **en el plan de abajo** y quedan anotadas aca para que no
se vuelvan a copiar:

- **Streaming out-of-core**: estaba tildado `[x]` en la Fase 3 y **no esta
  implementado** (`nodo-data/tasks.rs` hace `collect()` en RAM antes del sink;
  `with_streaming` solo aparece en `puro.rs`). Destildado. Ver N-18.
- **Retries, cron, backfill, SLA y cola acotada** figuraban como `[ ]` en la
  Fase 2 y la Fase 4 aunque estaban cerrados en las Rondas A y C. Tildados.
- **DuckDB** seguia en el diagrama de arquitectura y en la descripcion de
  `nodo-data`; se reemplazo por polars-sql, que es lo que se implemento.
- **Graficos 1M/10M/50M**: se entregaron como tabla markdown, no como grafico.
- **D.3 (landing por DAG)** figuraba como PENDIENTE en `PLAN-PENDIENTES.md` y
  estaba entregada en el commit `b00ad5e`.
- **Cantidad de tests**: el log dice "25/25" en varias entradas de 2026-08-16;
  hoy son **28** (el registry multi-DAG sumo 3).
- **Tamaño del binario**: el log menciona 66 / 79 / 82 MB segun la entrada. El
  release actual pesa **96.5 MB**.
- **El workflow de CI** (`.github/workflows/releases.yml`) que varias entradas
  dan por activo **fue borrado** el 2026-08-16 en el commit `adadcb9` por falta
  de minutos de CI. Las entradas del log se dejan como estan (son historia); lo
  que vale es esto.

---

## 3. Bugs resueltos

> Los bugs **abiertos** y toda la deuda viva se movieron a
> `IRREGULARIDADES.md`. Aca queda solo lo cerrado, como historial.
> Ojo: **B-00c figura como resuelto y no lo esta del todo** — los `&PathBuf` de
> `puro.rs` siguen dando warning (ver N-44).


#### B-00 — BUG funcional: topological_order invierte el orden [RESUELTO 2026-08-16]
- nodo-sdk/src/lib.rs:108 — `Dag::topological_order` hacía `out.reverse()` al
  final. Kahn produce raíz primero (orden topológico válido); el reverse lo
  dejaba con raíz al final (orden inválido: una tarea salia ANTES que su
  dependencia).
- No impactaba el engine (ejecuta por deps con `ready_slot`, no por el orden
  del vector), pero dejaba el test `orden_topologico_lineal` rojo.
- Fix: quitado el `out.reverse()`. Suite `cargo test --workspace`: 4/4 verdes.
- Commit: ver git log "Ronda A.1".

#### B-00b — BUG funcional: MCP no propagaba errores [RESUELTO 2026-08-16]
- nodo-cli/src/mcp.rs:35 — los dos branches del `if` eran idénticos:
  `json!({"result": resp})` tanto para error como para success. Cuando el
  handler devolvía `{"error": {...}}` el error se empaquetaba como `result`
  (nunca el cliente MCP lo veia como error).
- Fix: `if let Some(err) = resp.get("error")` → `{"error": err}`; si no,
  `{"result": resp}`. El cliente MCP ahora ve los errores correctamente.
- Commit: junto con A.1.

#### B-00c — Warnings clippy triviales [RESUELTO 2026-08-16]
- `for (id, _) in tasks` → `for id in tasks.keys()` (engine.rs)
- `opciones.clone()` (Copy) → `opciones` (deploy.rs)
- `&PathBuf` → `&Path` (puro.rs)
- `sort_by(|a,b| ...cmp(...))` → `sort_by_key(|t| Reverse(t.ram_peak_mb))` (main.rs)
- `println!("{}", "mensaje")` → `println!("mensaje")` (main.rs)

#### B-06 — UI: tabla de DAGs se armaba doble [RESUELTO 2026-08-16, UI rediseño]
- web.rs (UI embebida): la tabla de DAGs se construía en 2 pasos (primero
  `innerHTML` con las filas `<tr>` y después se reenvolvía con
  `<table>...innerHTML...</table>`), dejando los `<tr>` DUPLICADOS y uno de
  los sets fuera del `<table>` (invalid HTML, se veia roto).
- Fix: un solo `innerHTML` con la tabla completa en un template literal.
- Detectado al inspeccionar el DOM con un navegador headless.

#### B-07 — UI: campo `dur_ms` inexistente en /api/runs [RESUELTO 2026-08-16, UI rediseño]
- web.rs: el endpoint `/api/runs` devolvia `finished_at` pero el JS pedía
  `r.dur_ms` (que no existía) para mostrar la duración → siempre "1ms".
- Fix: quitar la columna duración (la info ya está en el detalle del run).

#### B-08 — UI: tab Alertas mostraba "2" cuando no había alertas [RESUELTO 2026-08-16, UI rediseño]
- web.rs (JS): `alist.innerHTML = alh.length || '<div>...sin alertas...</div>'`.
  En JS `[]` (array vacío) es truthy, pero `alh.length` cuando `alh` es `[]`
  da `0` (falsy) → OK, PERO el bug real era que se evaluaba el ARRAY (truthy
  siempre) en una versión intermedia, y al no haber alertas se imprimia el
  array en vez del fallback. Detectado en DOM: `#alist` mostraba "2".
- Fix: `alh.length ? alh.join('') : '<div class="empty">...sin alertas...</div>'`.

#### B-09 — wasmtime 47: `epoch_deadline_async_yield_and_update` solo para stores async [RESUELTO 2026-08-16]
- El método de cancelación "obvio" (`epoch_deadline_async_yield_and_update`)
  NO existe para stores sync en wasmtime 47 (error E0061/E0599). Se descubrió
  recién al compilar (la doc de docs.rs lo aclara: "para stores async").
- La API correcta para store SYNC es: `store.set_epoch_deadline(n)` + que un
  hilo externo llame a `engine.increment_epoch()`. El guest trapea en el
  próximo checkpoint de epoch. Cada `WasmTask` crea su `Engine` propio
  (epoch arranca en 0) → `set_epoch_deadline(1)` + `increment_epoch` alcanza.

#### B-10 — .wasm armado a mano: LEB128 de section sizes se rompe fácil [RESUELTO 2026-08-16]
- Al armar un .wasm como `&[u8]` literal para un test, los tamaños LEB128 de
  las secciones 6 (code) y 7 (exports) quedaron mal (31 vs 30, 12 vs 11) →
  "failed to parse wasms: invalid section: wrong length". 3 iteraciones.
- Fix: dev-dependency `wat = "1.252.0"` + `wat::parse_str(...)` en el test.

### Notas de proceso (de la epoca de BUGS.md)

- Siempre correr `cargo test --workspace` (no solo el crate que tocaste) antes
  de commit. El test de nodo-sdk estuvo rojo desde Fase 1 sin que se vea porque
  solo se corrían tests específicos.
- `cargo clippy --workspace --all-targets` antes de commit para detectar bugs
  nuevos. Los que son estilo van acá como DEUDA; los que son comportamiento
  van a corregir al tiro.
---

## 4. Log de desarrollo

Cronologico, del mas nuevo al mas viejo. Cada entrada registra un hallazgo,
decision o numero que valia la pena dejar escrito. Se conserva tal cual se
escribio en su momento: para el estado de hoy va `AGENTS.md`.


### 2026-08-18 — Ronda de correcciones: 51 de las 53 irregularidades

Sesion larga sobre la lista de `IRREGULARIDADES.md`. Lo que importa:

**El bug que hacia invisible a la feature estrella.** `recent_runs` mapeaba
`Cancelled` al comodin `_ => Running`, asi que un run cancelado se mostraba
"Running" para siempre — en la CLI, en la API y en la UI, con el boton Cancelar
todavia visible. Dos lineas de fix, pero explica por que "la cancelacion nunca
se veia funcionar". Ahora el parseo del estado vive en un solo lugar
(`RunState::parse`).

**La cancelacion no cruzaba de proceso.** La señal era un `AtomicBool` en
memoria del `Store`. El flujo real es `nodo web` (que solo encola) cancelando un
run que corre en `nodo serve`: el flag se prendia en un proceso que no ejecutaba
nada, y el `set_run_state(Success)` del final pisaba el `Cancelled`. Ahora el
pedido se persiste en `meta` (`cancel:<run_id>`), un `CancelWatcher` lo consulta
cada 250ms desde el proceso que ejecuta, y `Cancelled` gana siempre en la DB.
Verificado en vivo con los dos procesos: el log de serve dice "cancelacion
pedida desde otro proceso" y el run termina `Cancelled`.

**Streaming de verdad.** `PLAN.md` tenia tildado "streaming out-of-core (datos
mayores que RAM)" y el codigo hacia `lf.collect()` — todo el DataFrame en RAM —
antes de sinkear. `with_streaming(true)` aparecia una sola vez en el proyecto,
en `puro.rs`, que es el benchmark SIN orquestacion. O sea que la comparacion
"nodo vs nodo puro" tambien mezclaba dos motores distintos. Ahora el pipeline
sinkea el LazyFrame directo y cuenta las filas desde los metadatos del Parquet
(`parquet_rows`). **Los numeros del benchmark hay que volver a tomarlos.**

**Una regla inventada que se estaba propagando.** AGENTS.md y BUGS.md decian en
mayusculas "NO TOCAR el `let _ = ...init()`: devuelve un Guard que hay que
sostener". Es falso: `SubscriberInitExt::init()` devuelve `()`, y clippy lo
venia marcando. La confusion venia de `tracing_appender::non_blocking()`, que si
devuelve un `WorkerGuard` — pero este codigo no lo usa. Sacado.

**Dos implementaciones del mismo comando.** `backfill`, `sla` y `cola` estaban
escritas dos veces (CLI y MCP) y habian divergido: el backfill del MCP se
salteaba el ultimo dia del rango, y `sla --max-wait 0` borraba el tope por MCP
pero fijaba "0 segundos" por CLI (con lo que todo item violaba el SLA al
instante). Ahora hay una sola implementacion en `ops.rs`.

**Otros P0:** `status --run <uuid>` devolvia el header del run mas reciente en
vez del pedido (reproducido y corregido con `Store::run_by_id`); `list_dags` del
MCP estaba hardcodeado e ignoraba el registry multi-DAG; `recover_running`
ignoraba el `param` y mezclaba checkpoints entre runs; un run que reventaba
mataba el daemon y dejaba el item de cola trabado para siempre; la GC "diaria"
de `serve` no corria nunca porque el contador arrancaba en `now` y no se
persistia; el sampler dormia 100ms antes del primer sample, asi que toda tarea
corta registraba RAM y CPU en cero.

**Bugs que apuraron los tests nuevos.** `store.rs` no tenia un solo test (792
lineas con GC, cola, aging, admission control y recuperacion). Al escribirlos
aparecio N-55: `rollups_monthly.wall_secs` es `REAL` en el esquema y el struct
lo leia como `i64`, asi que `rollups()` fallaba **siempre que hubiera una fila**.
Nunca se habia notado porque los dos callers hacen `.unwrap_or_default()`: la
tab Metricas mostraba "sin agregados" en vez de un error.

**La UI.** Salio del `const UI_HTML` de 280 lineas dentro del `.rs` a
`ui.html` con `include_str!` (que es lo que la doc ya decia que hacia). Se
reescribio: escapa todo lo que viene del server (habia un XSS almacenado via
`param`, reproducido y ahora verificado como cerrado), tema claro/oscuro,
duraciones, filtros locales, refresco adaptativo (2s si hay algo corriendo, 6s
si no, nada si la pestaña esta oculta) y estados vacios que explican que hacer.
`/api/dags` pasó de traer 10.000 runs a memoria a un `GROUP BY`.

**Higiene.** `cargo fmt` (164 diffs) en su propio paso, clippy a cero (incluidos
dos warnings que `BUGS.md` daba por resueltos y no lo estaban), deps declaradas
sin usar fuera (`toml`, `tokio`, `thiserror` de mas), el log de runtime
versionado sacado del indice, los `_chromo*.json` borrados, y los scripts del
benchmark **sacados del .gitignore** y movidos a `bench/`: sin ellos las tablas
del README no se podian reproducir.

**Y el disco.** A mitad de la sesion `cargo` empezo a fallar con "espacio
insuficiente": el disco estaba al 100% y `zeruel/target` pesaba 57 GB, 48 de ellos
en `target/debug/deps` (cargo no limpia los hashes viejos). Se borro `debug`
(51 GB liberados), `release` quedo intacto.

Resultado: **84 tests verdes** (eran 28), clippy y fmt limpios. Quedan abiertos
N-19 (re-medir el benchmark en condiciones comparables) y N-21 (medir el binario
release despues de todo esto).

---

### 2026-08-16 — UI por tabs + cancelación dura de runs (Fase nueva, post PLAN-PENDIENTES)

#### UI rediseño (web.rs, ~570 líneas en total con HTML/JS embebido)

- El pedido fue "no se ve cómodo": separar en pestañas, vista de detalle por DAG,
  pestaña de métricas, pestaña de alertas, y el estilo verde bosque de la
  documentación. Se reescribió `UI_HTML` completo.
- Tabs: **Dashboard** (KPIs + grafo SVG del DAG + tabla de DAGs clicables +
  higiene) · **Runs** (recientes, clic → detalle por task con filas/RAM/cpu) ·
  **Métricas** (rollups mensuales + stats por task históricas + SLAs) ·
  **Alertas** (SLA superados, fails 24h, DB grande, cola; con badge en la
  pestaña) · **Cola y Logs** (cola del scheduler con espera vs tope SLA +
  últimas líneas del log).
- Refresh cada 2s. Estilo: mismas variables CSS de `docs/MANUAL.html`
  (--verde-oscuro #1a3a2a, acento #66bb6a, etc).
- Bugs UI resueltos al momento (B-06 tabla DAGs doble, B-07 dur_ms inexistente,
  B-08 alertas vacías mostrando "2") — ver BUGS.md.
- Endpoints nuevos: `GET /api/metrics` (rollups + task_stats + sla) y
  `GET /api/alerts` (sla_violaciones + fails_24h + db_warning).
- Store: nuevos `rollups()`, `task_stats()`, `sla_config()` + structs
  `RollupRow`/`TaskStatsRow` (serializables).

#### Cancelación dura (el pedido: "corte violento, hasta loop infinito")

- Decisión: NO pausar (lo descartamos juntos: en un DAG no tiene sentido).
  Cancelar SÍ, y debe cortar en vivo, no "no arrancar la próxima":
  - **Wasm** (caso real del loop infinito): epoch interruption de wasmtime.
    `set_epoch_deadline(1)` al arrancar (engine del WasmTask arranca en epoch
    0) + watcher thread (5ms) que hace `engine.increment_epoch()` cuando el
    flag se setea → el guest trapea en el próximo checkpoint (≈ cada instr
    en wasmtime). Corta a mitad de instrucción.
  - **Tasks nativas Rust**: el engine chequea el flag al inicio de cada
    iteración del loop principal; si está set, no arranca ninguna task nueva
    y el run termina `Cancelled` (las que ya van terminan: son acotadas por
    el datos).
  - Señal: `Store.cancels: Mutex<HashMap<Uuid, Arc<AtomicBool>>>` (en
    memoria). `TaskContext.cancel: Option<Arc<AtomicBool>>` (nuevo campo en
    el SDK, 4 sitios de construcción actualizados). `request_cancel(id)`
    setea el flag + `UPDATE runs SET state='Cancelled' WHERE state='Running'`.
    `recover_running` solo busca `state='Running'` → un run cancelado no se
    re-ancla al rebootear.
  - UI: botón "✕ Cancelar" en el detalle del run (solo si `Running`) →
    `POST /api/run/<id>/cancel`.
- Tests nuevos (25/25 verde en total): `cancela_wasm_loop_infinito_en_vivo`
  (fixture .wasm generado a vuelo con `wat::parse_str`, fuel 10^12, cancel a
  los 200ms → termina en 0.15s: prueba que el corte es por epoch, no por
  fuel) y `cancela_engine_no_lanza_siguiente_task` (task lenta chequea
  ctx.cancel; la task dependiente NO arranca; run queda terminado).
- Lecciones (BUGS.md B-09, B-10): en wasmtime 47 la cancelación de store SYNC
  es `set_epoch_deadline` + `increment_epoch` (el método con "yield" es solo
  async); y NO armar .wasm a mano en tests — las LEB128 de section sizes se
  rompen (usar la lib `wat`).

#### Documentación

- AGENTS.md: secciones nuevas "Web UI", "Cancelación dura" + gotchas de git en Windows (git se cuelga sin stdin:
  `cmd /c "... < NUL"`; commit con `-c commit.gpgsign=false`; commit -F
  archivo; matar git.exe no conhost; 2>&1 en PS5.1 da errores falsos).



---

### 2026-08-16 — Ronda C.5: docs/ (NO APLICABLE) — RONDA C COMPLETA

C.5 era opcional con condición: "desglosar AGENTS.md en docs/ solo si se vuelve
largo". AGENTS.md va a 153 líneas, así que dividir en 3 archivos (architecture /
deployment / benchmarks) no se justifica: complicaría el punto único de entrada
para una IA sin ganar legibilidad. Se reevalúa si AGENTS pasa de ~250 líneas.
**Con esto cierran las Rondas A, B y C de PLAN-PENDIENTES.md.**

---

### 2026-08-16 — Ronda C.4: Escala 1M/10M/50M (HECHO)

Tabla de escala en README.md. Misma secuencia `gen --root <t> --n <N>` +
`run --fresh --json`, midiendo el wall del run y el working set maximo del
proceso (muestreo `Get-Process`):

| Filas | Wall nodo | RAM peak nodo |
|-------|-----------|---------------|
| 1M    | 1.17s     | 347 MB        |
| 10M   | 6.03s     | 2862 MB       |
| 50M   | 29.22s    | 14.15 GB      |

Escalabilidad casi lineal: x5 filas → 4.9x wall y ~4.8-4.9x RAM en ambos saltos.
Cuello de botella `join_products` (tabla mas ancha, alcanza el peak en los 3).
50M corre end-to-end sin streaming (14GB, dentro de los 64GB). Sin punto de
quiebre en el rango medido. Nota: se pedia "grafico"; se resolvió con tabla
(no hay infra de graficos; la tabla muestra la escala). Python solo a 10M.

---

### 2026-08-16 — Ronda C.3: Cola acotada con rechazo (HECHO)

Tope global de items en cola (`meta queue.max_size`, 0 = sin tope). `nodo cola --max N`
lo fija; `nodo cola` (sin args) muestra tope + total en cola. `enqueue` lo respeta: si
la cola esta llena, rechaza con motivo claro. Es la contraparte del SLA (C.2): el SLA
alerta sobre la *espera*, la cola acotada limita el *total pendiente*.

- store.rs `enqueue`: si `queue.max_size > 0` y `COUNT(queue WHERE state='queued') >=
  max`, devuelve `ok: false` con "cola llena (X/N items en cola); sube el tope con
  `nodo cola --max N` o espera a que libre".
- CLI: `nodo cola [--max N]`. Smoke: tope 2, dos encolados OK, el tercero rechazado.
- Suite: 23 tests verdes, clippy sin errores nuevos.

---

### 2026-08-16 — Ronda C.2: SLA / max_wait por DAG (HECHO)

`nodo sla --dag X --max-wait SECS` configura un tope de espera en cola por DAG.
`nodo status` alerta si un item lo excede.

- El tope se guarda en `meta` con clave `sla:<dag>` (KV generico ya existia).
  `nodo sla` valida `--max-wait >= 0`.
- `sla_violaciones(store, items)`: para cada item de la cola, si su DAG tiene
  un tope configurado y `espera > tope`, lo reporta. Espera = `now - enqueued_at`
  (ya lo calculaba `ms_cola`).
- `nodo status --json`: `cola.sla_violaciones[]` con `{id, dag, param, espera_seg,
  max_seg}`. `nodo status` (human): bloque `[SLA] N item(s) superan su maximo de
  espera` debajo de la cola.
- No es un rechazo (eso es C.3): solo alerta. El scheduler sigue consumiendo la
  cola como antes; el SLA le avisa al operador que algo esta atrasado.
- Smoke: `gen` + `run` + `enqueue --param bf` + `sla --max-wait 1` + `sleep 2` +
  `status --json` → item con espera 4s, tope 1s, aparece en `sla_violaciones`.

---

### 2026-08-16 — Ronda C.1: backfill (HECHO)

`nodo backfill --dag X --desde YYYY-MM-DD --hasta YYYY-MM-DD [--priority N]`
encola un run por dia en el rango.

- Itera `NaiveDate` desde `desde` hasta `hasta` y llama a `store.enqueue(dag,
  fecha, prio)` por cada dia. Reutiliza la cola existente (el scheduler la
  consume cuando este corriendo) — no es un run inmediato.
- Validacion: `--desde`/`--hasta` en formato `YYYY-MM-DD` (parse con
  `NaiveDate`); si `desde > hasta`, error claro.
- Los dias que `enqueue` rechaza (ya en cola o Running del mismo DAG+param)
  se reportan en `skipped` sin hacer fallar el comando.
- `--json`: `{ dag, desde, hasta, encolados, totales, skipped }`.
- Smoke: `backfill 2026-08-10..2026-08-13` → 4/4 encolados; rango inverso →
  error con exit 1.

---

### 2026-08-16 — Ronda B.3: macro `dag!` (HECHO)

Reducción de boilerplate declarativa. Decidí `macro_rules!` en vez de
proc-macro: misma meta, cero crate nuevo, cero overhead.

```rust
let dag = nodo_sdk::dag!("ecommerce-etl",
    "validar"        => [],
    "limpiar"        => ["validar"],
    "join_customers" => ["limpiar"],
    "join_products"  => ["join_customers"],
    "rollups"        => ["join_products"],
    "export_csv"     => ["rollups"],
);
```

Antes: 6 llamadas a `TaskDef::new` con `TaskId("...".into())` en un `vec!`.
Despues: 6 lineas declarativas, legibles como un grafo.

- **nodo-sdk**: `macro_rules! dag` con `#[macro_export]`. Dos ramas:
  solo nombre, o nombre + tareas. Las deps son `&str` y la macro las
  convierte a `TaskId` internamente.
- **Aplicada al DAG real** de `pipeline.rs` (6 tasks ecommerce-etl). El
  `vec![TaskDef::new(...), ...]` se reemplazó por la macro; las tareas
  opt-in (wasm/sql) siguen con `dag.tasks.push(TaskDef::wasm(...))`.
- **Tests**: 2 nuevos en el SDK (`macro_dag_lineal`, `macro_dag_vacia`).
- **Suite**: 23 tests verdes, clippy sin warnings nuevos.
- **No es proc-macro**: no se puede anotar `fn`s existentes con `#[dag]`
  para generar id desde el nombre de la fn. Si eso se necesita mas adelante,
  es un crate `nodo-sdk-macros` aparte (proc-macro).

---

### 2026-08-16 — Ronda B.2: Tarea SQL sobre Parquet (HECHO, polars-sql)

Una query SQL ahora es **una tarea más del grafo** (con deps + param + rows +
retries). Sirve para el ETL del medio que no merece Polars a mano (join +
filtro + agregado), sin salir del motor.

**Decisión de motor: polars-sql, NO DuckDB** (tras evaluar ambos).
- Analizamos un caso real (top N con ranking + share, CTEs + window) y el
  escenario típico de "descargar → ETL pequeño → cargar a MySQL".
- El paso 3 (cargar a MySQL) es siempre una tarea Rust (sqlx/mysql) con
  ninguno de los dos motores.
- El ETL "pequeño" típico (join + filtro + agregado simple) lo cubre
  polars-sql a mano.
- DuckDB (SQL completo, CTEs, `QUALIFY`, `COPY TO`) valdría la pena **solo**
  si el ETL crece. En ese caso, mejor como feature flag de compilación
  (`duckdb = ["dep:duckdb"]`) para no tocar el binario base.
- Costo de DuckDB: binario +200MB (82 → ~280MB), build +10-15min, 2 motores
  en el binario, no streamea (carga todo a RAM).
- **Conclusión**: polars-sql. Si el ETL crece en 3 meses, se agrega DuckDB
  como opt-in.

- **nodo-sdk**: `SqlSpec { sql, tables: HashMap<nombre,PathBuf>, output }` +
  campo `sql: Option<SqlSpec>` en `TaskDef` + `TaskDef::sql`.
- **nodo-core/sql.rs**: `SqlTask` con `SQLContext` de polars-sql. Las tablas se
  registran como `LazyFrame::scan_parquet` (pushdowns nativos de Parquet; no
  carga todo a RAM antes de la query). El resultado se escribe Parquet por
  `sink` (row_group_size=1M, coherente con el resto del pipeline).
- **nodo-core/engine**: `effective_tasks()` resuelve las SQL declaradas
  (misma mecánica que las wasm del B.1).
- **nodo-cli**: `nodo run --sql-file X.sql --sql-tables "t=ruta,o=ruta2"
  [--sql-out Y.parquet]` agrega la tarea `sql_etl` al DAG (opt-in). Las rutas
  relativas se resuelven contra el `--root`.
- **Bug B-05 detectado por smoke E2E**: al colgar la SQL de `validar`, la
  query podía leer `output/validas.parquet` en paralelo a la tarea que lo
  escribe (tabla no existe / incompleta). La SQL opt-in ahora depende de
  `export_csv` (última tarea del pipeline): cuando corre, todos los Parquet
  del DAG ya existen.
- **Deuda B-01 (cmd_run 9 args) cerrada**: los args de tarea extra pasaron al
  struct `RunOpts { wasm, fuel, mem_mb, sql_file, sql_tables, sql_out }`.
  `cmd_run` vuelve a 6 args.
- **Tests** (4 nuevos): `sql_run_escribe_parquet_con_el_resultado`,
  `sql_tabla_inexistente_falla_claramente` (en sql.rs) +
  `sql_corre_dentro_del_dag`, `sql_en_dag_despues_de_tarea_native` (engine).
- **Smoke E2E** real: `nodo gen --n 1000` + `nodo run --sql-file q.sql
  --sql-tables "validas=..."` → 6 tareas (validar→...→export_csv→sql_etl) OK,
  `output/sql_smoke.parquet` con rollup por categoría.
- **Suite**: 21 tests verdes (16 + 4 nuevos + 1 sql_spec en sdk). Clippy: 0
  warnings nuevos (la deuda de estilo sigue en BUGS.md).

---

### 2026-08-16 — Ronda B.1: WasmTask dentro de un DAG (HECHO)

Un `.wasm` ajeno ahora es una **tarea más del grafo** (con deps + param + rows +
retries), no solo invocable vía `nodo wasm-run`. Completa la historia de sandbox
(Fase 5) que iba a medias.

- **nodo-sdk**: `WasmSpec { path, fuel, mem_mb }` (con `WasmSpec::new` → fuel 100M,
  16MB) + campo `wasm: Option<WasmSpec>` en `TaskDef`. Constructores
  `TaskDef::new` (nativa) y `TaskDef::wasm` (sandbox). El DAG queda
  auto-descriptivo.
- **nodo-core/engine**: `effective_tasks()` resuelve las tareas wasm declaradas y
  las suma al hashmap del caller. Cada `WasmTask` se **compila UNA vez por run**
  (wasmtime es caro; se amortiza en todos los reintentos). El dispatch es
  genérico: las wasm pasan por el mismo `ejecutar_con_retries` (deps/param/rows/
  retries/depuracion).
- **nodo-core/wasm**: `WasmTask::new(id, path, fuel, mem_mb)` ahora recibe el id
  del `TaskDef` para que logs/checkpoint la reconozcan por nombre.
- **nodo-cli**: `nodo run --wasm X.wasm [--fuel N --mem-mb N]` agrega la tarea wasm
  al DAG (tarea `wasm_etl`, dep de `validar`, opt-in). No toca el invariante de
  filas del benchmark (solo existe cuando se pide). `--json` run reporta `wasm`.
- **Fixture**: `nodo-core/tests/fixtures/wasm_hola.wasm` (5.4KB, copiado del build
  de examples) para que los tests corran sin depender de un build wasm previo.
- **Tests** (nodo-core/engine): `wasm_corre_dentro_del_dag` (run con solo una tarea
  wasm, rows=12 = param.len()+palabras de "hola mundo") y
  `wasm_en_dag_comparte_orden_topologico_con_rust` (DAG mixto rust→wasm respeta el
  orden topológico).

Suite: 16 tests (9 cron + 2 retries + 2 sdk + 2 wasm + 1 wasm_spec), 0 fallos.
Clippy: 1 warning nuevo trivial (`unnecessary_lazy_evaluations` `then`→`then_some`,
corregido al tiro) + 1 deuda estilo (`cmd_run` 9 args → B-01).

---

### 2026-08-16 — Ronda A.1: Retries con backoff (HECHO)

`Engine::ejecutar_con_retries` (nodo-core/src/engine.rs): ante fallo reintenta
inline con backoff exponencial (2^i * base_ms, base 2000ms). Cada intento
checkpointea `set_task_state` con `Failed { retries_left }`. Solo al agotar
`Dag.max_retries` el run falla. `Engine::with_retry_base_ms(ms)` para tests.
Tests: `reintenta_hasta_que_pasa` (falla 2, pasa en 3a) y
`agota_reintentos_y_falla` (siempre falla, max_retries=2).

**Bug preexistente corregido:** `Dag::topological_order` (nodo-sdk) hacía
`out.reverse()` al final del Kahn, dejando la raíz al final (orden inválido).
No lo usaba el engine (ejecuta por deps con `ready_slot`), pero dejaba la
suite roja. Quitado. `cargo test --workspace`: 4 tests, 0 fallos.

**Gotcha:** siempre correr `cargo test --workspace` completo antes de commit,
no solo tests específicos (el test de nodo-sdk estuvo rojo sin que se vea).

---

### 2026-08-16 — Ronda A.2: Cron/Schedules (HECHO)

`nodo serve` ahora dispara runs por schedule cron. Implementación:

- **`cron.rs`** (nodo-cli): parser cron propio, 5 campos UTC (min hora dia-mes mes
  dia-semana). Soporta `*`, `N`, `A-B`, `*/S`, `N/M`, listas (`a,b,c`). 9 tests.
  No soporta nombres de meses/dias, `L`, `W`, `@hourly` (restringido a lo necesario;
  se extiende ahi si falta).
- **`pipeline.rs`**: `dag_cron(env) -> (String, Option<String>)` expone nombre del
  DAG + su schedule. El DAG de ejemplo declara `"0 6 * * *"` (diario 06:00 UTC,
  opt-in: `None` = solo bajo demanda, no se dispara).
- **`scheduler.rs`**: `tick_cron(&store, &env)` al inicio de cada tick del bucle
  de `serve`. Si el cron coincide: encola run con `param = fecha ISO del día`.
  **Anti-doble-disparo**: clave `cron:<dag>` en la tabla `meta` guarda el minuto
  UTC ya disparado; si se repite el mismo minuto no re-encola. Sobrevive a
  re-levantar `serve` (dura en SQLite).
- **`store.rs`**: `meta_get(key)` / `meta_set(key, value)` sobre la tabla `meta`
  (ya existía para `last_gc`).

**Gotchas cron:**
- `?` no sirve en fn `-> bool`: usar `match` / `return false`.
- chrono 0.4: `with_ymd_and_hms(y, m, d, h, mi, sec)` — pide segundos (antes no).
- Day-of-week en cron: 0=domingo. `chrono` da `number_from_monday()` = 1..7.
  Fórmula: `number_from_monday() % 7` (7→0 domingo, 1→1 lunes, etc.).

Suite: 13 tests (9 cron + 2 retries + 2 sdk), 0 fallos.

---

### 2026-08-16 — Fase 7: Distribución portable (HECHO)

Última fase del plan. `nodo deploy` + `nodo self-test` + CI de releases.

**`nodo self-test [--keep]`** (nodo-cli/deploy.rs): valida el binario end-to-end
en un tempdir aislado: gen 10000 transacciones → run → 6/6 tasks OK (verifica contra
el store, no solo exit code) → gc dry-run. Sale 0 si el binario es útil.
`--keep` deja el dir para inspeccionar. Tiempo: <2s en release.

**`nodo deploy --dest DIR [--with-data D] [--with-state S] [--docker] [--zip]`**:
arma una carpeta portable:
- `nodo.exe` (o `nodo` en unix) — el binario
- `run.ps1` / `run.sh` — wrappers (`once`, `self-test`, `serve`, `web`, o cualquier comando)
- `README.md` — quickstart <10 min
- opcionales: `data/`, `state/`, `Dockerfile` + `docker-run.sh`, y `.zip` junto al destino

El zip se llama `nodo-portable-<windows|linux|macos>.zip`. **Verificado**: el paquete
desplegado corre `self-test` sin nada mas — la carpeta es 100% autonoma.

**Gotchas encontrados**:
- PS5.1: `$args[1..]` NO existe (range open-ended). Usar `$args[1..($args.Count-1)]`.
  Si un wrapper con esa sintaxis se escribe, `powershell -File run.ps1` falla en el
  parse inicial (no en runtime).
- `zip` crate v2 tenia API rota en `zip::write::FileOptions` (generico sin inferencia
  de tipo). Pasamos a **v8** (estable). `FileOptions<()>` tipo anotado para inferir.
- `zip` v8 `finish()` no pide `mut` (ya no devuelve writer).
- `cargo add --workspace` no existe: editar `zeruel/Cargo.toml` a mano.
- `cargo build --bin nodo` en modo dev SIN caché de dependencias toma el mismo tiempo
  que un build release (12 min). Si se corta a mitad, relanzar `cargo build --release`
  incremental es mas barato (solo recompila `nodo-cli`, ~6 min).

**CI de releases** (`.github/workflows/releases.yml`, trigger `push: tags: v*`):
- Matrix: windows-msvc, x86_64-unknown-linux-musl, macos (aarch64+x86_64)
- Cada job: `cargo check --target T` (2 min) → `cargo build --release --target T`
  (10-15 min) → `nodo self-test` (2s) → `nodo deploy --zip` → `upload-artifact`
- Linux musl necesita `zlib-ng-compat` (apt) + `musl-tools` (para binutils)
- `build_releases.ps1` (nodo/) hace lo mismo en local, si estan los toolchains

**Docker** (opcional, generado con `--docker`):
- Dockerfile multi-etapa: `rust:1.82` builder → `debian:bookworm-slim` runtime
- Runtime lleva SOLO el binario (82 MB), sin Rust ni deps.
- `ENTRYPOINT ["nodo"]`, `CMD ["serve", "--root", "/data", "--state", "/state"]`,
  `EXPOSE 8787`. `docker-run.sh` = build+run con `-v $(pwd)/data:/data` etc.
- `nodo` NO depende de Docker en ninguna otra parte: el Dockerfile es solo una
  opcion de despliegue, no un requisito.

**Docs**: README nodo/ actualizado (benchmarks 0.55.2 + seccion "Distribucion
portable"), PLAN.md Fase 7 cerrada, AGENTS.md con seccion "Self-test y
distribucion", .gitignore con `dist/`, `nodo-portable/`, `*.zip`.

---

### 2026-08-15 — Fase 6: UI web embebida (HECHO)

`nodo web --state S --root R --port 8787` (def 8787): server HTTP en el binario
(dep `tiny_http`), UI embebida con `include_str!` (sin assets externos).

**API JSON** (mismo store SQLite que la CLI):
- `GET /api/dags` — DAGs + conteos ok/fail por estado
- `GET /api/runs` — ultimos 200 runs
- `GET /api/run/<id>` — detalle: run + task_runs (estado, wall, RAM peak, CPU)
- `GET /api/queue` — cola en vivo
- `GET /api/hygiene` — pulso de higiene
- `GET /api/logs?tail=N` — cola de logs de `state/logs/`
- `POST /api/run {dag,param,priority}` — encola (conflicto DAG+param rechazado)
- `POST /api/retry` — reencola el ultimo run fallido

**UI** (`web.rs`, una pagina): header con estado/cola + retry, panel de DAGs con
boton "Nueva corrida" (param + prioridad), pulso de higiene, tablas de runs y cola
con auto-refresh cada 2s, click en un run abre el detalle de tareas.

Verificado en navegador (navegador headless): renders OK, encolado por UI OK, retry OK.
Solo 404 = favicon (cosmetico).

**Gotchas:** tiny_http 0.12: `request.url()` devuelve SOLO path+query (el metodo
se toma por `request.method()`); `Response::from_string(s).with_status_code(c)`.
PowerShell 5.1 rompe las comillas simples de `curl -d '{...}'` para POST:
escribir el body a archivo y usar `curl --data @file`.

---

### 2026-08-15 — Fase 6: sandbox WASM (wasmtime embebido) (HECHO)

El binario de nodo ejecuta tareas `.wasm` dentro de su propio proceso (Fase 5
de PLAN.md). Wasmtime 47.0.3 como dependencia (crate), target guest
`wasm32-wasip1` (rustup).

**Diseño:**
- `WasmTask` (nodo-core/wasm.rs): compila el `.wasm` UNA vez (`WasmTask::new`),
  instancia por corrida. Limits: `consume_fuel` (CPU) + `ResourceLimiter`
  (memoria lineal). Sin WASI: el guest no toca red/disco.
- ABI por offsets sobre una base dinàmica: el guest exporta `buf_ptr() -> i32`
  (la dirección real de su `static BUF`, que el linker ubica donde quiere —
  el guest usa 17 páginas mínimas) y el host lee/escribe en
  `base + 16` (param) / `base + 8192` (rows BE i32) / `base + 8196` (mensaje).
  Export `task_run() -> i32` (0 = OK, <0 = error).
- CLI: `nodo wasm-run --wasm X --param Y --fuel 100000000 --mem-mb 16 [--json]`

**Bugs de wasmtime 47 (nuevos, no en la doc):**
1. `Config::memory_pages` se removió → el tope de memoria es vía
   `ResourceLimiter` en `StoreData` (el closure de `store.limiter` toma el
   limiter de la data del store: si lo apilado, error de lifetime E0521).
2. `memory_growing(current, desired, maximum)` opera en **BYTES**, no en
   páginas: comparaba 1.1MB contra 256 → todo rechazado con "memory minimum
   size exceeds memory limits" (error engañoso: no es el minimum del módulo).
3. `instantiate(&mut store, &module)` ahora toma 2 args (el Linker va en self).
4. `get_typed_func` devuelve `Result` (no Option) en 47.
5. `mem.read/write` usan `usize` y devuelven `MemoryAccessError` (no anyhow):
   helper `to_err` genérico sobre `Display` (evita el clash de traits
   `context` de wasmtime con anyhow — mismo pattern que con polars 0.55).

**Verificado (release):**
- `wasm-hola` (guest): param "hola-mundo" → rows=12 (10 bytes + 2 palabras),
  mensaje OK, 0.2 ms por corrida.
- `wasm-bucle` (loop infinito + `black_box`): con `--fuel 1000000` cayó en
  <1 s con trap — el fuel se respeta, nodo no se cuelga.
- nodo.exe creció 66 MB → 82 MB con wasmtime embebido (JIT cranelift).

**Git:** commit 37a165f push a `origin main` (repo usa main, no master).

---

### 2026-08-15 — Fase 5: IA-friendly (`--json` + `nodo mcp` + AGENTS.md) (HECHO)

Hice el repo amigable para agentes de IA / scripts, sin tocar el engine:

1. **`--json` flag global** (clap `global = true`): `nodo --json <comando>` da salida
   JSON en `gen`, `run`, `list`, `gc`, `enqueue`, `status`. `status --json` devuelve
   `{run, tasks[], higiene, cola{items[]}}` en un solo objeto. El resto sigue
   imprimiendo texto humano (default, sin la flag).

2. **`nodo mcp` — MCP server por stdio** (JSON-RPC 2.0, una linea JSON por mensaje,
   spec 2024-11-05). Nuevo crate-módulo `nodo-cli/src/mcp.rs`. Tools:
   `list_dags`, `trigger_run` (encola), `run_status` (por uuid o "latest"),
   `task_metrics` (rows/ram/cpu por tarea), `gc_status` (pulso de higiene),
   `queue` (items pendientes). Responde `initialize`, `tools/list`, `tools/call`.
   Dependencia nueva: `serde_json` (workspace). Se prueba con un echo de peticiones
   JSON-RPC; no requiere red ni puerto.

3. **`AGENTS.md` en la raiz del repo**: mapa de workspace, comandos, invariantes
   (integridad, higiene por defecto, scheduler por DAG+param, polars 0.55 API,
   gotcha E0034 de PolarsContext vs anyhow), gotchas de Windows/CRLF, tabla de
   benchmarks de referencia y estructura de data/state. Es el punto de entrada para
   cualquier agente que abra el repo.

Verificado en release: `--json` en 6 comandos OK; MCP responde initialize + tools/list
(6 tools) + tools/call (task_metrics devolvió 6 tareas). Sin regresión en el pipeline.

Notas: en PLAN.md esto figura como **Fase 8** (la Fase 5 de PLAN es el sandbox WASM,
que sigue pendiente). Lo llamamos "Fase 5 IA-friendly" en la conversación.

---

### 2026-08-15 — Fase 4: scheduler `nodo serve` (HECHO)

**Motivo:** en Airflow el scheduler es el cuello de botella: los triggers se
pierden cuando la UI o la DB se ponen lentos. En nodo el scheduler vive en el
binario: un bucle simple sobre la cola SQLite, sin proceso extra.

**Diseño:**
- `queue` (SQLite): `id, dag, param, priority, enqueued_at, state`.
- `nodo enqueue --dag X --param Y --priority N`: encola. Rechaza si ya hay
  un run del mismo DAG+param en cola o Running (conflict policy: bloquear).
- `nodo serve --root data --workers 4 [--max N]`: daemon. Cada tick:
  1. GC automatica si pasaron 24h (reutiliza `store.gc`).
  2. `pick_next`: prioridad efectiva = `priority + en_cola/300s` (aging, no
     hay hambre), FIFO en empate. Run conflict: si el `dag|param` ya esta
     Running, queda bloqueado. Admission control: si el RAM peak historico
     del DAG + margen 20% > RAM disponible (`sysinfo`), se salta (sigue en
     cola) en vez de arrancar y swappear.
  3. Lanza el run reutilizando `pipeline::run` (mismo codigo que `nodo run`),
     lo retira de la cola al terminar (el run queda en `runs` con su estado).
- `--max N`: sale cuando terminaron N runs (testing). Sin `--max` corre para
  siempre.
- `nodo status` ahora muestra un **semaforo** al final: items en cola con su
  tiempo de espera, y la higiene (ya de 3.8).

**Verificado (release):** 2 enqueues (prio 9 y 1) → `serve --max 2` eligió
primero el de prioridad 9, luego el otro; el de prio 1 quedo en cola (se ve
en `status` con `espera=`). Conflicto DAG+param rechazado al encolar.

**Nota de implementacion:** la herramienta de edicion del agente se rompio
varias veces con multi-linea + CRLF (borraba en vez de reemplazar).
Workaround: reescribir el archivo completo.

---

### 2026-08-15 — Fase 3.8: higiene del sistema (HECHO)

**Motivo:** el problema que degrada a Airflow con el tiempo es la DB de
metadata y los logs sin tope. En nodo va acotado desde el dia 1 (no opcional).

**Schema — nueva tabla `rollups_monthly`** (`store.rs`):
Agregados por DAG por mes `(mes, dag, runs, ok, fail, wall_secs, ram_peak_mb)`,
PK `(mes, dag)`. Sobrevive a la purga de runs viejos: es el historico
"para siempre" que el plan pide.

**Retenciones + GC** (`Store::gc(dry_run)`, `store.rs`):
- `task_runs` (detalle) de runs con `started_at` > **30 dias** → borrar
- `runs` con `started_at` > **1 año** → borrar, pero antes consolidar sus
  stats en `rollups_monthly` (UPSERT con `MAX` en ram/wall)
- `rollups_monthly` → nunca se borra
- Registra `meta.last_gc` (unix) cada vez que corre de verdad
- `dry_run` solo cuenta lo que borraria, sin tocar nada
- `GcReport { runs_borrados, task_runs_borrados, meses_agregados, db_size_bytes }`

**Pulso de higiene** (`Store::hygiene()`, `store.rs`):
`Hygiene { db_size_bytes, runs, task_runs, meses, ultima_gc, db_mb_warning }`.
Warning si el DB > 512 MB. Se imprime al final de `nodo status`.

**CLI** (`main.rs`):
- `nodo gc [--dry-run]` — purga manual
- `nodo list` — ahora paginado: `--limit` (default 50, antes 10) + `--offset`
  (nuevo). `recent_runs(limit, offset)` en store.
- `nodo status` — al final muestra el pulso de higiene
- `nodo serve` (GC automatica diaria) → va en la **Fase 4** (aun no existe `serve`)

**Rotacion de logs** (`init_logs` + `prune_old_logs`, `main.rs`):
- Dep nueva: `tracing-appender = "0.2"` (workspace).
- Logger a **stdout** (siempre) + **archivo rotativo diario** en
  `state/logs/nodo.log.AAAA-MM-DD`. Los comandos con `state` (run/list/status/
  gc) loguean a archivo; `gen` y `puro` solo a stdout (no tienen state).
- `prune_old_logs(dir, 14)` borra rotados con mtime > 14d al arrancar.
- Nota: el appender bufferiza y flush al drop, al salir del proceso.
- Nota: `nodo run --fresh` borra `state` entero (incluye `state/logs`); el
  archivo a 0 bytes se ve si se listea justo despues de un `--fresh`.

**Verificacion (release):** `run` 100k OK, `list --limit 5` OK, `gc --dry-run`
y `gc` OK (0 a borrar, db reportado), `status` muestra pulso de higiene
(db/runs/task_runs/meses/ultima_gc), `state/logs/nodo.log.2026-08-15` con
lineas. `cargo check` 0 warnings.

**Depends:** la GC automatica diaria se conecta a `nodo serve` en Fase 4.

---

### 2026-08-15 — Benchmark 10M (sesión 2)

#### Problema inicial: `nodo-core` no compilaba (6 errores en `metrics.rs`)

Todos los errores eran de la API de `sysinfo 0.39` (había sido escrito contra
una API anterior):

1. `Pid(u32)` no se construye como tuple → `Pid::from_u32(u32)`
2. `System::new_with_specifics(ProcessRefreshKind::nothing())` → E0308
   (el arg es `RefreshKind`, no `ProcessRefreshKind`). Solución:
   `System::new_with_specifics(RefreshKind::new().with_memory(true))`
3. `refresh_processes_specifics(2 args)` → en 0.39 son 3:
   `(updates: ProcessesToUpdate, remove_dead: bool, kind: ProcessRefreshKind)`
4. `p.memory()` E0423 → error en cascada del E0308 de arriba; con el
   constructor correcto, `p` es `&Process` y `memory()` existe.
5. `cpu_usage()` devuelve `f32` → cast `as f64` antes de dividir por 100.0

**Bug real oculto por warning:** `set_task_state` en `store.rs` recibía
`ram_peak_mb` y `cpu_secs` pero el INSERT no las incluía. Agregadas al INSERT.

#### Verificación

- `cargo check --workspace`: limpio, 0 warnings
- `nodo run --fresh --root data` (100k): 3.5s, OK
- `nodo status --run <id>`: tabla de métricas por task (wall, filas, RAM peak)
- Release build: 12m, binary 51.8MB

#### Benchmark 10M — resultados

**Método:**
- nodo: `cargo build --release`, `nodo run --fresh --root data` (10M, seed 42)
- Python: pipeline directo (misma lógica, mismo dataset) corriendo dentro del
  container del scheduler de un Airflow propio (Python 3.12, pandas 2.3.3,
  polars 1.43.2, pyarrow 24)

**Problema con Airflow como benchmark:**
El subprocess sandbox de Airflow 3 aísla `/tmp` por task: las escrituras de una
task no son visibles para la siguiente. Intenté usar `dags test` y `dags trigger`
y el DAG no terminaba (limpiar no veía validas.parquet). La solución fue correr
el pipeline directo en Python sin Airflow (misma lógica), midiendo solo el
compute. El overhead de Airflow (scheduling, sandbox, metadata Postgres) no está
incluido en los números.

**Resultados (10M transacciones, 9.85M validadas, 1080 filas en rollups):**

| | nodo (Rust, release) | Python + pandas 2.3 | Python + polars 1.43 |
|---|---|---|---|
| Wall total | 14.84s | 36.96s | 6.39s |
| Peak RSS | 2952 MB | 4317 MB | 4702 MB |
| Time-to-first-task | ~0.7s | — | — |
| Binary | 51.8 MB, 0 deps | Python 3.12 + pandas + pyarrow | Python 3.12 + polars |

**Por task (nodo, release):**
| task | wall | RAM peak |
|---|---|---|
| validar | 4.25s | 1548 MB |
| limpiar | 4.42s | 1554 MB |
| join_customers | 3.70s | 1995 MB |
| join_products | 4.17s | 2952 MB |
| rollups | 0.70s | 1101 MB |
| export_csv | 0.03s | 0 MB |

**Por task (Python polars directo):**
| task | wall |
|---|---|
| validar | 0.92s |
| limpiar | 1.27s |
| join_customers | 1.45s |
| join_products | 1.82s |
| rollups | 0.93s |
| export | 0.00s |

**Hallazgo clave (verificado con `nodo puro`):**

| | wall | peak RSS |
|---|---|---|
| nodo completo (checkpoint + métricas) | 14.84s | 2952 MB |
| nodo puro (sin checkpoint, sin métricas, streaming) | 14.00s | ~2900 MB |
| polars-python 1.43 directo (streaming, 16 threads) | 6.39s | 4702 MB |
| pandas 2.3 directo | 36.96s | 4317 MB |

- Overhead de infraestructura nodo (checkpoint SQLite + métricas sysinfo):
  **0.84s (5.6%)**. No es la causa del gap.
- Gap vs polars-python: **~7.6s (2.2x)**. Causa: polars-rs 0.47 en Rust
  nativo vs polars-python 1.43.2 (mismo engine, pero las versiones difieren
  y la Python usa streaming + compresión zstd en el Parquet write por defecto,
  que en Rust es `Uncompressed` en `ParquetWriteOptions::default()`).
- Streaming (`with_new_streaming(true)`) mejora 1.3s sobre in-memory en Rust.

**Conclusión honesta:**
Para "procesar 10M de filas lo más rápido posible", polars-python directo
gana en wall cruda (6.4s vs 14.8s) y en RAM consume más (4.7 vs 2.9 GB).
El valor de nodo no es ser el engine más rápido — es:
- 35% menos RAM peak (2952 vs 4702 MB)
- Durabilidad: kill a mitad → re-lee el checkpoint, no re-computa
- Métricas por task de fábrica (wall, RAM peak, CPU, filas)
- Binary portable de 52MB, cero dependencias
- Arranque en ~0.7s
- Costo: 2x en wall vs el engine más rápido

**Lo que nodo gana:**
- 35% menos RAM peak (2952 vs 4702 MB)
- Durabilidad: kill a mitad → re-lee el checkpoint, no re-computa
- Métricas por task de fábrica (wall, RAM peak, CPU, filas)
- Binary portable de 52MB, cero dependencias
- Arranque en ~0.7s (Airflow tarda minutos en parsear DAGs + conectar Postgres)

**Lo que nodo pierde:**
- Wall cruda: polars-python directo (6.39s) vs nodo con infra (14.84s)

#### Gotchas técnicos que quedaron documentados

- sysinfo 0.39: `Pid::from_u32()`, `System::new_with_specifics(RefreshKind)`,
  `refresh_processes_specifics(updates, remove_dead, kind)` (3 args),
  `cpu_usage()` → f32, `memory()` → u64 (KB)
- polars 0.47: `scan_parquet(path, ScanArgsParquet::default())` (2 args),
  `df.lazy()` toma ownership (no `&df`), `sort(["col"], SortMultipleOptions)`
  (strings, no `col()`), `col("ts").dt().date()` (no `.cast(DataType::Date)`),
  `filter(a.and(b))` (no `a & b`), `sink_parquet(SinkTarget::Path(buf.into()),
  ParquetWriteOptions, None, SinkOptions)` (4 args),
  `with_new_streaming(true)` (no `with_streaming()`),
  `lit(0.0)` para comparar contra columnas float/decimal (no `lit(0)` que es i32)
- Airflow 3 subprocess sandbox: aísla `/tmp` por task; las escrituras de una
  task no son visibles para la siguiente. Para benchmarks, correr el pipeline
  directo en Python es más fiable.
- docker cp: si la carpeta de destino existe, se anida (`bench_data/bench_data`).
  Para copiar contenido: `docker cp src/. container:dest/`
- pip en contenedores Airflow: `pip` directo → "Permission denied" (PATH mal);
  usar `python -m pip install`
- El dag-processor de Airflow cachea import errors en la DB; no re-importa
  hasta que cambia el mtime del archivo (touch desde DENTRO del contenedor)

---

### 2026-08-15 — Upgrade polars 0.47 → 0.55.2 (HECHO)

**Motivo:** cerrar el gap de 2.2x vs polars-python 1.43 (6.4s vs 14s). Mismo
engine. El fix directo de la velocidad.

**Aclaracion de versiones (importante):** el "1.43" que aparece en los
benchmarks anteriores es la version de **polars-Python** (pypi). El crate Rust
`polars` en crates.io va en **0.5x**:
- polars-Python actual: 1.43.2 (pypi)
- polars-Rust actual: 0.55.2 (crates.io, 2026-08-06)
- Crate Rust que envuelve Python 1.43.1: **0.54.4** (verificado en el
  `Cargo.lock` del tag `py-1.43.1`)
- En main de GitHub: 0.55.1

Se fue directo a **0.55.2** (la mas nueva de crates.io),
no al par exacto del Python. 0.55.2 = 0.55.0 + fixes (rate-limiter cloud IO,
bugfix de soundsness en rayon block_on, CSE). El changelog de 0.55.0 trae
mucho: OOC spilling a disco (naive), `POLARS_OOC_DISK_BUDGET_MB`, `concat`
optimizado, pre-particion de joins en datos hive, predicate pushdown mejorado,
`struct.drop()`, `is_sorted`, etc.

**Archivos tocados:**
- `zeruel/Cargo.toml` (workspace): version `0.47` → `0.55.2`, agregado feature `csv`
- `crates/zeruel-data/src/tasks.rs`: helper `pl_path()`, `read_parquet`, `sink_parquet_df`, `sink_csv_df`, `row_group_size`
- `crates/zeruel-cli/src/puro.rs`: helper `pl_path()`, `read`, `write_parquet`, `export_csv`, `row_group_size`
- `crates/zeruel-cli/src/dataset.rs`: desambiguacion `anyhow::Context::context` (generator)

**Breaking changes 0.47 → 0.55.2 (los que realmente aparecieron):**
1. `LazyFrame::scan_parquet` ahora toma `PlRefPath` (no `&PathBuf`):
   `impl From<&str> for PlRefPath` → helper `p.to_string_lossy().as_ref().into()`
2. `sink_parquet` / `sink_csv` eliminados. Hay un unico `sink`:
   ```rust
   lf.sink(
       SinkDestination::File { target: SinkTarget::Path(pl_path(out)) },
       FileWriteFormat::Parquet(std::sync::Arc::new(ParquetWriteOptions::default())),
       UnifiedSinkArgs::default(),
   )
   ```
   `FileWriteFormat::Csv(CsvWriterOptions::default())` para CSV. `SinkOptions`
   ya no existe; reemplazado por `UnifiedSinkArgs { mkdir, maintain_order,
   sync_on_close, cloud_options, sinked_paths_callback }`.
3. `.with_new_streaming(true)` → `.with_streaming(true)`
4. `JoinArgs`, `JoinCoalesce::CoalesceColumns`, `col("x").gt(lit(0.0))`,
   `.and()`, `.dt().date()`, `group_by`, `sort(["col"], SortMultipleOptions::default())`:
   SIN cambios. La API de expresiones es estable entre 0.47 y 0.54.
5. Feature `csv` en el crate `polars` ahora es obligatoria para que
   `CsvWriterOptions` exista (antes venia transitivamente).
6. **Conflicto de traits `PolarsContext` vs `anyhow::Context` (E0034)** — en
   0.55 el prelude trae `PolarsContext` con `context`/`with_context`, y con
   `use polars::prelude::*` en scope Rust no puede resolver el método.
   Fix: llamar `anyhow::Context::context(expr, msg)` /
   `anyhow::Context::with_context(c, || msg)` de forma explícita
   (pattern recomendado cuando dos traits comparten método). Tocado en
   `tasks.rs` (4x) y `dataset.rs` (5x).

**Buenas prácticas de Parquet aplicadas (0.55.2):**
- **Compresión ZSTD** — ya es default de `ParquetWriteOptions::default()` en
  0.55 (en 0.47 era Uncompressed; esto explica parte del gap histórico).
- **`row_group_size: Some(1_000_000)`** — row groups de 1M de filas permiten
  predicate pushdown: al leer con filtros se saltan row groups enteros
  usando las estadísticas. Aplicado en `sink_parquet_df` y `write_parquet`.
- **Stats por página** — default de `StatisticsOptions::PageLevel`, suficiente
  para que el planner aproveche los min/max en el pushdown.

**Números (release, 10M, seed 42):**
| | 0.47 (antes) | 0.54.4 | **0.55.2 (final)** | polars-python 1.43 | pandas 2.3 |
|---|---|---|---|---|---|
| nodo completo (con infra) | 14.84s | 6.51s | **6.18s** | — | — |
| nodo puro (streaming, sin checkpoint) | 14.00s | 6.23s | **5.56s** | — | — |
| polars-python directo | — | — | — | 6.39s | — |
| pandas directo | — | — | — | — | 36.96s |
| Peak RSS | 2952 MB | 2845 MB | **2813 MB** | 4702 MB | 4317 MB |

**Por task (nodo 0.55.2, release, 10M):**
| task | wall |
|---|---|
| validar | 0.86s |
| limpiar | 1.26s |
| join_customers | 1.26s |
| join_products | 1.64s |
| rollups | 0.55s |
| export_csv | 0.01s |

**Conclusión del upgrade:**
- Wall: 14.84s → **6.18s** (**2.4x más rápido**). El gap vs polars-python pasó
  de 2.2x a **-3%**: nodo **gana** en wall (6.18 vs 6.39s). El overhead de
  infra (checkpoint SQLite + métricas sysinfo) es 0.62s (11%) sobre `nodo
  puro` (5.56s).
- RAM peak: 2952 → **2813 MB** (5% menos, par vs 0.54.4).
- Integridad: 9.85M filas validadas, 1080 filas en rollups — **idéntico** a
  0.47, 0.54.4 y Python. Mismo dataset (seed 42).
- Release binary: 66.2 MB (antes 51.8 MB; +28% por crate mas grande).
- Build release: 10m 40s (full) / 3m 56s (incremental).
- `nodo run --fresh` (100k): 0.49s (antes 3.5s).

**Lo que nodo gana ahora (actualizado):**
- RAM peak: 2813 vs 4702 MB python (**40% menos**)
- Wall: **6.18 vs 6.39s python — nodo gana por 3%**
- Durabilidad, métricas por task, binary portable 66MB, arranque ~0.7s
- Compresión ZSTD por default en Parquet (ahora mismo que Python)
- Row groups de 1M para predicate pushdown

**Lo que nodo pierde (actualizado):**
- Nada vs Python: wall y RAM a favor (con el costo de 0.62s de infra)
- Binary 28% mas grande que antes del upgrade

**Dónde está la versión:**
- `zeruel/Cargo.toml` workspace root:
  `polars = { version = "0.55.2", features = ["lazy", "parquet", "csv", "dtype-categorical", "strings", "temporal", "streaming"] }`
- `nodo-data/Cargo.toml` y `nodo-cli/Cargo.toml` heredan con `workspace = true`.
- `feature streaming` es nuevo: antes `with_new_streaming(true)` venia por
  default; ahora hace falta la feature para que el flag exista.
  Solo se cambia el número en el workspace root.

**Archivos que usan polars directamente:**
- `crates/zeruel-data/src/tasks.rs` — helpers, JoinTask, ValidarTask, LimpiarTask, RollupsTask, ExportCsvTask
- `crates/zeruel-cli/src/puro.rs` — benchmark puro (con `with_new_streaming(true)`, `SinkTarget::Path`)
- `crates/zeruel-cli/src/pipeline.rs` — construcción del grafo (usa ValidarTask, lit(0.0), col, SortMultipleOptions)

**Breaking changes ya conocidos (de esta sesión, compilando contra 0.47):**
1. `JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::CoalesceColumns)` — verificar en 1.x
2. `.with_new_streaming(true)` — en 1.x puede cambiar
3. `LazyFrame::scan_parquet(path, ScanArgsParquet::default())` — verificar
4. `SinkTarget::Path`, `ParquetWriteOptions`, `SinkOptions` — verificar
5. `filter(a.and(b))` vs `filter(a & b)` — verificar
6. `SortMultipleOptions::default()` — verificar
7. `.dt().date()` — verificar
8. `.sort(["dia"], SortMultipleOptions::default())` — verificar

**Números base (polars 0.47, release, 10M, seed 42):**
| | wall | RAM peak |
|---|---|---|
| nodo completo | 14.84s | 2956 MB |
| nodo puro (streaming) | 14.00s | — |
| polars-python 1.43 | 6.39s | 4702 MB |
| pandas 2.3 | 36.96s | 4320 MB |

**Dataset 10M:** en `zeruel/data/input/`. Si se borra: `nodo gen --root data --n 10000000` (~60s).

**No hay git repo** — no hay que hacer commits.
---

## 5. Plan original (Fases 0-8)

El plan de trabajo tal como se escribio, con los checkboxes puestos al dia
el 2026-08-17 (ver seccion 2 para el detalle de que se corrigio).


Airflow reimaginado: Rust puro, single-binary portable, optimizado para big data
con poco hardware. Este documento es el plan de trabajo; los items se van
tachando a medida que avanzamos.

### Decisiones (cerradas)

- **Nombre:** `nodo`.
- **Estado:** SQLite-WAL embebido para metadata (runs, task states, timers).
  DuckDB se evaluo como feature opt-in; en B.2 se descarto a favor de polars-sql.
- **Tareas:** nativas (Rust) primero; sandbox WASM (Wasmtime) en fase 2.
- **UI web:** despues del MVP (CLI + logs primero).
- **Hito 1:** MVP de 3 tareas nativas (SQL -> Polars -> Parquet) corriendo
  end-to-end desde el binario.

### Fase 0 — Fundacion

- [x] Investigar estado del arte (Restate, Windmill, Bacalhau, Polars,
      DataFusion, Wasmtime, benchmarks)
- [x] Definir arquitectura (este documento)
- [x] Crear workspace Cargo con crates: nodo-core, nodo-sdk, nodo-data,
      nodo-cli
- [x] README del repo + estructura de directorios

### Fase 1 — SDK: como se declara un DAG

- [x] Definir API publica de nodo-sdk (trait `Task` + builder de `Dag`/`TaskDef`)
      (la macro `#[dag]` queda pendiente: hoy el DAG se construye por code)
- [x] Registro de tareas: nombre, parametros, dependencias
- [x] Compile-time: metadata del DAG generada por macro (Ronda B.3: `dag!` macro_rules)

### Fase 2 — Core: scheduler y estado

- [x] Metadata store en SQLite-WAL (DAGs, runs, task states, retries)
- [x] Ejecucion de un run: topological order, worker pool
- [x] Checkpoint / durable execution: re-levar desde el punto de corte
- [x] Retries con backoff (Ronda A.1, 2026-08-16)
- [x] CLI minima: `nodo run <dag>`, `nodo list`, `nodo status <run>`

### Fase 3 — Capa de datos

- [x] Integracion Arrow in-mem (zero-copy entre tareas)
- [x] Lectura/escritura Parquet
- [x] Tarea SQL sobre datasets (Ronda B.2: polars-sql, no DuckDB)
- [x] Tarea Polars para transformaciones dataframe
- [ ] Streaming out-of-core — **NO implementado** (ver N-18): el pipeline hace `collect()` en RAM. Estuvo tildado por error hasta 2026-08-17.

### Hito 1 — MVP end-to-end

- [x] Pipeline de ejemplo: leer Parquet -> validar -> limpiar -> joins
      -> rollups -> export
- [x] Correrlo con el binario en este hardware y medir tiempos
      (100k en 2.2s debug; 10M pendiente)
- [x] Verificar durabilidad: matar el proceso a mitad de run y re-levarlo
      (Fase 2.5, LOG 2026-08-15: kill a mitad → re-levar → 3 OK + 3 re-ejec)

### Benchmark — nodo vs Airflow (dataset de e-commerce)

**Dataset sintetico** (generador local, seed fija, reproducible):
10M transacciones + orders + customers + products. ~1.2GB en Parquet.
Variantes: 1M (sanity), 10M (base), 50M (stress).

**Pipeline (6 etapas, mismo grafo en ambos sistemas):**
1. Ingesta de zonas raw (4 datasets)
2. SQL: validar (descartar monto<=0, sin order_id)
3. Polars: limpiar (unificar categorias, imputar geo)
4. Polars: enriquecer (3 joins)
5. Rollups: agregados por hora / dia / categoria
6. Export: Parquet particionado por fecha + resumen

**Metricas:**
- [ ] Wall time total
- [ ] Time-to-first-task (arranque del proceso al primer work util)
- [ ] Peak RSS (RAM maxima)
- [ ] Footprint instalado (MB de dependencias)
- [ ] Pasos de setup (cero -> primer run)
- [ ] Durabilidad: kill a mitad de run + re-levar (re-ejecucion)

**Competidores (mismo pipeline):**
- [ ] Airflow standalone + SQLite, tareas pandas (uso tipico)
- [ ] Airflow standalone + SQLite, tareas polars-python (uso moderno)
- [ ] nodo (Rust nativo)

**Resultados (10M transacciones, 2026-08-15, Windows 11 / Docker Desktop, Python 3.12):**

| Metrica | nodo (Rust 0.55.2, release) | Python + pandas 2.3 | Python + polars 1.43 |
|---|---|---|---|
| Wall total | **6.18s** | 36.96s | 6.39s |
| Peak RSS | **2813 MB** | 4317 MB | 4702 MB |
| Time-to-first-task | **~0.7s** | — | — |
| Binary / instalacion | **66.2 MB**, 0 deps | Python 3.12 + pandas + pyarrow (~80MB) | Python 3.12 + polars (~60MB) |
| Pasos setup | 1 (cargo build o binario) | 3+ (venv, pip, deps) | 3+ (venv, pip, deps) |
| Durabilidad (kill a mitad) | [x] SQLite + re-ejecucion | [ ] no | [ ] no |
| Metricas por task | [x] RAM peak, CPU, wall, filas | [ ] no | [ ] no |

Nota: los números de Python miden el pipeline **directo** (misma logica, mismo dataset, mismo grafo).
El overhead de Airflow (scheduling, sandbox de subprocess por task, metadata en Postgres, retries)
no esta incluido; en un stack Airflow real el wall seria mayor.

Observaciones:
- nodo gana en TODO: wall (6.18 vs 6.39/36.96s), peak RSS (2813 vs 4317/4702),
  time-to-first-task, durabilidad, metricas por task.
- Verificado con `nodo puro` (mismo grafo, sin checkpoint SQLite, sin sampler sysinfo,
  streaming ON): wall = 5.56s, peak ~2800 MB. El overhead de infraestructura
  de nodo (checkpoint SQLite + muestreo) es 0.62s (11%).
- El upgrade de polars 0.47 → 0.55.2 (2026-08-15) cerro el gap historico de 2.2x:
  mejorias del engine (concat, pushdown, CSE) + ZSTD por default + row groups de 1M.
- Los 3 sistemas producen el mismo resultado: 9.85M filas validadas, 1080 filas en rollups.

- [x] Tabla comparativa con las 6 metricas x 3 sistemas
- [x] Escala 1M/10M/50M (Ronda C.4: se resolvio con tabla, no grafico)
- [x] Seccion de benchmark en README

### Higiene del sistema (por defecto, no opcional) — HECHO (3.8)

Problema que mata a Airflow con el tiempo: UI lento (parsea DAGs),
DB de metadata y logs sin tope. En nodo va acotado desde el dia 1:

- [x] Retencion de metadata: detail tasks 30d, runs 1a, agregados
      mensuales para siempre (tabla `rollups_monthly`, `Store::gc`)
- [x] GC: `nodo gc [--dry-run]` manual; automatica diaria va con `nodo serve`
      (Fase 4)
- [x] Logs: rotacion diaria en `state/logs/nodo.log.*`, retencion 14d
      (`tracing-appender` + `prune_old_logs`); el DB guarda solo el ultimo
      error por task (message), nunca el log completo
- [x] CLI paginada: `nodo list --limit --offset` (default 50)
- [x] `nodo status`: pulso de higiene al final (db, runs, task_runs, meses,
      ultima_gc, warning si db > 512MB)
- [ ] API/UI paginada (va con Fase 6, UI)

### Fase 3.5 — Metricas de recursos por tarea (por defecto)

Problema que Airflow NO resuelve por defecto: no mide RAM/CPU por DAG ni por
task, asi que los cuellos de botella de performance de un DAG se descubren
a ciegas. En nodo viene de fabrica:

- [x] Muestreo por tarea: wall (ya), **CPU consumido**, **RAM peak** (thread
      de muestreo a 100ms durante la ejecucion, max de RSS), filas (ya)
      (Fase 3.5, metrics.rs con sysinfo 0.39, LOG 2026-08-15)
- [x] Persistencia en `task_runs` (cpu_secs, rss_peak_mb) en SQLite
- [x] `nodo status --run X`: tabla por tarea ordenada por RAM peak
- [x] `nodo list`: totales de CPU/RAM por run
- [x] Historico consultable (deteccion de drift de consumo entre versiones
      de un DAG)

Dep: crate `process` (RSS + CPU time del propio proceso, cross-platform).

### Fase 4 — Scheduler global, cola y recursos (el corazon)

Problema que resuelve: si entran 30 DAGs en el mismo horario, como se
orquestan sin apilarse, sin pelearse y sin matar el hardware. En Airflow los
"pools" son slots arbitrarios sin conexion con RAM/CPU reales; en nodo el
scheduler decide con los recursos MEDIDOS de la Fase 3.5.

- [x] **Un solo scheduler** (`nodo serve`): 1 daemon, cola global de runs en
      SQLite (prioridad + FIFO), nunca 30 procesos peleando por el DB
- [x] **Admission control por recursos reales**: con RAM peak historico
      por DAG decide si el proximo run cabe en la maquina; margen 20% para no
      OOM. Big data con poca hardware, por diseño.
- [x] **Prioridades** por DAG + FIFO dentro de prioridad
- [x] **Anti-starvation (aging)**: prioridad efectiva = base + 1 por cada
      5min en cola — ningun run espera infinito
- [x] **max_wait por DAG** (Ronda C.2, 2026-08-16): al llegar el tope, el run salta al
      frente + aviso en `nodo status` (SLAs)
- [x] **Cola acotada** (Ronda C.3, 2026-08-16; el tope es global, no por prioridad): overflow se rechaza con
      log explicito, no se acumula en silencio
- [x] `nodo status` muestra cola con tiempo de espera y prioridad
      (semaforo: en cola / corriendo / bloqueado)
- [x] **Run conflict policy** por DAG+param (default: bloquear si ya hay run
      activo; rechazo al encolar si ya esta en cola) — sin colas infinitas
- [x] Schedules cron (Ronda A.2, 2026-08-16)
- [x] Backfill (Ronda C.1, 2026-08-16)
- [ ] Logs estructurados (tracing + JSON)
- [ ] OpenTelemetry (opt-in)

### Fase 5 — Sandbox WASM (fase 2 de tareas) — HECHO (2026-08-15, parcial)

- [x] Wasmtime embebido (47.0.3), modulo compilado 1 vez, instancia por corrida
- [x] Limits de memoria (ResourceLimiter, en BYTES) y CPU (fuel) por tarea
- [x] Toolchain: `cargo build --target wasm32-wasip1` (rustup), guest `no_std`
- [x] Ejemplos: `examples/wasm-hola` (OK), `examples/wasm-bucle` (fuel verificado)
- [x] CLI: `nodo wasm-run --wasm X --param Y --fuel N --mem-mb N`
- [ ] Tareas .wasm con WASI: whitelist de rutas y sockets (fase 2, no toca red hoy)
- [x] WasmTask integrable a un DAG (Ronda B.1: tarea .wasm en el grafo con deps+param+rows+retries, `nodo run --wasm`)

### Fase 6 — UI web embebida — HECHO (2026-08-15)

- [x] UI: lista de DAGs, runs en vivo, logs
- [x] Retry con un click
- [x] Sirve desde el propio binario (sin backend separado)

**Implementado:** `nodo web --state S --root R --port 8787` (dep `tiny_http`, UI embebida con
`include_str!` en `nodo-cli/src/web.rs`).

### Fase 8 — IA-friendly (el repo se explica a una IA con poco contexto)

- [x] `AGENTS.md` en la raiz (2026-08-15): mapa de workspace, comandos, invariantes,
      gotchas de Windows/CRLF, benchmarks de referencia. Cubre los crates inline
      (no uno por crate: el de raiz ya mapea cada crate)
- [x] `--json` en TODOS los comandos (2026-08-15): flag global en gen/run/list/gc/
      enqueue/status
- [x] `nodo mcp` por stdio (2026-08-15): list_dags, trigger_run, run_status,
      task_metrics, gc_status, queue (spec 2024-11-05)
- [ ] `docs/`: un archivo corto por concern, enlazado desde AGENTS.md
      (opcional; ya cubierto por LOG/ESTADO/PLAN + AGENTS.md)

### Fase 7 — Distribucion portable — HECHO (2026-08-15)

- [x] `nodo self-test`: verifica el binario end-to-end (gen -> run -> status ->
      gc) en un dir temporal. Sale 0 si todo funciona. (`--keep` para inspeccionar)
- [x] `nodo deploy --dest DIR [--data D] [--state S] [--docker] [--zip]`:
      carpeta portable (binario + wrappers + README <10min + opcionales)
- [x] Wrappers `run.ps1` / `run.sh` (once | self-test | cualquier comando)
- [x] Dockerfile (imagen runtime, debian-slim + binario) + `docker-run.sh`
- [x] Cross-builds: `.github/workflows/releases.yml` (tag `v*`): windows-msvc,
      linux-musl, macos aarch64/x86_64. Cada job corre `self-test` antes de
      empaquetar con `deploy`. Script local: `zeruel/build_releases.ps1`
- [x] README: seccion "Distribucion portable" + comandos self-test/deploy

### Arquitectura (resumen)

```
nodo (single binary estatico)
├── CLI / API HTTP
├── Scheduler (cron + backfill)        [Fase 4]
├── Metadata store (SQLite-WAL)        [Fase 2]
├── Worker pool + durable execution    [Fase 2]
├── Task Runtime
│   ├── Native (binario Rust)          [Fase 1-3]
│   └── WASM sandbox (Wasmtime)        [Fase 5]
└── Data layer
    ├── Arrow (zero-copy in-mem)       [Fase 3]
    ├── Parquet (on-disk)              [Fase 3]
    ├── Polars (dataframe)             [Fase 3]
    └── SQL (polars-sql)               [Fase 3 / B.2 — DuckDB descartado]
```

### Estructura del workspace

```
nodo/
  Cargo.toml          # workspace
  crates/
    nodo-sdk/         # macros y API que usa el desarrollador
    nodo-core/        # scheduler, DAG engine, metadata store
    nodo-data/        # Arrow/Parquet/Polars
    nodo-cli/         # binario y CLI
  examples/
    etl-basico/
  docs/
```