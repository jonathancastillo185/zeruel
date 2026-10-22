# IRREGULARIDADES — zeruel

> **Nota para quien llega de afuera.** Este documento se escribió durante el
> desarrollo privado, cuando el proyecto se llamaba `nodo` y el workspace
> vivía en una subcarpeta. Los nombres y rutas (`nodo-core/...`,
> `zeruel/crates/...`, `BUGS.md`, `PLAN.md`) son históricos: hoy todo es
> `zeruel` y la estructura es la que describe el [README](../README.md).


Lista de todo lo que esta mal o no coincide, encontrada en la auditoria del
2026-08-17 sobre el commit `be4ec45`. Reemplaza a `BUGS.md` (los bugs abiertos
de ahi estan al final, en "Deuda heredada").

**Como usar esta lista (importante):**

- Cada item es autocontenido: dice DONDE esta, QUE pasa, COMO se arregla y
  COMO se verifica. No hace falta releer todo el proyecto para tomar uno.
- Se agarran de arriba para abajo: P0 primero. Un P0 rompe algo que la
  documentacion promete que funciona.
- **Al cerrar un item**: se cambia el titulo a `[RESUELTO fecha]`, se deja el
  texto (es historial) y se anota el commit. No se borra nada.
- **Antes de cada commit**: `cargo test --workspace` Y `cargo clippy --workspace
  --all-targets`. Hoy no hay CI, asi que no lo corre nadie mas que vos.
- Si aparece algo nuevo, se agrega aca con el mismo formato, sin que se pida.

## Estado

**53 relevados · 51 resueltos · 2 abiertos**, mas N-54 a N-59 encontrados al
arreglar y al empaquetar (todos resueltos). Los dos que quedan —N-19 y N-21— no
son cambios de codigo: necesitan re-medir con el entorno a mano y actualizar
numeros en la documentacion.

> Ultima revision contra el codigo: **2026-08-22**. Si algo de aca no coincide
> con lo que hace el repositorio, gana el repositorio y hay que corregir esto.

| Chequeo | Al relevar (2026-08-17) | Ahora (2026-08-18) |
|---|---|---|
| `cargo test --workspace` | 28 verdes | **84 verdes** |
| `cargo clippy --workspace --all-targets` | 7 warnings (2 marcados como resueltos sin estarlo) | **0** |
| `cargo fmt --all --check` | 164 diffs en 17 archivos | **limpio** |
| Modulos sin un solo test | store, scheduler, web, mcp, deploy, nodo-data | **store, web, ops y nodo-data cubiertos** |
| CI | no hay | sigue sin haber; queda `zeruel/check.ps1` para correr lo mismo a mano |

Lo verificado en vivo, no solo por test:

- **Cancelar desde `nodo web` un run que corre en `nodo serve`** (el flujo real
  de N-02): el log de serve dice *"cancelacion pedida desde otro proceso"*, no
  arranca la task siguiente, y el run queda `Cancelled` en la CLI y en la UI.
- **El XSS de N-32**: el payload `<img src=x onerror=...>` se ve como texto en
  la tabla, no se ejecuta, y la consola del navegador queda sin errores.
- **`status --run <uuid>`** devuelve el run pedido y no el mas reciente.
- **Las metricas de tareas cortas** ya no son 0 (48 MB / 50 MB en un run chico).

---

## P0 — Bugs funcionales (rompen algo documentado)

### N-01 [RESUELTO 2026-08-18] — Un run `Cancelled` se muestra como `Running` en todos lados
- **Donde:** `zeruel/crates/zeruel-core/src/store.rs:454` (`recent_runs`).
- **Que pasa:** el match del estado es `"Success" => Success, "Failed" => Failed,
  _ => Running`. `Cancelled` (y `Pending`) caen en el `_` y salen como `Running`.
- **Impacto:** es el bug que hace invisible a la feature estrella. `nodo list`,
  `nodo status`, `/api/runs`, `/api/run/<id>` y la UI muestran "Running" para
  siempre a un run cancelado. La pill `p-c` (gris, "Cancelled") del CSS de la UI
  es codigo muerto: nunca se puede renderizar. Peor: el boton "✕ Cancelar" solo
  aparece si `state === 'Running'`, asi que sigue apareciendo en un run ya
  cancelado.
- **Arreglo:** agregar `"Cancelled" => RunState::Cancelled` al match. Aprovechar
  y mapear el resto explicitamente, con un fallback razonado en vez de `_`.
- **Verificar:** cancelar un run y confirmar que `nodo list` dice `Cancelled`.

### N-02 [RESUELTO 2026-08-18] — La cancelacion no funciona entre procesos (el caso real de uso)
- **Donde:** `store.rs` (campo `cancels: Mutex<HashMap<..>>`), `web.rs:31`
  (`web::run` nunca ejecuta un DAG), `scheduler.rs:116` (el run corre aca).
- **Que pasa:** la señal de cancelacion es un `AtomicBool` **en memoria de la
  instancia de `Store`**. `nodo web` solo encola (`POST /api/run` → `enqueue`);
  el run lo ejecuta el proceso `nodo serve`. Cuando la UI cancela, setea el flag
  en el `Store` del proceso *web*, que nadie lee, y hace
  `UPDATE runs SET state='Cancelled'`. El engine de `serve` sigue corriendo y al
  terminar hace `set_run_state(Success)`, **pisando** el Cancelled.
- **Impacto:** "cancelacion dura desde la UI" no funciona en el unico flujo
  donde se usa (web + serve). Los 2 tests que la cubren corren engine y cancel
  en el mismo proceso, por eso pasan.
- **Arreglo:** persistir la intencion de cancelar en la DB (columna
  `cancel_requested` en `runs`, o clave `cancel:<run_id>` en `meta`) y que el
  engine la consulte en el loop principal y en el watcher de wasm (cada N ms),
  ademas del flag en memoria. Y que `set_run_state` no pise un `Cancelled`.
- **Verificar:** `nodo serve` en una consola, `nodo web` en otra, encolar,
  cancelar desde la UI, y confirmar que el run termina `Cancelled` de verdad.

### N-03 [RESUELTO 2026-08-18] — `nodo status --run <uuid>` devuelve el header de OTRO run
- **Donde:** `zeruel/crates/zeruel-cli/src/main.rs:662`.
- **Que pasa:** las tasks se leen del `id` pedido, pero el bloque `run` del JSON
  sale de `store.recent_runs(1, 0)` — o sea, del run **mas reciente**, no del
  pedido. Ademas el `.unwrap()` de esa linea **panica** si la tabla `runs` esta
  vacia y se paso un UUID explicito.
- **Impacto:** verificado en vivo: pedi el run de `--param P1` y el JSON
  devolvio el id, param y estado del run de `P3`. Cualquier script o IA que
  consuma `--json status --run X` recibe datos cruzados.
- **Arreglo:** agregar `Store::run_by_id(uuid) -> Result<Option<Run>>` y usarlo;
  error claro (no panic) si no existe.
- **Verificar:** dos runs con params distintos, pedir el viejo, confirmar que el
  `run.id` devuelto es el pedido.

### N-04 [RESUELTO 2026-08-18] — MCP `list_dags` esta hardcodeado
- **Donde:** `zeruel/crates/zeruel-cli/src/mcp.rs:143`.
- **Que pasa:** `"list_dags" => json!(["ecommerce-etl"])`. Es una regresion del
  commit `c843c45`, que creo el registry multi-DAG (`dags.rs`) y actualizo web y
  scheduler, pero se olvido del MCP.
- **Impacto:** cualquier DAG nuevo que se agregue a `dags::all()` es invisible
  para las IAs que usan el MCP. Rompe justo la razon de existir de `dags.rs`.
- **Arreglo:** `json!(crate::dags::names())`.
- **Verificar:** `echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_dags"}}' | nodo mcp --state state`

### N-05 [RESUELTO 2026-08-18] — El `backfill` del MCP se saltea el ultimo dia
- **Donde:** `zeruel/crates/zeruel-cli/src/mcp.rs:217`.
- **Que pasa:** `cur = cur.succ_opt().unwrap_or(hasta); if cur == hasta { break; }`
  corta **antes** de encolar `hasta`. Un backfill 08-10..08-13 encola 3 dias, no 4.
- **Impacto:** falta un dia de datos, en silencio (`"ok": true`). La version CLI
  (`main.rs`, `cmd_backfill`) esta bien: son dos implementaciones distintas del
  mismo comando y solo una es correcta.
- **Arreglo:** que el MCP llame a la misma funcion que la CLI. Extraer
  `backfill(store, dag, desde, hasta, prio) -> Result<BackfillReport>` a un modulo
  compartido y que CLI y MCP la usen. Aplica igual a `sla` y `queue_set_max`
  (ver N-12).
- **Verificar:** un test de rango de 4 dias que cuente 4 encolados.

### N-06 [RESUELTO 2026-08-18] — `recover_running` ignora el `param`: cruza runs de params distintos
- **Donde:** `store.rs:387` + `engine.rs:119`.
- **Que pasa:** `recover_running(dag)` devuelve **cualquier** run del DAG en
  estado `Running`, sin mirar el `param`. Si quedo un run colgado de `--param A`
  (kill, crash, o el bug N-08) y arrancas `nodo run --param B`, el engine
  **retoma el run_id de A** y escribe los task_runs de B adentro. El estado
  inicial de las tasks si mira el param (`task_initial_state`), asi que queda un
  run con `param=A` en `runs` y checkpoints de B en `task_runs`.
- **Impacto:** metadata corrupta y silenciosa. Es exactamente el escenario que
  el MANUAL documenta en "Run queda en Running tras un kill".
- **Arreglo:** `recover_running(dag, param)` con `AND param = ?2`.
- **Verificar:** test que deje un run Running de `p1` y corra `p2`, y chequee
  que se crea un run nuevo.

### N-07 [RESUELTO 2026-08-18] — Re-correr el mismo DAG+param escribe metricas en cero
- **Donde:** `engine.rs` (bloque "durabilidad", `set_task_state(..., 0,
  "reutilizado", 0, 0.0)`).
- **Que pasa:** cuando una task se reutiliza de un run anterior, se inserta una
  fila nueva en `task_runs` con `rows=0`, `ram_peak_mb=0`, `cpu_secs=0` y
  mensaje "reutilizado".
- **Impacto:** verificado en vivo: el 2do `run` del mismo param devuelve
  `{"filas":0,"skipped":6}` y deja 6 filas en cero. Esas filas entran en
  `task_stats()` (`AVG(rows)`, `AVG(cpu_secs)`) que es lo que muestra la tab
  **Metricas** de la UI: los promedios se hunden a medida que se re-corre.
  Ademas rompe el metodo de verificacion que documenta AGENTS ("comparar
  `filas` contra la ultima corrida"): da 0 salvo que uses `--fresh`.
- **Arreglo:** copiar `rows`/`ram`/`cpu` de la fila reutilizada en vez de
  escribir ceros, **o** marcar la fila (`state='Skipped'` o `reused=1`) y
  excluirla de `task_stats()`. Preferible lo segundo: no inventa mediciones.
- **Verificar:** correr 2 veces el mismo param y mirar `nodo status`.

### N-08 [RESUELTO 2026-08-18] — Un run que falla mata el daemon y deja el item de cola trabado
- **Donde:** `zeruel/crates/zeruel-cli/src/scheduler.rs:116`.
- **Que pasa:** `let report = run_dag(...)?;` — el `?` propaga hacia afuera del
  loop y **termina `nodo serve`**. Como `pick_next` ya marco el item como
  `state='running'`, ese item queda asi para siempre: no lo toma nadie mas
  (`pick_next` filtra por `state='queued'`) y no lo limpia nadie
  (`complete_queue` nunca se llama).
- **Impacto:** un DAG con un error de I/O (falta un parquet, disco lleno) tira
  abajo el scheduler entero y deja basura permanente en la cola. Notar que un
  run que falla *sus tasks* no entra por aca (devuelve `Ok(report)` con
  `success:false`); esto es para errores del engine.
- **Arreglo:** envolver en `match`, loguear el error, `complete_queue(q.id)` (o
  devolver el item a `queued` con un contador de intentos) y seguir el loop.
  Agregar ademas una limpieza al arrancar: items en `running` sin run activo
  vuelven a `queued`.
- **Verificar:** borrar `data/input/transactions.parquet` y encolar; `serve`
  tiene que seguir vivo.

### N-09 [RESUELTO 2026-08-18] — La GC automatica de `serve` no corre nunca en la practica
- **Donde:** `scheduler.rs:75` y `:90`.
- **Que pasa:** `ultimo_gc` se inicializa con `Utc::now()` y vive solo en
  memoria. La condicion es `now - ultimo_gc > 86_400`, o sea que `serve` tiene
  que estar **24h corridas arriba** para que corra una sola vez. Si se reinicia
  antes (deploy, reboot, Ctrl-C), el contador vuelve a cero.
- **Impacto:** "GC automatica diaria dentro de serve" es una de las promesas
  centrales de la seccion Higiene y no se cumple. La DB crece sin tope.
- **Arreglo:** leer el ultimo GC de `meta` (la clave `last_gc` ya existe y ya la
  escribe `Store::gc`) en vez de una variable local.
- **Verificar:** setear `last_gc` a hace 2 dias, levantar `serve` y ver la GC en
  el log al primer tick.

### N-10 [RESUELTO 2026-08-18] — Las metricas por task miden el proceso entero, y en 0 si la task es rapida
- **Donde:** `zeruel/crates/zeruel-core/src/metrics.rs:59`.
- **Que pasa:** dos problemas en el mismo sampler:
  1. El loop hace `sleep(100ms)` **antes** del primer sample. Cualquier task de
     menos de 100ms registra `ram_peak_mb=0` y `cpu_secs=0` (verificado en vivo:
     las 6 tasks de un run chico dieron 0).
  2. Mide RSS y CPU del **proceso completo**, no de la task. Con
     `--workers > 1` todas las tasks concurrentes reportan el mismo pico, y como
     el RSS no baja cuando el allocator retiene memoria, toda task posterior al
     pico hereda un numero inflado.
- **Impacto:** "metricas de RAM/CPU por tarea, de fabrica" es el diferencial que
  el README vende contra Airflow, y el numero no significa lo que dice. Ademas
  el admission control del scheduler decide con este dato
  (`dag_ram_peak` = `MAX(ram_peak_mb)`), y la conclusion del README de que
  "el cuello de botella es join_products" sale de una metrica monotona.
- **Arreglo:** minimo, tomar un sample inmediato antes del sleep y bajar el
  intervalo a ~20ms. Para lo demas: documentar honestamente que es una metrica
  **por proceso** (valida solo con `--workers 1`) o medir el delta contra un
  baseline tomado al arrancar la task.
- **Verificar:** un run chico no deberia dar todo 0.

### N-11 [RESUELTO 2026-08-18] — `nodo deploy --docker` en Windows genera un Dockerfile roto
- **Donde:** `zeruel/crates/zeruel-cli/src/deploy.rs:110` y `:262`.
- **Que pasa:** `bin_name` es `nodo.exe` en Windows, y el Dockerfile hace
  `COPY nodo.exe /usr/local/bin/nodo` sobre `debian:bookworm-slim`: la imagen
  copia un binario PE de Windows a Linux. Ademas la linea 110 escribe **un solo**
  wrapper (`run.ps1` en Windows, `run.sh` en Unix), pero el README que genera y
  el mensaje final ("primer paso: ... o bash run.sh self-test") ofrecen los dos.
- **Impacto:** el paquete portable generado desde la maquina de desarrollo (que
  es Windows) no sirve para Docker ni para Linux. Se ve en
  `zeruel/dist/nodo-portable/`: hay `run.ps1` y `docker-run.sh`, pero no `run.sh`.
- **Arreglo:** escribir **siempre** los dos wrappers; que el Dockerfile use
  siempre el nombre Unix (`nodo`) y que `--docker` avise si el binario que se
  esta empaquetando no es ELF.
- **Verificar:** `nodo deploy --dest x --docker` y revisar que `run.sh` exista y
  que el Dockerfile no mencione `.exe`.

### N-12 [RESUELTO 2026-08-18] — `sla --max-wait 0` significa cosas distintas segun por donde entres
- **Donde:** `main.rs` (`cmd_sla`) vs `mcp.rs:234` vs `docs/MANUAL.md:178`.
- **Que pasa:** por MCP, `max_wait=0` **borra** el tope (guarda `""`). Por CLI,
  guarda `"0"`, que significa "tope de 0 segundos" → **todo item en cola viola
  el SLA al instante**. El MANUAL documenta la semantica del MCP como si fuera
  la de la CLI ("Borrar tope (0)").
- **Impacto:** seguir el manual llena la tab Alertas de falsos positivos.
- **Arreglo:** una sola semantica (sugerido: 0 = borrar, con `meta_delete(key)`
  de verdad) implementada una sola vez y usada por CLI y MCP (ver N-05).
- **Verificar:** `nodo sla --dag X --max-wait 0` + encolar + `nodo status`: no
  tiene que alertar.

### N-13 [RESUELTO 2026-08-18] — Los runs viejos "no existen" para la UI y el MCP
- **Donde:** `web.rs:179` (`run_detail`, 200), `web.rs:276` (`alerts`, 2000),
  `mcp.rs` (`run_status` y `task_metrics`, 200).
- **Que pasa:** para buscar un run por id se traen los N runs mas recientes y se
  filtra en memoria con un `find` lineal. Pasado ese N, el run devuelve
  `{"error": "run no encontrado"}` aunque este en la DB.
- **Impacto:** con retencion de 1 año y varios runs por dia, el detalle de un
  run de la semana pasada deja de abrir. Ademas es O(n) por request, cada 2s.
- **Arreglo:** el mismo `Store::run_by_id` de N-03, con `WHERE id = ?1`.
- **Verificar:** generar >200 runs y abrir el detalle del primero.

### N-14 [RESUELTO 2026-08-18] — Una dependencia inexistente se reporta como "ciclo" [RESUELTO 2026-08-17]
- **Donde:** `zeruel/crates/zeruel-sdk/src/lib.rs:155` (`topological_order`).
- **Que pasa:** el grado de entrada se calcula contando las deps **declaradas**
  (`for _d in &t.deps`), no las que existen en el grafo. Si una task depende de
  un id que no existe, su grado nunca baja a 0, el `out` queda corto y se
  devuelve `DagError::Cycle`. Por eso `DagError::UnknownDep` — que esta definida
  con un mensaje perfecto — **nunca se construye en todo el proyecto**. Lo mismo
  pasa con ids duplicados: colapsan en el HashMap y salen como "ciclo".
- **Impacto:** un typo en una dep da un error que manda a buscar un ciclo que no
  existe. Con la macro `dag!` (strings sueltos, sin chequeo del compilador) es
  el error mas probable de todos.
- **Arreglo:** validar primero que toda dep exista (y que no haya ids repetidos)
  y devolver `UnknownDep`/un error nuevo; recien despues correr Kahn.
- **Verificar:** test con `"b" => ["no-existe"]` que espere `UnknownDep`.

---

## P1 — El codigo no hace lo que dice la documentacion

Estos no rompen nada por si mismos, pero hacen que cualquiera (humano o IA) que
lea los `.md` tome decisiones sobre una realidad que no existe. Son los mas
baratos de arreglar y los que mas confusion ahorran.

### N-15 [RESUELTO 2026-08-18] — El workflow de CI que la doc da por vivo esta borrado
- **Donde:** `AGENTS.md`, `zeruel/README.md`, `PLAN.md`, `ESTADO.md` (ahora
  fusionados) citan `.github/workflows/releases.yml`.
- **Que pasa:** el commit `adadcb9` ("Desactivar workflow de releases", por falta
  de minutos de CI) lo borro. **No existe `.github/` en el repo.**
- **Impacto:** no hay CI de ningun tipo: nadie corre `cargo test`, `clippy` ni
  `fmt` salvo a mano. Explica casi toda la deriva de esta lista. Y no hay builds
  cross-platform, asi que los `.zip` para linux/macos que promete el README no
  se generan.
- **Arreglo:** decidir una de dos y que la doc diga eso: (a) restaurar el
  workflow, aunque sea solo `test + clippy + fmt` en push (gratis en repos
  publicos), o (b) documentar que el release se arma a mano con
  `zeruel/build_releases.ps1`. **Ya corregido en `AGENTS.md` en esta pasada.**

### N-16 [RESUELTO 2026-08-18] — "DAG runner con Rayon": no hay Rayon
- **Donde:** decia `AGENTS.md`; el codigo es `engine.rs` (`std::thread::spawn`).
- **Que pasa:** rayon no es dependencia de ningun crate. El engine lanza un
  thread del sistema por task y hace `join()` de todo el lote antes de buscar
  mas trabajo: no es un worker pool, es ejecucion por tandas.
- **Impacto:** una IA que lea "Rayon" va a buscar un thread pool que no existe.
  Y el modelo real tiene un costo: una task rapida encolada detras de una lenta
  en el mismo lote espera igual. **Ya corregido en `AGENTS.md`.**

### N-17 [RESUELTO 2026-08-18] — "UI embebida con `include_str!`": es un `const` inline
- **Donde:** `web.rs:2` (comentario) y `AGENTS.md`; el codigo es `web.rs:369`
  (`const UI_HTML: &str = r##"..."##`). No hay ningun `include_str!` en el repo.
- **Impacto:** son ~280 lineas de HTML+CSS+JS dentro de un `.rs`, sin resaltado
  ni linter, que es parte de por que se colaron los bugs B-06/B-07/B-08.
- **Arreglo:** mover la UI a `nodo-cli/src/ui.html` y usar `include_str!` de
  verdad (que es lo que la doc ya dice). Es un cambio chico y mejora mucho el
  trabajo sobre la UI. Corregir el comentario de `web.rs:2` igual.

### N-18 [RESUELTO 2026-08-18] — "Streaming out-of-core (datos mayores que RAM)" no esta implementado [RESUELTO 2026-08-17]
- **Donde:** estaba tildado `[x]` en `PLAN.md` (Fase 3) y se afirma en
  `zeruel/README.md`. El codigo es `nodo-data/src/tasks.rs:69`.
- **Que pasa:** `sink_parquet_lf` hace `lf.collect()` — materializa **todo el
  DataFrame en RAM** — y recien despues `df.lazy().sink(...)`. `with_streaming(true)`
  aparece **una sola vez en el proyecto**, en `puro.rs:12`, que es el benchmark
  sin orquestacion. El pipeline real nunca activa streaming.
- **Impacto:** explica los 14 GB a 50M filas (el README lo admite a medias:
  "sin streaming out-of-core"), contradiciendo el check de PLAN y el README.
  Un dataset mayor que la RAM no corre, que es justo el caso de uso que vende.
- **Arreglo:** o implementarlo (sacar el `collect()` y sinkear el LazyFrame
  directo, midiendo el impacto en wall) o destildarlo. **Destildado en
  `HISTORIAL.md`**; la implementacion queda como trabajo pendiente.

### N-19 — El benchmark contra Python no es comparable
- **Donde:** las tablas de `zeruel/README.md`, `PLAN.md` y `ESTADO.md`; los
  scripts `bench_direct.py`, `bench_mem.py`.
- **Que pasa:** tres problemas de metodo, ninguno declarado en la doc:
  1. **Distinto sistema.** `bench_direct.py` usa rutas `/opt/airflow/bench_data`:
     corrio dentro del contenedor Linux de Airflow. nodo se midio nativo en
     Windows 11. Distinto SO, distinto filesystem, contenedor vs nativo.
  2. **Distinta medicion de RAM.** `bench_mem.py` suma el `VmRSS` de **todos**
     los procesos del contenedor; el de nodo es el working set de un proceso.
  3. **`nodo puro` no es "el mismo pipeline sin orquestacion".** `puro.rs` usa
     `with_streaming(true)` y el pipeline real no (N-18). Los 0.62s de
     diferencia se atribuyen a "checkpoint SQLite + sampler" cuando adentro hay
     una diferencia de motor.
- **Impacto:** "nodo gana en TODO" no esta sostenido por la medicion que se hizo.
  Y los scripts estan en `.gitignore` (N-50), asi que no se puede rehacer.
- **Arreglo:** re-medir los tres en el mismo SO y con el mismo metodo, o
  reescribir las tablas aclarando las condiciones de cada columna. Hasta
  entonces las tablas quedan como estan pero con la nota de metodologia.

### N-20 [RESUELTO 2026-08-18] — Invariante falso: "el `.init()` de tracing devuelve un Guard, NO TOCAR"
- **Donde:** lo afirmaban `AGENTS.md` y `BUGS.md` (B-04) sobre `main.rs:291,298`.
- **Que pasa:** es incorrecto. `SubscriberInitExt::init()` devuelve `()`; clippy
  lo dice textual: *"this let-binding has unit value"*. El `let _ =` no sostiene
  nada, y sacarlo **no apaga el logging**. La confusion viene de
  `tracing_appender::non_blocking()`, que si devuelve un `WorkerGuard` — pero
  este codigo no lo usa: escribe con `RollingFileAppender` sincronico.
- **Impacto:** es el peor tipo de item de esta lista: una regla inventada,
  escrita en mayusculas en dos documentos, que se va a seguir arrastrando y va a
  frenar a cualquiera que quiera limpiar ese warning.
- **Arreglo:** sacar los dos `let _ =`. Los 2 warnings de clippy se van solos.
  **Ya sacada la regla falsa de la doc en esta pasada.**
- **Verificar:** sacarlos, correr `nodo run` y confirmar que
  `state/logs/nodo.log.*` se sigue escribiendo.

### N-21 — El tamaño del binario esta mal en todos lados, y distinto en cada uno
- **Que pasa:** el binario release real es **96.5 MB**
  (`zeruel/target/release/nodo.exe`). La doc dice 66 MB (README, PLAN), 79 MB
  (ESTADO y el README del portable) y 82 MB (ESTADO, seccion WASM). El de
  `dist/nodo-portable/nodo.exe` (79 MB) es de un build viejo.
- **Impacto:** "66 MB, 0 deps" es uno de los argumentos de venta de la tabla
  comparativa.
- **Arreglo:** un solo numero, medido, y de paso investigar por que crecio 45%.
  **Corregido en `AGENTS.md`; falta en `zeruel/README.md`** (ver N-25).

### N-22 [RESUELTO 2026-08-18] — "Dos params del mismo DAG pueden correr en paralelo": no pueden
- **Donde:** lo decia el invariante 4 de `AGENTS.md`. El codigo es
  `scheduler.rs:116`.
- **Que pasa:** `serve` llama a `run_dag(...)` **de forma sincronica** dentro del
  loop. Hasta que ese run no termina, no se vuelve a llamar a `pick_next`. Nunca
  hay dos runs a la vez, ni del mismo DAG ni de DAGs distintos. `--workers` es
  paralelismo *dentro* de un run, no entre runs.
- **Impacto:** toda la maquinaria de conflicto por DAG+param, aging y admission
  control esta escrita para un escenario multi-run que no puede darse. El
  contador `bloqueados_conflicto` solo puede activarse con runs lanzados por
  fuera (`nodo run` a mano).
- **Arreglo:** decidir el alcance. Si `serve` tiene que ser mono-run, decirlo en
  la doc (**ya corregido en `AGENTS.md`**) y simplificar. Si tiene que ser
  multi-run, lanzar cada run en su thread con un semaforo de N slots — ahi si
  toda la logica de la cola pasa a tener sentido.

### N-23 [RESUELTO 2026-08-18] — El admission control no hace la cuenta que documenta
- **Donde:** `store.rs` (`pick_next`), doc en `AGENTS.md` y `PLAN.md`.
- **Que pasa:** la doc dice "ram_peak historico * 1.2 > RAM disponible → se
  salta". El codigo hace `ram_limite = disponible - disponible*20/100` y compara
  `peak > ram_limite`, o sea `peak > 0.8 * disponible`. La doc equivale a
  `peak > 0.833 * disponible`.
- **Impacto:** chico en la practica, pero es la clase de detalle que se copia de
  la doc a un test y despues no cierra.
- **Arreglo:** elegir una y que la otra la refleje. Y agregar un test de
  `pick_next` (hoy no hay ninguno, ver N-35).

### N-24 [RESUELTO 2026-08-18] — El MANUAL tiene datos que no coinciden con la CLI
- **Donde:** `zeruel/docs/MANUAL.md`.
- **Que pasa:** al menos cuatro:
  - "procesa cola + cron cada 30s" (linea 155): el default de `--tick-ms` es
    **1000 ms**.
  - `nodo sla --max-wait 0` documentado como "borrar tope": ver N-12.
  - `nodo self-test` figura como "(sin flags)": tiene `--n`, `--workers`, `--keep`.
  - La tabla de flags no incluye `nodo web`, `nodo wasm-run`, `nodo puro`, ni el
    `--root` de `nodo mcp`.
- **Arreglo:** pasar la tabla de flags contra `nodo --help` de cada subcomando.

### N-25 [RESUELTO 2026-08-18] — `zeruel/README.md`: enlaces rotos y numeros viejos
- **Que pasa:** apunta a `PLAN.md` dos veces (linea 6 y 76), que ya no existe
  tras la unificacion; repite el binario de 66 MB (N-21); describe el workflow de
  releases como activo (N-15); y afirma streaming out-of-core (N-18).
- **Arreglo:** los enlaces ya se corrigieron en esta pasada. Los numeros de
  benchmark **no los toque**: hay que re-medirlos (N-19) antes de reescribirlos.

### N-26 [RESUELTO 2026-08-18] — `PLAN.md` tenia sin tildar la mitad de lo que ya estaba hecho
- **Que pasa:** figuraban como pendientes `[ ]` "Retries con backoff",
  "Schedules cron", "Backfill", "max_wait por DAG (SLA)" y "Cola acotada", todos
  cerrados en las Rondas A y C. Y la seccion Arquitectura seguia listando
  "DuckDB/SQL (opt-in)" cuando en B.2 se decidio polars-sql.
- **Impacto:** el documento que decia "que falta" mandaba a rehacer lo hecho.
- **Arreglo:** **ya resuelto**: el plan se fusiono en `HISTORIAL.md` con el
  estado real de cada item.

### N-27 [RESUELTO 2026-08-18] — `PLAN-PENDIENTES.md` daba por pendiente algo ya entregado
- **Que pasa:** D.3 "Landing por DAG" figuraba `PENDIENTE (opcional)` pero esta
  implementada y commiteada (`b00ad5e`, hash routing `#/dag/<nombre>`), como
  reconocia `sesion.md`. Dos documentos de la misma carpeta se contradecian.
- **Arreglo:** **ya resuelto en la unificacion.**

### N-28 [RESUELTO 2026-08-18] — El generador de dataset no genera lo que dice
- **Donde:** `zeruel/crates/zeruel-cli/src/dataset.rs`.
- **Que pasa:** tres cosas chicas:
  - El docstring dice "Genera los 4 datasets"; genera **3** (transactions,
    customers, products). `PLAN.md` mencionaba un dataset `orders` que no existe.
  - El comentario "en bloques para no explotar RAM" es enganoso: los bloques se
    acumulan en un `Vec<DataFrame>` y se `vstack`ean todos, mas un `df.clone()`
    al escribir. Todo termina en RAM igual.
  - `n_customers` es fijo en 200.000 aunque `--n` sea chico: `gen --n 2000`
    reporta "207000 filas" (verificado).
- **Arreglo:** corregir los comentarios; escalar `n_customers`/`n_products`
  con `--n`; sinkear cada bloque en vez de acumular.

### N-29 [RESUELTO 2026-08-18] — `--root` es un flag que no se usa en `web` y en `mcp`
- **Donde:** `web.rs:29` (`_root: PathBuf`), `mcp.rs:14` (`_root: PathBuf`).
- **Que pasa:** ambos comandos aceptan y documentan `--root` y lo ignoran.
- **Impacto:** confunde sobre lo que hacen: la UI **no** ejecuta pipelines (solo
  encola), justamente porque no tiene el `DataEnv` — y eso es lo que hace que la
  cancelacion no cruce procesos (N-02).
- **Arreglo:** o sacar el flag, o usarlo para que la web pueda ejecutar en
  proceso. Cual de las dos depende de como se resuelva N-02.

### N-30 [RESUELTO 2026-08-18] — MANUAL.html y MANUAL.pdf estan desactualizados respecto del .md
- **Donde:** `zeruel/docs/`.
- **Que pasa:** `MANUAL.md` se edito 14:28 del 16-08; el `.html` (10:56) y el
  `.pdf` (10:57) son de esa mañana. Tres copias del mismo contenido, mantenidas
  a mano, y dos ya divergieron. El PDF ademas son 287 KB de binario versionado.
- **Arreglo:** dejar el `.md` como fuente unica y generar html/pdf a demanda (o
  no versionarlos). Si se quieren versionar, regenerarlos en el mismo commit.

---

## P2 — Robustez, seguridad y calidad

### N-31 [RESUELTO 2026-08-18] — La UI escucha en 0.0.0.0 sin autenticacion
- **Donde:** `web.rs:30` (`format!("0.0.0.0:{port}")`).
- **Que pasa:** el server queda expuesto en **todas** las interfaces de red, sin
  auth, sin CORS, sin rate limit — y con endpoints `POST` que encolan y cancelan
  runs. La doc ademas explica como publicarlo por Tailscale.
- **Impacto:** cualquiera en la red puede encolar y cancelar trabajo.
- **Arreglo:** bindear `127.0.0.1` por defecto y pedir `--bind 0.0.0.0` explicito
  (con aviso en el log). Un token compartido por header seria el minimo para
  exponerlo.

### N-32 [RESUELTO 2026-08-18] — XSS almacenado en la UI (verificado)
- **Donde:** todo el JS de `UI_HTML` interpola con `innerHTML` sin escapar:
  `param`, `dag`, `message`, `motivo`.
- **Que pasa:** `POST /api/run` acepta cualquier `param`. Lo verifique en vivo:
  encole `param = <img src=x onerror=alert(1)>` y `/api/queue` lo devuelve tal
  cual; la tabla de cola lo inyecta con `<td>${c.param}</td>`.
- **Impacto:** con N-31 (sin auth, expuesto), cualquiera puede dejar JS
  ejecutandose en el navegador de quien abra la UI.
- **Arreglo:** una funcion `esc(s)` que escape `& < > " '` aplicada a todo valor
  que venga del store, o pasar a `textContent`. Y validar `param`/`dag` en el
  server (largo maximo, charset).

### N-33 [RESUELTO 2026-08-18] — `Task::deps()` es una API muerta que enseña algo falso [RESUELTO 2026-08-17]
- **Donde:** `nodo-sdk/src/lib.rs` (trait `Task`), y las 8 implementaciones.
- **Que pasa:** **todas** devuelven `vec![]`, incluidas las del pipeline real.
  Las dependencias reales viven en `TaskDef.deps` y son las unicas que el engine
  mira (`ready_slot`). El metodo del trait no se llama nunca.
- **Impacto:** quien implemente una `Task` nueva va a poner sus deps ahi y no
  van a tener ningun efecto: el DAG se va a ejecutar en el orden equivocado sin
  ningun error.
- **Arreglo:** sacar `deps()` del trait (el grafo es del `Dag`, no de la task).
  Es un cambio mecanico en 8 archivos y saca una trampa del SDK.

### N-34 [RESUELTO 2026-08-18] — `ExportCsvTask` ignora el nombre que le pasan [RESUELTO 2026-08-17]
- **Donde:** `nodo-data/src/tasks.rs:193`.
- **Que pasa:** `fn id()` devuelve `TaskId("export_csv")` fijo, a diferencia de
  `PolarsTask` y `JoinTask` que usan su `name`. `ExportCsvTask::new` ni siquiera
  recibe un nombre.
- **Impacto:** dos exports en un mismo DAG colisionan silenciosamente en el
  HashMap de tasks (el segundo pisa al primero) y el chequeo de "task sin
  implementacion" del engine no lo detecta.
- **Arreglo:** agregar el parametro `name` como en las otras dos.

### N-35 [RESUELTO 2026-08-18] — Los modulos con mas bugs son los unicos sin un solo test
- **Que pasa:** los 28 tests estan en `cron.rs` (9), `engine.rs` (7),
  `nodo-sdk` (6), `dags.rs` (3), `sql.rs` (2) y `wasm.rs` (1). Tienen **cero**:
  `store.rs` (GC, enqueue, pick_next, aging, admission, recover — 792 lineas),
  `scheduler.rs`, `web.rs`, `mcp.rs`, `deploy.rs` y **todo el crate
  `nodo-data`** (las tareas ETL que son el producto).
- **Impacto:** no es casualidad que N-01, N-05, N-06, N-08, N-09 y N-13 esten
  todos en esos archivos. `store.rs` en particular se presta a tests: es
  SQLite y ya existe `Store::open_in_memory()`.
- **Arreglo (orden sugerido):** 1) `store`: enqueue/conflicto/cola llena, gc con
  fechas viejas, pick_next con prioridad+aging, recover_running. 2) `nodo-data`:
  un parquet chico por tarea, comparando filas. 3) `mcp`: un test por tool.
- **Nota:** cada test que se agregue deberia cerrar un item de esta lista, no
  ser un test suelto.

### N-36 [RESUELTO 2026-08-18] — La UI hace 8 requests cada 2 segundos, algunos carisimos
- **Donde:** `UI_HTML` (`setInterval(refresh,2000)`), `web.rs:127` y `:276`.
- **Que pasa:** cada refresh pide 7 endpoints + el grafo + (si hay un run
  seleccionado) el detalle. `/api/dags` trae **10.000 runs** a memoria y los
  agrega en Rust en vez de un `GROUP BY`; `/api/alerts` trae 2.000. `tiny_http`
  atiende de a uno.
- **Impacto:** con historial real la UI se arrastra sola, que es exactamente el
  problema de Airflow que el proyecto dice resolver ("UI lento" en la seccion
  Higiene).
- **Arreglo:** `GROUP BY dag` en SQL para `/api/dags`; `WHERE started_at >= ?`
  para los fails de 24h; subir el intervalo a 5s; no re-pedir el grafo en cada
  refresh (cambia solo si cambia el DAG).

### N-37 [RESUELTO 2026-08-18] — El parseo de query string no decodifica URL
- **Donde:** `web.rs:107` (`query_param`).
- **Que pasa:** compara el valor crudo. `?dag=mi%20dag` busca literalmente
  `mi%20dag`. La UI ya usa `encodeURIComponent` al armar el hash, asi que un DAG
  con espacio o acento no abre su landing.
- **Extra:** `split()` (`web.rs:99`) devuelve una tupla de 3 donde el primer
  campo es **siempre** `String::new()` y se descarta en el caller.
- **Arreglo:** un decode minimo (`%XX` y `+`) y que `split` devuelva 2 campos.

### N-38 [RESUELTO 2026-08-18] — `Store::open_in_memory` no corre las migraciones
- **Donde:** `store.rs` (`open_in_memory` vs `open`).
- **Que pasa:** `open` hace `execute_batch(SCHEMA)` + `migrate()`;
  `open_in_memory` solo el `SCHEMA`. Hoy no rompe (el SCHEMA ya trae las
  columnas), pero significa que **los tests corren contra un esquema distinto
  del de produccion**: la proxima migracion no va a estar cubierta.
- **Arreglo:** llamar a `migrate()` en los dos, o mejor, un solo
  `fn init(conn)` que use ambos caminos.

### N-39 [RESUELTO 2026-08-18] — El tamaño de la DB ignora el WAL
- **Donde:** `store.rs` (`db_size`).
- **Que pasa:** mide solo `nodo.db`. En modo WAL, buena parte de los datos
  recientes vive en `nodo.db-wal` (en `state-web/` el `-wal` es varias veces el
  `.db`). El warning de "DB > 512MB" y el KPI de higiene subestiman.
- **Arreglo:** sumar `-wal` y `-shm` si existen.

### N-40 [RESUELTO 2026-08-18] — La tab "Metricas" esta vacia por diseño durante el primer año
- **Donde:** `store.rs` (`gc`), tab Metricas de la UI.
- **Que pasa:** `rollups_monthly` **solo** se llena consolidando runs de mas de
  **1 año** antes de borrarlos. Antes de eso la tabla esta vacia, y "Volumen
  mensual por DAG" no muestra nada (la UI ya lo aclara en el texto vacio).
- **Impacto:** una tab entera de la UI, mas el KPI "meses agregados" y la
  columna de rollups del landing por DAG, no muestran nada util hasta 2027.
- **Arreglo:** consolidar el mes **cerrado** (no el de hace un año) en cada GC,
  con UPSERT idempotente. Los agregados quedan disponibles desde el primer mes y
  la purga sigue funcionando igual.

### N-41 [RESUELTO 2026-08-18] — Items de cola en estado `running` sin dueño ni timeout
- **Donde:** `store.rs` (`pick_next` marca `running`, `complete_queue` borra).
- **Que pasa:** no hay nada que devuelva a `queued` un item cuyo proceso murio
  (ver N-08). Tampoco un timeout, ni un campo que diga quien lo tomo.
- **Arreglo:** guardar `started_at`/`owner` en la fila de cola y al arrancar
  `serve` recuperar los `running` sin run activo.

### N-42 [RESUELTO 2026-08-18] — El MCP viola JSON-RPC en las notificaciones
- **Donde:** `mcp.rs` (loop principal).
- **Que pasa:** responde a **todo** mensaje entrante, incluidos los que no traen
  `id` (notificaciones). Un cliente MCP manda `notifications/initialized` justo
  despues del handshake y recibe `{"id":null,"error":{"code":-32601}}`, que
  segun el spec no debe existir. Ademas los errores de una tool se devuelven
  como `result` con el texto adentro, sin `isError: true`.
- **Arreglo:** si no hay `id`, procesar y **no** responder. Agregar
  `"isError": true` en el content cuando la tool falla.

### N-43 [RESUELTO 2026-08-18] — El proyecto no esta formateado y no hay nada que lo verifique
- **Que pasa:** `cargo fmt --all --check` reporta **164 diffs en 17 archivos**
  (los peores: `engine.rs` 30, `main.rs` 29, `store.rs` 19). Hay bloques con
  indentacion visiblemente rota (`metrics.rs:53-58`). No hay `rustfmt.toml`.
- **Arreglo:** un `cargo fmt --all` en un commit **solo de formato** (para no
  ensuciar los diffs de los arreglos), y despues siempre antes de commitear.

### N-44 [RESUELTO 2026-08-18] — Clippy no esta limpio, y dos warnings figuraban como resueltos
- **Que pasa:** 7 warnings vivos: `ptr_arg` x2 (`puro.rs:11` y `:15`),
  `let_unit_value` x2 (`main.rs:291`, `:298` — ver N-20), `too_many_arguments`
  x2 (`engine.rs:253`, `store.rs:342`) y `doc_lazy_continuation`
  (`store.rs:494`). Los dos de `puro.rs` estaban anotados como **RESUELTOS** en
  `BUGS.md` (B-00c) y nunca se arreglaron.
- **Arreglo:** los 4 primeros son triviales. Los `too_many_arguments` son la
  deuda B-01 de siempre (structs `RetryCtx` y `TaskStateRecord`).
- **Leccion:** no marcar RESUELTO sin correr el comando que lo prueba.

### N-45 [RESUELTO 2026-08-18] — `puro.rs` tiene 15 `.unwrap()`
- **Que pasa:** cualquier problema (falta un parquet, disco lleno) es un panic
  con backtrace en vez de un mensaje.
- **Impacto:** menor — es codigo de benchmark — pero es el comando que se corre
  para producir los numeros que despues van al README.
- **Arreglo:** `-> anyhow::Result<()>` y `?`, como el resto del proyecto.

### N-46 [RESUELTO 2026-08-18] — Dependencias declaradas que nadie usa, y metadata placeholder
- **Donde:** `zeruel/Cargo.toml` y los `Cargo.toml` de los crates.
- **Que pasa:** `toml = "1.1"` (workspace) no se usa en ningun `.rs`; `tokio`
  es dev-dependency de `nodo-core` y no se usa; `thiserror` esta en las deps de
  `nodo-core` y `nodo-data` pero solo se usa en `nodo-sdk`. Ademas
  `repository = "https://example.com/nodo"` es un placeholder, y hay dos lineas
  de comentario casi identicas sobre la version de polars.
- **Impacto:** tiempo de build y superficie de deps sin razon.
- **Arreglo:** sacarlas; poner la URL real del repo o borrar el campo.

### N-47 [RESUELTO 2026-08-18] — El engine spawnea un thread del SO por task, por lote
- **Donde:** `engine.rs` (loop principal).
- **Que pasa:** por cada tanda se hace `thread::spawn` por task y despues
  `join()` de todas **en orden de spawn** antes de volver a buscar trabajo. Una
  task corta detras de una larga en el mismo lote no libera su slot. Ademas la
  rama de `sleep(25ms)` con "trabajo en vuelo" es inalcanzable (al llegar ahi ya
  se hizo join de todo). Y `ready_slot` hace un `find` lineal sobre el orden
  topologico por cada task en cada vuelta.
- **Arreglo:** si se toca N-22, resolverlo junto: un pool con canal de
  resultados en vez de spawn+join por lote.

---

## P3 — Higiene del repositorio

### N-48 [RESUELTO 2026-08-18] — Hay un log de runtime versionado en git
- **Que pasa:** `zeruel/state-web/logs/nodo.log.2026-08-16` esta **trackeado**.
  `.gitignore` cubre `state/` y `zeruel/state/` pero no `state-web/` ni
  `data-web/`, que son las carpetas que se usan para probar la UI.
- **Arreglo:** `git rm --cached` del log y agregar `zeruel/state-web/` y
  `zeruel/data-web/` (o el patron `state*/` y `data*/`) al `.gitignore`.

### N-49 [RESUELTO 2026-08-18] — Dos JSON de basura versionados en la raiz
- **Que pasa:** `_chromo.json` (59 bytes: `{"errors":[{"detail":"crate
  'chromoxide' does not exist"}]}`) y `_chromo2.json` (9 KB de un listado de
  crates.io) son respuestas de la API de crates.io de alguna busqueda, sin
  relacion con el proyecto. Estan commiteados.
- **Arreglo:** borrarlos.

### N-50 [RESUELTO 2026-08-18] — Los scripts del benchmark estan excluidos del repo
- **Que pasa:** `.gitignore` tiene `bench_*.py` y `bench_*.ps1`, asi que
  `bench_direct.py`, `bench_direct_pandas.py`, `bench_mem.py` y
  `bench_sampler.ps1` existen en disco pero **no en el repo**.
- **Impacto:** las tablas comparativas del README son la parte mas destacada del
  proyecto y nadie puede reproducirlas (ni verificar N-19).
- **Arreglo:** versionarlos en `bench/` con un README de como se corrieron
  (SO, contenedor, versiones, comando exacto).

### N-51 [RESUELTO 2026-08-18] — Nueve `.md` con el mismo contenido repetido
- **Que pasa:** 1844 lineas en 7 archivos en la raiz mas README y MANUAL. Cada
  hecho estaba escrito 3 o 4 veces (la cancelacion dura aparecia en AGENTS,
  ESTADO, LOG, PLAN-PENDIENTES y sesion), y por eso divergieron (N-26, N-27).
- **Arreglo:** **ya resuelto en esta pasada.** Quedan 4:
  `AGENTS.md` (guia + estado + pendientes), `IRREGULARIDADES.md` (este),
  `HISTORIAL.md` (historia + fases cerradas + bugs resueltos),
  `zeruel/README.md` y `zeruel/docs/MANUAL.md`.

### N-52 [NO APLICA] — Un commit con mensaje de descarte, citado como referencia
- **Que pasa:** el commit `8bbc98a` tiene como mensaje
  `[descartado: eran 477 caracteres, ya se escribio]` — es un artefacto de la
  herramienta, no un mensaje. `ESTADO.md` lo citaba como la referencia de la
  Ronda A.2 (cron).
- **Arreglo:** no se reescribe la historia por esto; queda anotado para que no
  confunda. Cuidar el `-F .commitmsg.txt` de ahora en mas.

### N-53 [RESUELTO 2026-08-18] — Numeros de la doc que se contradicen entre si
- **Que pasa:** ademas del binario (N-21): la suite figura como "25 tests" en
  ESTADO/LOG/sesion y son **28** (el mensaje del ultimo commit si dice 28);
  AGENTS.md figura como "153 lineas" en LOG y "~200" en PLAN-PENDIENTES cuando
  tenia **215**. Son sintomas del mismo problema que N-51.
- **Arreglo:** no repetir numeros derivables en varios lados. **Ya corregido**
  al unificar.

---

## Deuda heredada de BUGS.md (sigue abierta)

- **B-01 — `too_many_arguments`** (prioridad baja):
  `Engine::ejecutar_con_retries` (`engine.rs:253`, 8 args) y
  `Store::set_task_state` (`store.rs:342`, 8 args). Structs `RetryCtx` y
  `TaskStateRecord`. Sigue apareciendo en clippy (ver N-44).
- **B-02 — `doc_lazy_continuation`** (`store.rs:494`): la linea "Con `dry_run`
  solo cuenta lo que borraria." necesita 4 espacios de indentacion. Sigue viva.
- **B-04 — el `let _ = ...init()`**: la regla era **incorrecta**, ver N-20. Se
  puede sacar.
- Los bugs B-00, B-00b, B-00c, B-03, B-05 a B-10 estan cerrados y su detalle
  quedo en `HISTORIAL.md`. Ojo que **B-00c figura como resuelto y no lo esta del
  todo** (los `&PathBuf` de `puro.rs`, N-44).

---

## Lo que esta bien (para no "arreglar" de mas)

Vale la pena decirlo, porque hay decisiones solidas que no conviene tocar al
ir corrigiendo esta lista:

- **La suite pasa entera** (28/28) y los tests que existen son buenos: los de
  cron cubren casos de borde reales, y `cancela_wasm_loop_infinito_en_vivo`
  prueba de verdad que el corte es por epoch y no por fuel.
- **La cancelacion de WASM por epoch interruption esta bien implementada** y es
  la API correcta para un store sincronico en wasmtime 47. El problema es de
  plomeria entre procesos (N-02), no del sandbox.
- **El esquema de SQLite es razonable**: `ON CONFLICT` en `task_runs`, PK
  compuesta, migracion aditiva, WAL. La durabilidad por checkpoint funciona
  (verificado: el 2do run saltea 6/6 tasks).
- **La decision polars-sql sobre DuckDB** esta bien argumentada y documentada.
- **El registry `dags.rs`** es la direccion correcta para multi-DAG; solo falta
  terminar de cablearlo (N-04).
- **El uso de `dag!`** para declarar el grafo es legible y el pipeline real ya
  lo usa.

---

## Encontrados al arreglar (2026-08-18)

### N-54 [RESUELTO 2026-08-18] — `target/` crecio a 57 GB y lleno el disco
- **Que paso:** en medio de los arreglos, `cargo` empezo a fallar con
  "Espacio en disco insuficiente" (os error 112). El disco estaba al **100%**
  (0 bytes libres de 931 GB) y `zeruel/target` pesaba **57 GB**, de los cuales
  48 GB eran `target/debug/deps`: cargo no borra los artefactos de hashes
  viejos, y meses de `cargo test` los fueron acumulando.
- **Arreglo:** se borro `target/debug` (51 GB liberados; `target/release` quedo
  intacto, con el binario del benchmark). Es regenerable: un `cargo build`
  lo rehace.
- **Para que no vuelva a pasar:** `cargo clean` cada tanto, o
  `cargo clean --profile dev` despues de una tanda larga de tests. Si el disco
  vuelve a llenarse, este es el primer lugar donde mirar.

### N-55 [RESUELTO 2026-08-18] — `rollups()` fallaba siempre que hubiera datos
- **Donde:** `store.rs`, `RollupRow.wall_secs`.
- **Que pasa:** la columna es `REAL` en el esquema SQLite pero el struct la leia
  como `i64`. Cualquier fila en `rollups_monthly` hacia que la query devolviera
  `Invalid column type Real at index: 5`. Nunca se noto porque los dos callers
  hacen `.unwrap_or_default()`: la tab **Metricas** de la UI mostraba "sin
  agregados" en vez de un error.
- **Como aparecio:** lo destapo el test de N-40, que es el primero que llena la
  tabla de rollups. Es el tipo de bug que solo aparece cuando el codigo se
  ejercita de verdad — hasta ahora ese camino no lo corria nadie.
- **Arreglo:** `wall_secs: f64`.

---

## Lo que queda abierto

> Revisado contra el codigo el 2026-08-22. Tres items que esta seccion daba por
> abiertos ya estaban resueltos y se movieron abajo: un documento de estado que
> miente es peor que no tenerlo.

**N-19 — el benchmark contra Python no es comparable.** El unico que no se
puede cerrar desde el codigo: hay que volver a medir los tres sistemas en
igualdad de condiciones. Los scripts estan versionados en `bench/` con un README
que explica que esta mal y como re-medir. Ojo que ademas **los numeros de nodo
cambiaron** con el streaming de N-18: la tabla del README quedo vieja.

**N-21 — el tamaño del binario.** Medido de nuevo el 2026-08-22: **97,5 MB** en
release con todos los features. Sigue abierto solo como tarea de documentacion:
hay que actualizar el numero en `zeruel/README.md` y `AGENTS.md`, que todavia
dicen 66/79/82 MB. El desglose por feature esta en `docs/COSTOS.md`, que si esta
al dia.

### Decisiones de alcance, no bugs

- **N-10 — las metricas eran del proceso, no de la tarea.** Con un proceso por
  run el proceso **es** el run, asi que el numero paso a ser exacto sin que
  hubiera que medir por thread. Lo que sigue siendo cierto es que con
  `--workers > 1` las tareas de un mismo run comparten proceso; para atribuir
  RAM por tarea haria falta un proceso por task, que no vale lo que cuesta.

---

## Resueltas despues del relevamiento

**N-22 / N-47 — `serve` corria un run por vez.** Resuelto con el scheduler
multiproceso: `ServeArgs.paralelo` fija cuantos subprocesos hay a la vez, y
`recoger()` los cosecha sin bloquear. La cola ya tenia prioridad, aging,
conflicto por DAG+param y admission control por RAM; lo que faltaba era
ejecutar, y ahora se lanza el binario del pipeline en vez de correrlo adentro
del loop.

**N-57 — `busy_timeout` sin configurar.** Resuelto en `Store::open`, junto a los
pragmas de WAL. Era prerrequisito del scheduler multiproceso: con N pipelines
escribiendo checkpoints sobre la misma base, sin timeout SQLite devolvia
"database is locked" al instante en vez de esperar.

**N-58 — no habia ningun manejo de señales** (encontrado al empaquetar el
2026-08-22). `nodo serve` no escuchaba SIGTERM, asi que un `systemctl stop`
mataba el proceso de golpe y los pipelines hijos quedaban huerfanos: en Linux
los adopta init y siguen escribiendo en la base que el scheduler ya no
administra, con sus runs clavados en `Running` hasta el arranque siguiente.
Resuelto con apagado ordenado —deja de lanzar, espera hasta `--gracia-secs`, y
lo que corta vuelve a la cola— mas servicio nativo en Windows y unidad de
systemd en Linux. Ver `packaging/README.md`.

**N-59 — el techo duro de memoria solo existia en Windows** (misma fecha).
`limite.rs` implementaba Job Objects y devolvia `false` en el resto, asi que en
Linux —el sistema donde esto se va a desplegar— la garantia dura no existia.
Resuelto con `setrlimit(RLIMIT_DATA)`, que desde Linux 4.7 cubre heap y mapeos
anonimos sin contar los respaldados por archivo: exactamente la distincion que
hace falta para que un parquet mapeado no consuma del techo. El descarte de
`RLIMIT_AS` y de cgroups esta razonado en el modulo.

---

### N-56 [RESUELTO 2026-08-18] — Un `--state` sin permiso hacia panic, no error
- **Donde:** `main.rs`, `init_logs`.
- **Que pasa:** `RollingFileAppender::new` **panica** si no puede crear el
  directorio de logs. Cualquier comando con un `--state` en una ruta sin
  permiso de escritura moria con un backtrace de `tracing-appender` antes de
  que la CLI pudiera decir nada. Apareció al probar los backtraces despues del
  cambio de perfil de compilacion (N-54).
- **Arreglo:** usar `RollingFileAppender::builder().build()`, que devuelve
  `Result`. Si no se puede loguear a archivo, avisa con un `warn` y sigue con
  la salida por pantalla — que no se pueda escribir un log no es motivo para
  no ejecutar el comando.
- **Verificar:** `nodo status --state /ruta/sin/permiso` tiene que dar
  "no puedo crear ...", no un panic.

### N-57 [RESUELTO 2026-08-19] — `busy_timeout` sin configurar: "database is locked"
- **Donde:** `zeruel/crates/zeruel-core/src/store.rs`, en `Store::open` (donde se
  setean los pragmas `journal_mode = WAL` y `synchronous = NORMAL`).
- **Que pasa:** falta `busy_timeout`. En SQLite el default es **0**: si una
  conexion encuentra la base bloqueada por otra, devuelve `SQLITE_BUSY` al
  instante en vez de esperar y reintentar.
- **Impacto:** hoy es latente pero real — `nodo web` y `nodo serve` ya son dos
  procesos escribiendo sobre la misma base, y con poca carga las colisiones no
  se ven. Pasa a ser un bloqueador con la arquitectura del ROADMAP, donde el
  scheduler lanza N pipelines como subprocesos y todos escriben checkpoints.
  El sintoma seria un run que falla con "database is locked" sin motivo aparente.
- **Arreglo:** `conn.busy_timeout(Duration::from_secs(5))` (o el pragma
  equivalente) al abrir, y revisar que las escrituras sean cortas — nunca tener
  una transaccion abierta mientras corre una tarea.
- **Verificar:** un test que abra N conexiones desde threads y escriba
  checkpoints en paralelo sobre la misma base de disco, sin que ninguna falle.
- **Prioridad:** P1, y es prerrequisito de la Fase 2 del ROADMAP.

## Encontrados al preparar la publicación (2026-10-22)

### N-60 [P0] — Una variable de entorno pisa a `--config` sin avisar
- **Donde:** `crates/zeruel-cli/src/main.rs` (resolución de `state` y
  `pipelines`: flag > entorno > archivo).
- **Que pasa:** con `ZERUEL_STATE` / `ZERUEL_PIPELINES` definidas a nivel de
  máquina (las deja una instalación), `zeruel serve --config otro.toml` ignora
  las rutas del archivo y abre el estado de la otra instalación. El log dice
  *"configuracion de otro.toml"* mientras usa las rutas del entorno.
- **Impacto:** una segunda instalación de prueba en la misma máquina opera
  sobre la base de producción: aplica su propio `timeout_secs` a las corridas
  en curso (las marca fallidas) y publica su configuración efectiva en la
  pestaña Configuración de la otra. Pasó de verdad al armar la demo.
- **Arreglo:** cuando se pasa `--config` explícito, el archivo tiene que ganarle
  al entorno, o como mínimo avisar en el log y en stderr qué variable pisó qué
  ajuste. Además el log de arranque tiene que decir de dónde salió cada ruta.
- **Verificar:** test que define `ZERUEL_STATE`, pasa `--config` con otro
  `state` y comprueba cuál se usa y qué se loguea.

### N-61 [P1] — El pico de RAM por tarea de `zeruel run` no es confiable
- **Donde:** sampler de `zeruel-core/src/metrics.rs` usado por el pipeline de
  ejemplo (`zeruel run`).
- **Que pasa:** con el mismo dataset de 10 M de filas, `join_products` reportó
  44 MB en una corrida y 88 MB en otra; `join_customers` 42 MB en una y
  1.070 MB en otra. Medido desde afuera (`PeakWorkingSet64` del proceso), el
  pico es estable: 1.774 / 1.781 / 1.775 MB. El CPU por tarea también da 0 en
  la mayoría de las tareas.
- **Impacto:** el número de 833 MB que figuraba en README/COSTOS/MANUAL salió
  de esta métrica y estaba subestimado más de 2x. Ya se corrigió a 1,8 GB
  medido desde afuera.
- **Arreglo:** revisar el sampler (frecuencia, si mide el proceso o el árbol, y
  si se reinicia entre tareas). Contrastar contra el pico del proceso.

### N-62 [P2] — La pestaña Sistema muestra 0 MB para el scheduler instalado como servicio
- **Donde:** `crates/zeruel-cli/src/salud.rs`.
- **Que pasa:** el panel corre con su propia cuenta de servicio y no tiene
  permiso para leer la memoria del proceso del scheduler (ni de las corridas),
  así que los muestra con 0 MB. Medido con `Get-Process`: 30,5 MB.
- **Arreglo:** que cada proceso publique su propio consumo en la base (como ya
  publica su configuración efectiva) y que el panel lo lea de ahí; o mostrar
  "sin permiso para medir" en vez de 0.
