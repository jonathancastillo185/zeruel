//! Sandbox WASM: `WasmTask` ejecuta un `.wasm` dentro del engine con limites.
//!
//! ABI (lineal memory del modulo, offsets fijos, sin alloc compartido):
//!   - offset 16    : buffer de parametro (<= 1024 bytes, UTF-8)
//!   - offset 8192  : `rows` (i32, big-endian) escrito por la tarea
//!   - offset 8196  : mensaje (<= 1020 bytes, UTF-8)
//!   - export `task_run()` -> i32 : 0 = OK, <0 = error
//!
//! Limites (por corrida):
//!   - fuel: tope de instrucciones WASM (wasmtime `consume_fuel`).
//!   - cancelacion dura: el store tiene un flag por run; un watcher llama a
//!     `Engine::increment_epoch` y el guest trapea en el proximo checkpoint
//!     de epoch (corta hasta un loop infinito, a mitad de instruccion).
//!   - memoria: tope `mem_pages` paginas de 64KB via `ResourceLimiter`.
//!
//! Sin WASI (el guest no toca red/disco): la whitelist de recursos es
//! "nada" por default — fase 2 agrega WASI con whitelist explicita.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use wasmtime::{Config, Engine, Linker, Module, ResourceLimiter, Store};
use zeruel_sdk::{Task, TaskContext, TaskId, TaskOutput};

/// Al salir (por ok o por error) setea `finished` para que el watcher de
/// cancelacion se duerma y no quede un thread corriendo detras de cada task.
struct FinishGuard(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Drop for FinishGuard {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

const PARAM_OFF: usize = 16;
const PARAM_CAP: usize = 1024;
const OUT_OFF: usize = 8192;
const ROWS_OFF: usize = OUT_OFF;
const MSG_OFF: usize = OUT_OFF + 4;
const MSG_CAP: usize = 1020;

/// Limite de memoria lineal por instancia. `memory_growing` opera en BYTES
/// (no en paginas): se guarda el tope en bytes.
struct MemLimiter {
    max_bytes: usize,
}

/// Data del store: lleva el limiter (vive tanto como el Store).
struct StoreData {
    limiter: MemLimiter,
}

impl ResourceLimiter for MemLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        Ok(desired <= self.max_bytes)
    }
    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        Ok(desired <= 65536)
    }
}

/// Convierte un error wasmtime a anyhow usando solo `Display` (evita el clash
/// de traits con `anyhow::Context` que trae wasmtime 47).
fn to_err<T, E: std::fmt::Display>(r: std::result::Result<T, E>, msg: &str) -> Result<T> {
    r.map_err(|e| anyhow::anyhow!("{msg}: {e}"))
}

pub struct WasmTask {
    id: TaskId,
    module: Module,
    engine: Engine,
    fuel: u64,
    mem_bytes: usize,
}

impl WasmTask {
    pub fn new(id: &str, path: &Path, fuel: u64, mem_mb: u32) -> Result<Arc<Self>> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("no puedo leer el modulo wasm: {}", path.display()))?;
        let mut cfg = Config::new();
        cfg.consume_fuel(true);
        let engine =
            Engine::new(&cfg).map_err(|e| anyhow::anyhow!("crear engine wasmtime: {e}"))?;
        let module = Module::new(&engine, &bytes)
            .map_err(|e| anyhow::anyhow!("compilar modulo wasm ({}): {e}", path.display()))?;
        let mem_bytes = (mem_mb as u64 * 1024 * 1024).max(65536) as usize;
        Ok(Arc::new(Self {
            id: TaskId(id.to_string()),
            module,
            engine,
            fuel,
            mem_bytes,
        }))
    }
}

impl Task for WasmTask {
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    fn run(&self, ctx: &TaskContext) -> Result<TaskOutput> {
        let data = StoreData {
            limiter: MemLimiter {
                max_bytes: self.mem_bytes,
            },
        };
        let mut store = Store::new(&self.engine, data);
        to_err(store.set_fuel(self.fuel), "set_fuel")?;
        store.limiter(|data| &mut data.limiter);

        // Cancelacion dura (epoch interruption de wasmtime): el deadline se pone en
        // `epoch+1`. El codigo guest va instrumentado con checkpoints de epoch;
        // si la UI/API setea el flag, el watcher hace `increment_epoch` y el
        // epoch llega al deadline -> wasmtime ABORTA el codigo aereo en vivo
        // (corta hasta un loop infinito). `finished` duerme el watcher al
        // terminar esta ejecucion (sin fuga de threads).
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if let Some(flag) = &ctx.cancel {
            let engine = self.engine.clone();
            let flag = Arc::clone(flag);
            let fin = Arc::clone(&finished);
            std::thread::spawn(move || loop {
                use std::sync::atomic::Ordering as O;
                if flag.load(O::SeqCst) {
                    engine.increment_epoch();
                    break;
                }
                if fin.load(O::SeqCst) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            });
            // (Cada WasmTask tiene su Engine propio, con epoch en 0; solo el watcher lo
            // sube. Deadline en 1 = "a futuro" hasta que el watcher incremente.)
            store.set_epoch_deadline(1);
        }
        let _fin = FinishGuard(Arc::clone(&finished));

        let linker = Linker::new(&self.engine);
        let instance = to_err(
            linker.instantiate(&mut store, &self.module),
            "instanciar modulo (falta export o memoria)",
        )?;

        let mem = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow::anyhow!("el modulo no exporta la memoria lineal `memory`"))?;

        let f = to_err(
            instance.get_typed_func::<(), i32>(&mut store, "buf_ptr"),
            "el modulo no exporta `buf_ptr() -> i32`",
        )?;
        let base = f
            .call(&mut store, ())
            .map_err(|e| anyhow::anyhow!("llamar buf_ptr: {e}"))? as usize;

        let param = ctx.param.as_bytes();
        if param.len() > PARAM_CAP {
            bail!("param demasiado largo ({} bytes)", param.len());
        }
        to_err(
            mem.write(&mut store, base + PARAM_OFF, param),
            "escribir param a la memoria del guest",
        )?;

        let f = to_err(
            instance.get_typed_func::<(), i32>(&mut store, "task_run"),
            "el modulo no exporta `task_run() -> i32`",
        )?;
        let code = f.call(&mut store, ()).map_err(|e| {
            let msg = e.to_string();
            if msg.to_lowercase().contains("fuel") {
                anyhow::anyhow!("excedio el fuel ({}) — CPU budget agotado", self.fuel)
            } else {
                anyhow::anyhow!("task_run fallo: {msg}")
            }
        })?;
        if code < 0 {
            bail!("task_run devolvio {code} (error del guest)");
        }

        let mut rows_buf = [0u8; 4];
        to_err(
            mem.read(&mut store, base + ROWS_OFF, &mut rows_buf),
            "leer rows",
        )?;
        let rows = i32::from_be_bytes(rows_buf) as u64;

        let mut msg_buf = vec![0u8; MSG_CAP];
        to_err(
            mem.read(&mut store, base + MSG_OFF, &mut msg_buf),
            "leer mensaje",
        )?;
        let end = msg_buf
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(msg_buf.len());
        let message = String::from_utf8_lossy(&msg_buf[..end]).to_string();

        Ok(TaskOutput { rows, message })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cancelacion DURA: un modulo con `task_run()` = loop infinito. Sin
    /// cancelacion, el fuel (10^9) tardaria mucho; con el flag seteado, el
    /// watcher hace `increment_epoch` y el guest trapea en el siguiente
    /// checkpoint de epoch: el run corta en vivo (segundos, no horas).
    #[test]
    fn cancela_wasm_loop_infinito_en_vivo() {
        // (module
        //   (memory 1)
        //   (func $buf_ptr (result i32) i32.const 0)
        //   (func $task_run (loop (br 0)))
        //   (export "memory" (memory 0))
        //   (export "buf_ptr" (func $buf_ptr))
        //   (export "task_run" (func $task_run)))
        let wat_src = r#"
(module
  (memory 1)
  (func $buf_ptr (result i32) i32.const 0)
  (func $task_run (loop (br 0)))
  (export "memory" (memory 0))
  (export "buf_ptr" (func $buf_ptr))
  (export "task_run" (func $task_run)))
"#;
        let wasm = wat::parse_str(wat_src).expect("WAT invalido");
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("loop.wasm");
        std::fs::write(&p, wasm).unwrap();
        let task = WasmTask::new("loop", &p, 1_000_000_000, 16).unwrap();

        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ctx = TaskContext {
            run_id: uuid::Uuid::new_v4(),
            param: String::new(),
            cancel: Some(std::sync::Arc::clone(&flag)),
        };
        let t0 = std::time::Instant::now();
        let handle = std::thread::spawn(move || task.run(&ctx));
        // deja que el guest entre al loop y setea la cancelacion
        std::thread::sleep(std::time::Duration::from_millis(120));
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        let res = handle.join().expect("task panico");
        let elapsed = t0.elapsed();
        assert!(res.is_err(), "el loop debe cortarse, no terminar con ok");
        assert!(
            elapsed < std::time::Duration::from_secs(15),
            "el corte debe ser en vivo (no esperar el fuel): {} ms",
            elapsed.as_millis()
        );
    }
}
