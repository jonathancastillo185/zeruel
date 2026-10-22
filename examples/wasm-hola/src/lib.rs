//! Guest WASM de ejemplo para el sandbox de zeruel.
//!
//! ABI (offsets fijos en la memoria lineal del modulo):
//!   - offset 16    : param (UTF-8, <=1024 bytes) — lo escribe el host
//!   - offset 8192  : rows i32 big-endian — lo escribe el guest
//!   - offset 8196  : mensaje (UTF-8, <=1020 bytes) — lo escribe el guest
//!   - export task_run() -> i32 : 0 = OK, <0 = error
//!
//! Compilar:
//!   cd examples/wasm-hola
//!   cargo build --release --target wasm32-wasip1
//!   # binario: target/wasm32-wasip1/release/wasm_hola.wasm

#![no_std]
#![no_main]

const PARAM_OFF: usize = 16;
const PARAM_CAP: usize = 1024;
const ROWS_OFF: usize = 8192;
const MSG_OFF: usize = 8196;
const MSG_CAP: usize = 1020;

#[allow(non_snake_case)]
static mut BUF: [u8; 65536] = [0u8; 65536];

#[no_mangle]
pub extern "C" fn buf_ptr() -> usize {
    unsafe { BUF.as_ptr() as usize }
}

#[no_mangle]
pub extern "C" fn task_run() -> i32 {
    unsafe {
        // Leer param (offset 16, hasta \0 o PARAM_CAP)
        let mut len = 0usize;
        while len < PARAM_CAP && BUF[PARAM_OFF + len] != 0 {
            len += 1;
        }
        let param = match core::str::from_utf8(&BUF[PARAM_OFF..PARAM_OFF + len]) {
            Ok(s) => s,
            Err(_) => return -1,
        };

        // Trabajo: filas = param.len() + conteo de palabras
        let words = param
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .count();
        let rows: i32 = (param.len() + words) as i32;

        // Escribir rows (offset 8192, big-endian)
        BUF[ROWS_OFF..ROWS_OFF + 4].copy_from_slice(&rows.to_be_bytes());

        // Escribir mensaje (offset 8196, simple concatenacion sin alloc)
        let msg_prefix = b"wasm-hola: ";
        let mut pos = 0usize;
        for &b in msg_prefix.iter() {
            if pos < MSG_CAP {
                BUF[MSG_OFF + pos] = b;
            }
            pos += 1;
        }
        for &b in param.as_bytes() {
            if pos < MSG_CAP {
                BUF[MSG_OFF + pos] = b;
            }
            pos += 1;
        }
        if pos < MSG_CAP {
            BUF[MSG_OFF + pos] = 0;
        }
    }
    0
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}