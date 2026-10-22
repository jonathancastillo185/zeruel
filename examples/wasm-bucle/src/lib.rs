#![no_std]
#![no_main]

#[no_mangle]
pub extern "C" fn buf_ptr() -> usize {
    0usize
}

/// Bucle infinito a proposito: si el fuel no funciona, esto cuelga a zeruel para siempre.
#[no_mangle]
pub extern "C" fn task_run() -> i32 {
    let mut x: u64 = 0;
    loop {
        x = x.wrapping_add(1);
        core::hint::black_box(x);
    }
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}