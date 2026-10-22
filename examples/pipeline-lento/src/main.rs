//! Pipeline de prueba que tarda a proposito: sirve para ver el paralelismo, la
//! cancelacion y el timeout del scheduler, que con un pipeline de 400 ms no se
//! alcanzan a observar.
//!
//! Cada tarea duerme en tramos cortos y mira `ctx.cancel` entre tramo y tramo,
//! que es como tiene que estar escrita cualquier tarea larga para poder cortarse.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use zeruel::prelude::*;

struct Dormir {
    id: &'static str,
    segundos: u64,
}

impl Task for Dormir {
    fn id(&self) -> TaskId {
        TaskId(self.id.into())
    }

    fn run(&self, ctx: &TaskContext) -> TaskResult {
        let t0 = Instant::now();
        while t0.elapsed().as_secs() < self.segundos {
            if ctx.cancel.as_ref().is_some_and(|c| c.load(Ordering::Relaxed)) {
                return Err(zeruel::falla(format!(
                    "cancelado a los {:.1}s",
                    t0.elapsed().as_secs_f64()
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(TaskOutput {
            rows: self.segundos,
            message: format!("dormi {}s", self.segundos),
        })
    }
}

/// Reserva memoria a lo bruto: sirve para comprobar que el techo corta de
/// verdad, y que el mensaje dice que fue por memoria.
struct Tragon {
    mb_por_paso: usize,
    pasos: usize,
}

impl Task for Tragon {
    fn id(&self) -> TaskId {
        TaskId("tragon".into())
    }

    fn run(&self, ctx: &TaskContext) -> TaskResult {
        let mut bloques: Vec<Vec<u8>> = Vec::new();
        for i in 0..self.pasos {
            if ctx.cancel.as_ref().is_some_and(|c| c.load(Ordering::Relaxed)) {
                return Err(zeruel::falla(format!(
                    "cortado con {} MB reservados",
                    i * self.mb_por_paso
                )));
            }
            // Se escribe cada pagina: si solo se reserva, el sistema operativo
            // puede no asignar nada y el RSS no sube.
            let mut b = vec![0u8; self.mb_por_paso * 1024 * 1024];
            for j in (0..b.len()).step_by(4096) {
                b[j] = 1;
            }
            bloques.push(b);
            std::thread::sleep(Duration::from_millis(60));
        }
        Ok(TaskOutput {
            rows: (self.pasos * self.mb_por_paso) as u64,
            message: format!("reserve {} MB", self.pasos * self.mb_por_paso),
        })
    }
}

/// Reserva TODO de una sola vez. Es el caso que al techo por muestreo se le
/// escapa: entre dos muestras (25 ms) ya reservo todo.
struct Golpe {
    mb: usize,
}

impl Task for Golpe {
    fn id(&self) -> TaskId {
        TaskId("golpe".into())
    }

    fn run(&self, _ctx: &TaskContext) -> TaskResult {
        let mut b = vec![0u8; self.mb * 1024 * 1024];
        for j in (0..b.len()).step_by(4096) {
            b[j] = 1;
        }
        Ok(TaskOutput {
            rows: self.mb as u64,
            message: format!("reserve {} MB de una", self.mb),
        })
    }
}

fn main() -> zeruel::Result<()> {
    // Dos pipelines en un mismo binario, elegidos por variable de entorno: uno
    // para probar paralelismo y cancelacion, otro para probar el techo de RAM.
    if std::env::var("ZERUEL_MODO").as_deref() == Ok("golpe") {
        let dag = zeruel::dag!("golpe", "golpe" => []);
        return zeruel::pipeline(dag).register(Golpe { mb: 3000 }).start();
    }

    if std::env::var("ZERUEL_MODO").as_deref() == Ok("tragon") {
        let dag = zeruel::dag!("tragon", "tragon" => []);
        return zeruel::pipeline(dag)
            .register(Tragon { mb_por_paso: 64, pasos: 40 })
            .start();
    }

    let dag = zeruel::dag!("lento",
        "preparar" => [],
        "procesar" => ["preparar"],
    );
    zeruel::pipeline(dag)
        // Empotra este archivo en el binario para poder verlo desde la
        // interfaz. Es el codigo que compilo, no el que este hoy en el repo.
        .fuente(zeruel::fuente!())
        .register(Dormir { id: "preparar", segundos: 2 })
        .register(Dormir { id: "procesar", segundos: 20 })
        .start()
}
