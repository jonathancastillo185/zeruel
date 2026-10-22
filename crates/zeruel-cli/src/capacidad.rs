//! ¿Alcanza el paralelismo para lo que hay agendado?
//!
//! # Por qué existe
//!
//! Es una cuenta de una línea —cuánto trabajo piden los cron contra cuánto
//! puede hacer la máquina— y zeruel tenía **todos los datos para hacerla y no
//! la hacía nunca**. Los horarios salen del `--describe` de cada pipeline; las
//! duraciones, de las corridas que ya pasaron.
//!
//! Sin esa cuenta, un paralelismo insuficiente no se ve como un error: se ve
//! como una cola que crece. En la instalación real el scheduler quedó con
//! `paralelo=2` cuando hacía falta 4, y el síntoma fue una cola de 1.400 ítems
//! **dos días después**. La cuenta lo decía desde el primer minuto:
//!
//! ```text
//!   trabajo agendado:   85,2 h de computo por dia
//!   paralelo=2  uso 178%   NO ALCANZA
//!   paralelo=6  uso  59%   OK
//! ```
//!
//! Con 178% la cola no puede drenar nunca. No es un bug que haya que
//! encontrar: es aritmética, y decirla evita buscar la causa en otro lado.
//!
//! # Lo que esto NO es
//!
//! No es una predicción. Usa la mediana de lo que cada DAG tardó, así que un
//! pipeline que se ponga más lento mañana no está contemplado hasta que corra.
//! Sirve para contestar "¿esto puede funcionar?", no "¿cuándo termina?".

use std::collections::BTreeMap;

use zeruel_core::Store;

use crate::pipelines::Pipeline;

/// Lo que un DAG le cuesta al sistema por día.
#[derive(Debug, Clone)]
pub struct Peso {
    pub dag: String,
    pub spec: String,
    /// Cuántas veces se dispara por día.
    pub por_dia: u32,
    /// Mediana de lo que tarda, en segundos. 0 si nunca corrió.
    pub dur_mediana: f64,
    /// Horas de cómputo que genera por día.
    pub horas_dia: f64,
}

/// El resultado de la cuenta.
#[derive(Debug, Clone)]
pub struct Informe {
    /// Horas de cómputo por día que piden los cron.
    pub demanda_horas_dia: f64,
    /// Runs en paralelo configurados.
    pub paralelo: usize,
    /// Qué fracción de la capacidad se usa. > 1.0 = imposible.
    pub uso: f64,
    /// Los que más pesan, de mayor a menor.
    pub pesos: Vec<Peso>,
    /// DAGs agendados que nunca corrieron: su costo no se puede estimar.
    pub sin_historial: Vec<String>,
}

impl Informe {
    /// Con este uso, ¿la cola puede drenar?
    pub fn alcanza(&self) -> bool {
        self.uso < 1.0
    }

    /// Cuántos runs en paralelo harían falta para quedar por debajo del 70%.
    ///
    /// El 70% y no el 100%: una cola necesita holgura para absorber los picos
    /// y los reintentos. Al 100% cualquier corrida más lenta que su mediana ya
    /// genera atraso, y el atraso no se recupera solo.
    pub fn paralelo_sugerido(&self) -> usize {
        ((self.demanda_horas_dia / 24.0 / 0.7).ceil() as usize).max(1)
    }
}

/// Los valores que matchea un campo de cron (`*/10`, `3-21/6`, `0,30`, `5`).
///
/// Se cuentan, no se evalúan contra un reloj: para saber cuánto trabajo genera
/// un horario alcanza con saber cuántas veces dispara.
fn expandir(campo: &str, lo: u32, hi: u32) -> Vec<u32> {
    let mut out = std::collections::BTreeSet::new();
    for parte in campo.split(',') {
        let (rango, paso) = match parte.split_once('/') {
            Some((r, p)) => (r, p.parse::<u32>().unwrap_or(1).max(1)),
            None => (parte, 1),
        };
        let (a, b) = if rango == "*" {
            (lo, hi)
        } else if let Some((x, y)) = rango.split_once('-') {
            (
                x.trim().parse().unwrap_or(lo),
                y.trim().parse().unwrap_or(hi),
            )
        } else {
            match rango.trim().parse::<u32>() {
                Ok(v) => (v, v),
                Err(_) => continue,
            }
        };
        let (a, b) = (a.max(lo), b.min(hi));
        let mut v = a;
        while v <= b {
            out.insert(v);
            v += paso;
        }
    }
    out.into_iter().collect()
}

/// Cuántas veces por día dispara un spec de cron.
///
/// Sólo se miran minuto, hora y día del mes: si el spec restringe días, se
/// prorratea. Los meses y días de semana se ignoran a propósito — un DAG que
/// corre los lunes genera la séptima parte del trabajo, y afinar eso no cambia
/// la respuesta de "¿alcanza o no?".
pub fn disparos_por_dia(spec: &str) -> u32 {
    let campos: Vec<&str> = spec.split_whitespace().collect();
    if campos.len() < 3 {
        return 0;
    }
    let minutos = expandir(campos[0], 0, 59).len() as u32;
    let horas = expandir(campos[1], 0, 23).len() as u32;
    // Día del mes: si está restringido, el promedio diario baja.
    let dias = expandir(campos[2], 1, 31).len().max(1) as f64 / 31.0;
    ((minutos * horas) as f64 * dias).round() as u32
}

/// Hace la cuenta con lo que hay desplegado y lo que ya corrió.
pub fn calcular(store: &Store, pipelines: &[Pipeline], paralelo: usize) -> Informe {
    // La mediana de cada DAG, de una sola consulta y no una por pipeline.
    let medianas: BTreeMap<String, f64> = store
        .medianas_por_dag()
        .unwrap_or_default()
        .into_iter()
        .collect();

    let mut pesos = Vec::new();
    let mut sin_historial = Vec::new();
    for p in pipelines {
        let Some(spec) = &p.desc.schedule else {
            // Sin cron no genera carga por sí solo: lo encola alguien.
            continue;
        };
        let por_dia = disparos_por_dia(spec);
        let dur = medianas.get(&p.desc.name).copied().unwrap_or(0.0);
        if dur == 0.0 {
            sin_historial.push(p.desc.name.clone());
        }
        pesos.push(Peso {
            dag: p.desc.name.clone(),
            spec: spec.clone(),
            por_dia,
            dur_mediana: dur,
            horas_dia: por_dia as f64 * dur / 3600.0,
        });
    }
    pesos.sort_by(|a, b| b.horas_dia.total_cmp(&a.horas_dia));

    let demanda_horas_dia: f64 = pesos.iter().map(|p| p.horas_dia).sum();
    let paralelo = paralelo.max(1);
    Informe {
        demanda_horas_dia,
        paralelo,
        uso: demanda_horas_dia / (24.0 * paralelo as f64),
        pesos,
        sin_historial,
    }
}

/// Lo escribe en el log al arrancar. Grita sólo cuando no alcanza.
pub fn reportar(inf: &Informe) {
    if inf.pesos.is_empty() {
        return;
    }
    let pct = inf.uso * 100.0;
    // Los que nunca corrieron no suman a la cuenta, asi que la demanda real es
    // MAYOR que la calculada. Decirlo evita que el numero se lea como exacto.
    if !inf.sin_historial.is_empty() {
        tracing::info!(
            "capacidad: {} DAG(s) agendados sin historial todavia, su costo no entra en la cuenta: {}",
            inf.sin_historial.len(),
            inf.sin_historial.join(", ")
        );
    }
    if inf.alcanza() {
        tracing::info!(
            "capacidad: los cron piden {:.1} h de computo por dia; con paralelo={} \
             eso es el {pct:.0}% de la maquina",
            inf.demanda_horas_dia,
            inf.paralelo
        );
    } else {
        tracing::error!(
            "LA COLA NO VA A PODER DRENAR: los cron piden {:.1} h de computo por dia \
             y con paralelo={} la capacidad es {:.1} h. Es el {pct:.0}%, y por encima \
             del 100% la cola crece para siempre. Subi `paralelo` a {} o espacia los \
             horarios.",
            inf.demanda_horas_dia,
            inf.paralelo,
            24.0 * inf.paralelo as f64,
            inf.paralelo_sugerido()
        );
        for p in inf.pesos.iter().take(3) {
            tracing::error!(
                "  el que mas pesa: {} — {}/dia x {:.0}s = {:.1} h/dia ({})",
                p.dag,
                p.por_dia,
                p.dur_mediana,
                p.horas_dia,
                p.spec
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cuenta_los_disparos_de_un_cron() {
        assert_eq!(disparos_por_dia("*/10 * * * *"), 6 * 24);
        assert_eq!(disparos_por_dia("*/30 * * * *"), 2 * 24);
        assert_eq!(disparos_por_dia("0 * * * *"), 24);
        // El caso real que me hizo equivocar la cuenta a mano: mirando sólo el
        // campo de minutos, `0 3-21/6 * * *` parecía "una vez por hora" cuando
        // son cuatro por día. La hora hay que mirarla.
        assert_eq!(disparos_por_dia("0 3-21/6 * * *"), 4);
        assert_eq!(disparos_por_dia("0 1-23/2 * * *"), 12);
        assert_eq!(disparos_por_dia("30 8 * * *"), 1);
        assert_eq!(disparos_por_dia("0,30 8,20 * * *"), 4);
    }

    #[test]
    fn un_spec_roto_no_rompe_la_cuenta() {
        assert_eq!(disparos_por_dia(""), 0);
        assert_eq!(disparos_por_dia("no es un cron"), 0);
    }

    fn inf(demanda: f64, paralelo: usize) -> Informe {
        Informe {
            demanda_horas_dia: demanda,
            paralelo,
            uso: demanda / (24.0 * paralelo as f64),
            pesos: vec![Peso {
                dag: "x".into(),
                spec: "* * * * *".into(),
                por_dia: 1,
                dur_mediana: 1.0,
                horas_dia: demanda,
            }],
            sin_historial: vec![],
        }
    }

    /// **El caso real.** 85,2 h/día de trabajo agendado: con paralelo=2 es
    /// imposible y con 6 sobra. La cuenta lo dice sin que nadie investigue.
    #[test]
    fn dice_si_el_paralelismo_alcanza() {
        assert!(!inf(85.2, 2).alcanza(), "178%: la cola crece para siempre");
        assert!(
            !inf(85.2, 4).alcanza() || inf(85.2, 4).uso > 0.85,
            "89%: al limite"
        );
        assert!(inf(85.2, 6).alcanza(), "59%: holgado");
    }

    /// La sugerencia deja holgura: al 100% cualquier corrida mas lenta que su
    /// mediana genera atraso, y el atraso no se recupera solo.
    #[test]
    fn el_paralelismo_sugerido_deja_margen() {
        let i = inf(85.2, 2);
        let sug = i.paralelo_sugerido();
        let con_sug = inf(85.2, sug);
        assert!(con_sug.uso <= 0.7, "quedo en {:.0}%", con_sug.uso * 100.0);
        assert!(sug >= 6, "hacen falta al menos 6, dio {sug}");
    }

    /// Sin nada agendado no hay nada que decir, y no puede dividir por cero.
    #[test]
    fn sin_cron_no_inventa_un_problema() {
        let i = Informe {
            demanda_horas_dia: 0.0,
            paralelo: 1,
            uso: 0.0,
            pesos: vec![],
            sin_historial: vec![],
        };
        assert!(i.alcanza());
        assert_eq!(i.paralelo_sugerido(), 1);
    }
}
