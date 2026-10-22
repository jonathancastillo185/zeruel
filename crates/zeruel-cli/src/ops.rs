//! Operaciones de la cola compartidas por la CLI y el MCP.
//!
//! Existen porque `backfill`, `sla` y `cola` estaban implementadas **dos
//! veces**: una en `main.rs` y otra en `mcp.rs`. Las dos versiones divergieron
//! y solo una era correcta:
//!
//! - el backfill del MCP se salteaba el ultimo dia del rango (N-05);
//! - `sla --max-wait 0` borraba el tope por MCP y fijaba "0 segundos" por CLI,
//!   que hace que todo item viole el SLA al instante (N-12).
//!
//! Con una sola implementacion, un arreglo vale para las dos puertas de entrada.

use anyhow::{Context, Result};
use chrono::NaiveDate;
use zeruel_core::Store;

/// Resultado de un backfill: cuantos entraron y que se salteo (con motivo).
#[derive(Debug)]
pub struct BackfillReport {
    pub dag: String,
    pub desde: String,
    pub hasta: String,
    pub encolados: u32,
    pub totales: i64,
    pub skipped: Vec<String>,
}

/// Encola un run por dia en el rango `[desde, hasta]`, **ambos inclusive**.
///
/// Los dias que `enqueue` rechaza (ya en cola, o con un run activo) se
/// reportan en `skipped` sin abortar el resto: un backfill parcial es mas util
/// que uno que se corta en el primer conflicto.
pub fn backfill(
    store: &Store,
    dag: &str,
    desde: &str,
    hasta: &str,
    priority: i64,
) -> Result<BackfillReport> {
    let d0 = NaiveDate::parse_from_str(desde, "%Y-%m-%d")
        .with_context(|| format!("--desde invalido, usar YYYY-MM-DD (vino {desde:?})"))?;
    let d1 = NaiveDate::parse_from_str(hasta, "%Y-%m-%d")
        .with_context(|| format!("--hasta invalido, usar YYYY-MM-DD (vino {hasta:?})"))?;
    if d0 > d1 {
        anyhow::bail!("--desde ({desde}) es posterior a --hasta ({hasta})");
    }

    let mut encolados = 0u32;
    let mut skipped = Vec::new();
    let mut dia = d0;
    loop {
        let param = dia.format("%Y-%m-%d").to_string();
        match store.enqueue(dag, &param, priority) {
            Ok(r) if r.ok => encolados += 1,
            Ok(r) => skipped.push(format!("{param}: {}", r.motivo)),
            Err(e) => skipped.push(format!("{param}: {e}")),
        }
        // El corte va DESPUES de encolar `hasta`: el rango es inclusive.
        if dia >= d1 {
            break;
        }
        dia = match dia.succ_opt() {
            Some(d) => d,
            None => break,
        };
    }

    Ok(BackfillReport {
        dag: dag.to_string(),
        desde: desde.to_string(),
        hasta: hasta.to_string(),
        encolados,
        totales: (d1 - d0).num_days() + 1,
        skipped,
    })
}

/// Clave de `meta` donde vive el tope de espera de un DAG.
fn sla_key(dag: &str) -> String {
    format!("sla:{dag}")
}

/// Fija el tope de espera en cola de un DAG, en segundos.
///
/// `0` **borra** el tope (una sola semantica para CLI y MCP, N-12). Guardar
/// "0" significaria "tope de cero segundos", con lo que cualquier item en cola
/// viola el SLA al instante y la tab Alertas se llena de falsos positivos.
pub fn set_sla(store: &Store, dag: &str, max_wait: i64) -> Result<Option<i64>> {
    if max_wait < 0 {
        anyhow::bail!("--max-wait tiene que ser >= 0 (vino {max_wait}); 0 borra el tope");
    }
    if max_wait == 0 {
        store.meta_del(&sla_key(dag))?;
        Ok(None)
    } else {
        store.meta_set(&sla_key(dag), &max_wait.to_string())?;
        Ok(Some(max_wait))
    }
}

/// Tope de espera configurado para un DAG (None = sin tope).
pub fn get_sla(store: &Store, dag: &str) -> Option<i64> {
    store
        .meta_get(&sla_key(dag))
        .ok()
        .flatten()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|n| *n > 0)
}

/// Clave de `meta` con el tope global de items en cola.
pub const QUEUE_MAX_KEY: &str = "queue.max_size";

/// Fija el tope global de items en cola (0 = sin tope).
pub fn set_queue_max(store: &Store, max: usize) -> Result<()> {
    if max == 0 {
        store.meta_del(QUEUE_MAX_KEY)?;
    } else {
        store.meta_set(QUEUE_MAX_KEY, &max.to_string())?;
    }
    Ok(())
}

/// Tope global de items en cola (0 = sin tope).
pub fn get_queue_max(store: &Store) -> i64 {
    store
        .meta_get(QUEUE_MAX_KEY)
        .ok()
        .flatten()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    #[test]
    fn backfill_incluye_el_ultimo_dia() {
        // N-05: la version del MCP cortaba antes de encolar `hasta`.
        let s = store();
        let r = backfill(&s, "d", "2026-08-10", "2026-08-13", 0).unwrap();
        assert_eq!(r.totales, 4);
        assert_eq!(r.encolados, 4, "10, 11, 12 y 13");
        let params: Vec<String> = s
            .queue_items()
            .unwrap()
            .into_iter()
            .map(|i| i.param)
            .collect();
        assert!(
            params.contains(&"2026-08-13".to_string()),
            "el ultimo dia entra"
        );
    }

    #[test]
    fn backfill_de_un_solo_dia() {
        let s = store();
        let r = backfill(&s, "d", "2026-08-10", "2026-08-10", 0).unwrap();
        assert_eq!((r.totales, r.encolados), (1, 1));
    }

    #[test]
    fn backfill_rechaza_rango_invertido_y_fecha_mala() {
        let s = store();
        assert!(backfill(&s, "d", "2026-08-13", "2026-08-10", 0).is_err());
        assert!(backfill(&s, "d", "ayer", "2026-08-10", 0).is_err());
    }

    #[test]
    fn backfill_reporta_los_dias_que_no_entraron() {
        let s = store();
        s.enqueue("d", "2026-08-11", 0).unwrap(); // ya esta en cola
        let r = backfill(&s, "d", "2026-08-10", "2026-08-12", 0).unwrap();
        assert_eq!(r.encolados, 2);
        assert_eq!(r.skipped.len(), 1);
        assert!(r.skipped[0].starts_with("2026-08-11"));
    }

    #[test]
    fn sla_cero_borra_el_tope() {
        // N-12: una sola semantica para CLI y MCP.
        let s = store();
        assert_eq!(set_sla(&s, "d", 3600).unwrap(), Some(3600));
        assert_eq!(get_sla(&s, "d"), Some(3600));
        assert_eq!(set_sla(&s, "d", 0).unwrap(), None);
        assert_eq!(get_sla(&s, "d"), None, "0 = sin tope, no tope de 0s");
        assert!(set_sla(&s, "d", -1).is_err());
    }

    #[test]
    fn queue_max_cero_quita_el_tope() {
        let s = store();
        set_queue_max(&s, 5).unwrap();
        assert_eq!(get_queue_max(&s), 5);
        set_queue_max(&s, 0).unwrap();
        assert_eq!(get_queue_max(&s), 0);
        // y con el tope quitado se puede encolar sin limite
        for i in 0..10 {
            assert!(s.enqueue("d", &format!("p{i}"), 0).unwrap().ok);
        }
    }
}
