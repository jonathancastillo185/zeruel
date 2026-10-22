//! Parser de cron (5 campos: min hora dia-mes mes dia-semana) para schedules
//! de DAG.
//!
//! Sintaxis soportada por campo:
//! - `*`      cualquier valor
//! - `N`      un valor fijo
//! - `A-B`    rango inclusive
//! - `*/S`    cada S unidades
//! - `N/M`    desde N, cada M (p. ej. `30/15` = 30,45,60->no, solo dentro del rango)
//! - lista: `a,b,c` (mezcla de los anteriores)
//!
//! dia-semana: 0-6 (0 = domingo).
//!
//! No soporta nombres (jan, mon), `L`, `W` ni `@hourly`. Para lo que un DAG de
//! e-commerce necesita alcanza; si falta, se extiende aca.

use chrono::{Datelike, Timelike};

/// ¿El cron `spec` dispara en el momento `t`? (t en UTC.)
pub fn matches(spec: &str, t: &chrono::DateTime<chrono::Utc>) -> bool {
    let parts: Vec<&str> = spec.split_whitespace().collect();
    if parts.len() != 5 {
        return false;
    }
    let ok_min = campo_parts(parts[0], 0, 59, t.minute() as i32);
    let ok_hora = campo_parts(parts[1], 0, 23, t.hour() as i32);
    let ok_dia = campo_parts(parts[2], 1, 31, t.day() as i32);
    let ok_mes = campo_parts(parts[3], 1, 12, t.month() as i32);
    let dow = t.weekday().number_from_monday() % 7; // 0=domingo
    let ok_dow = campo_parts(parts[4], 0, 6, dow as i32);
    ok_min && ok_hora && ok_dia && ok_mes && ok_dow
}

/// Evaluacion de un solo campo cron (lista de terminos, OR entre terminos).
fn campo_parts(spec: &str, lo: i32, hi: i32, val: i32) -> bool {
    spec.split(',')
        .any(|termo| campo_termo(termo.trim(), lo, hi, val))
}

fn campo_termo(terminal: &str, lo: i32, hi: i32, val: i32) -> bool {
    if terminal.is_empty() {
        return false;
    }
    // separar paso: A-B/S o */S o N
    let (base, paso) = match terminal.split_once('/') {
        Some((b, s)) => (b, s.parse::<i32>().ok().filter(|p| *p > 0)),
        None => (terminal, None),
    };
    if base == "*" {
        return match paso {
            Some(p) => (val - lo) % p == 0,
            None => true,
        };
    }
    // rango A-B
    if let Some((a, b)) = base.split_once('-') {
        let a: i32 = match a.parse() {
            Ok(v) => v,
            Err(_) => return false,
        };
        let b: i32 = match b.parse() {
            Ok(v) => v,
            Err(_) => return false,
        };
        if a > b || a < lo || b > hi {
            return false;
        }
        if val < a || val > b {
            return false;
        }
        return match paso {
            Some(p) => (val - a) % p == 0,
            None => true,
        };
    }
    // valor fijo (con o sin paso: N/M = desde N, cada M)
    let n: i32 = match base.parse() {
        Ok(v) => v,
        Err(_) => return false,
    };
    if n < lo || n > hi {
        return false;
    }
    match paso {
        Some(p) => (val - n) >= 0 && (val - n) % p == 0,
        None => val == n,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, m: u32, d: u32, h: u32, mi: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
    }

    #[test]
    fn asterisco_completo() {
        assert!(matches("* * * * *", &at(2026, 8, 16, 10, 30)));
    }

    #[test]
    fn cada_minuto_exacto() {
        assert!(matches("30 * * * *", &at(2026, 8, 16, 10, 30)));
        assert!(!matches("30 * * * *", &at(2026, 8, 16, 10, 31)));
    }

    #[test]
    fn cada_15_minutos() {
        assert!(matches("*/15 * * * *", &at(2026, 8, 16, 10, 0)));
        assert!(matches("*/15 * * * *", &at(2026, 8, 16, 10, 15)));
        assert!(matches("*/15 * * * *", &at(2026, 8, 16, 10, 30)));
        assert!(!matches("*/15 * * * *", &at(2026, 8, 16, 10, 7)));
    }

    #[test]
    fn rango_de_horas() {
        assert!(matches("0 9-17 * * *", &at(2026, 8, 16, 9, 0)));
        assert!(matches("0 9-17 * * *", &at(2026, 8, 16, 17, 0)));
        assert!(!matches("0 9-17 * * *", &at(2026, 8, 16, 8, 0)));
        assert!(!matches("0 9-17 * * *", &at(2026, 8, 16, 18, 0)));
    }

    #[test]
    fn lista() {
        assert!(matches("0 9,13,17 * * *", &at(2026, 8, 16, 9, 0)));
        assert!(matches("0 9,13,17 * * *", &at(2026, 8, 16, 13, 0)));
        assert!(!matches("0 9,13,17 * * *", &at(2026, 8, 16, 10, 0)));
    }

    #[test]
    fn dia_de_semana_domingo() {
        // 2026-08-16 es domingo
        assert!(matches("0 0 * * 0", &at(2026, 8, 16, 0, 0)));
        // 2026-08-17 es lunes
        assert!(!matches("0 0 * * 0", &at(2026, 8, 17, 0, 0)));
        assert!(matches("0 0 * * 1", &at(2026, 8, 17, 0, 0)));
    }

    #[test]
    fn dia_y_mes() {
        assert!(matches("0 6 1 1 *", &at(2027, 1, 1, 6, 0)));
        assert!(!matches("0 6 1 1 *", &at(2027, 1, 2, 6, 0)));
    }

    #[test]
    fn paso_desde_n() {
        // 30/15 = 30, 45 (dentro de 0..59)
        assert!(matches("30/15 * * * *", &at(2026, 8, 16, 10, 30)));
        assert!(matches("30/15 * * * *", &at(2026, 8, 16, 10, 45)));
        assert!(!matches("30/15 * * * *", &at(2026, 8, 16, 10, 0)));
    }

    #[test]
    fn spec_invalida_no_dispara() {
        assert!(!matches("nope", &at(2026, 8, 16, 10, 30)));
        assert!(!matches("* * * *", &at(2026, 8, 16, 10, 30))); // 4 campos
        assert!(!matches("* * * * * *", &at(2026, 8, 16, 10, 30))); // 6 campos
    }
}

/// Cuando dispara `spec` por primera vez despues de `desde`.
///
/// Devuelve `None` si el cron es invalido o si no dispara en el proximo año —
/// que en la practica significa que esta mal escrito (`0 0 30 2 *`, un 30 de
/// febrero).
///
/// ## Por que no se recorre minuto a minuto
///
/// Un año son 527.040 minutos. Evaluando el cron completo en cada uno, y con
/// 59 pipelines en la lista, la pantalla tardaria segundos en dibujarse.
///
/// Se recorre en tres niveles con corte temprano: si el dia no matchea se
/// saltan sus 1.440 minutos de una; si la hora no matchea, sus 60. El peor
/// caso pasa de 527.040 evaluaciones a unas 366 + 24 + 60, y el caso normal
/// es muchisimo menos.
pub fn proxima(
    spec: &str,
    desde: &chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    let parts: Vec<&str> = spec.split_whitespace().collect();
    if parts.len() != 5 {
        return None;
    }

    // Se arranca del minuto siguiente: un cron que matchea AHORA ya disparo.
    let inicio = (*desde + chrono::Duration::minutes(1))
        .with_second(0)?
        .with_nanosecond(0)?;

    for dias in 0..366 {
        let dia = inicio.date_naive() + chrono::Duration::days(dias);
        let dow = dia.weekday().number_from_monday() % 7; // 0 = domingo
        if !campo_parts(parts[2], 1, 31, dia.day() as i32)
            || !campo_parts(parts[3], 1, 12, dia.month() as i32)
            || !campo_parts(parts[4], 0, 6, dow as i32)
        {
            continue;
        }
        // Solo el primer dia arranca a mitad de camino; los demas, a las 00:00.
        let (h0, m0) = if dias == 0 {
            (inicio.hour(), inicio.minute())
        } else {
            (0, 0)
        };
        for h in h0..24 {
            if !campo_parts(parts[1], 0, 23, h as i32) {
                continue;
            }
            let desde_min = if dias == 0 && h == h0 { m0 } else { 0 };
            for m in desde_min..60 {
                if campo_parts(parts[0], 0, 59, m as i32) {
                    return dia.and_hms_opt(h, m, 0).map(|t| t.and_utc());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests_proxima {
    use super::*;
    use chrono::TimeZone;

    fn en(a: i32, m: u32, d: u32, h: u32, mi: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(a, m, d, h, mi, 0).unwrap()
    }

    #[test]
    fn cada_hora_en_punto() {
        let p = proxima("0 * * * *", &en(2026, 8, 24, 10, 30)).unwrap();
        assert_eq!(p, en(2026, 8, 24, 11, 0));
    }

    #[test]
    fn un_cron_que_matchea_ahora_devuelve_el_siguiente() {
        // Justo en el momento del disparo, la proxima es la de la vuelta que
        // viene: si devolviera "ahora", la pantalla mostraria una hora que ya
        // paso apenas se refresque.
        let p = proxima("0 * * * *", &en(2026, 8, 24, 10, 0)).unwrap();
        assert_eq!(p, en(2026, 8, 24, 11, 0));
    }

    #[test]
    fn cada_dos_horas_cruzando_la_medianoche() {
        let p = proxima("0 */2 * * *", &en(2026, 8, 24, 23, 30)).unwrap();
        assert_eq!(p, en(2026, 8, 25, 0, 0));
    }

    #[test]
    fn mensual_el_dia_uno() {
        // El caso real de `sucursales`: 04:00 del dia 1 de cada mes. Es el que
        // obliga a mirar mas de un mes hacia adelante.
        let p = proxima("0 4 1 * *", &en(2026, 8, 24, 12, 0)).unwrap();
        assert_eq!(p, en(2026, 9, 1, 4, 0));
    }

    #[test]
    fn diario_de_madrugada() {
        let p = proxima("0 2 * * *", &en(2026, 8, 24, 12, 0)).unwrap();
        assert_eq!(p, en(2026, 8, 25, 2, 0));
    }

    #[test]
    fn un_cron_imposible_no_cuelga() {
        // 30 de febrero: no existe. Tiene que devolver None tras agotar el
        // año, no quedarse buscando.
        assert!(proxima("0 0 30 2 *", &en(2026, 8, 24, 12, 0)).is_none());
    }

    #[test]
    fn un_cron_mal_escrito_devuelve_none() {
        assert!(proxima("no es un cron", &en(2026, 8, 24, 12, 0)).is_none());
        assert!(proxima("0 4 1 *", &en(2026, 8, 24, 12, 0)).is_none());
    }

    #[test]
    fn coincide_con_matches() {
        // La proxima que devuelve tiene que ser un momento que `matches`
        // reconozca: si no, las dos funciones estarian diciendo cosas
        // distintas sobre el mismo cron.
        for spec in ["0 * * * *", "*/30 * * * *", "0 4 1 * *", "0 2 * * *"] {
            let p = proxima(spec, &en(2026, 8, 24, 12, 0)).unwrap();
            assert!(matches(spec, &p), "{spec} no matchea su propia proxima {p}");
        }
    }
}
