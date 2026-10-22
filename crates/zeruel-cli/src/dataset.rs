//! Generador de dataset sintetico de e-commerce (seed fija, reproducible).
//!
//! Genera **3** datasets (el plan original hablaba de 4, pero `orders` nunca
//! existio: el `order_id` vive dentro de transactions):
//!   transactions: id, order_id, customer_id, product_id, monto, categoria,
//!                 estado, ts  (10M por defecto)
//!   customers:    customer_id, pais, ciudad, segmento
//!   products:     product_id, nombre, precio_base, categoria
//!
//! `--n` fija las transacciones; customers y products escalan con ella (con un
//! piso, para que un dataset chico no quede sin claves para los joins). Antes
//! eran fijos en 200.000 y 5.000, asi que `gen --n 2000` generaba 207.000 filas
//! y reportaba ese numero como si fueran las transacciones pedidas.

use anyhow::{Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;
use rand::Rng;
use rand::SeedableRng;
use zeruel_data::{write_parquet_rows, DataEnv};

const PAISES: [&str; 8] = [
    "Argentina",
    "Brasil",
    "Chile",
    "Colombia",
    "Espana",
    "Mexico",
    "Peru",
    "Uruguay",
];
const CIUDADES: [&str; 16] = [
    "Buenos Aires",
    "Cordoba",
    "Rosario",
    "Sao Paulo",
    "Rio",
    "Santiago",
    "Bogota",
    "Madrid",
    "CDMX",
    "Guadalajara",
    "Lima",
    "Montevideo",
    "Valparaiso",
    "Mendoza",
    "La Plata",
    "Quito",
];
const CATEGORIAS: [&str; 6] = [
    "Electronica",
    "Ropa",
    "Hogar",
    "Deportes",
    "Libros",
    "Juguetes",
];
const ESTADOS: [&str; 3] = ["pagado", "pendiente", "reembolsado"];
const SEGMENTOS: [&str; 4] = ["nuevo", "regular", "vip", "inactivo"];

pub struct Dataset {
    pub n_transactions: u32,
    pub n_customers: u32,
    pub n_products: u32,
    pub seed: u64,
    /// % de transacciones invalidas que hay que filtrar (monto<=0 o order nulo)
    pub dirty_pct: f64,
}

impl Default for Dataset {
    fn default() -> Self {
        Self::con_transacciones(10_000_000)
    }
}

impl Dataset {
    /// Dataset dimensionado a partir de las transacciones pedidas.
    ///
    /// customers = n/50 y products = n/2000, con pisos de 1.000 y 100 para que
    /// un dataset chico siga teniendo variedad en los joins. Con el default de
    /// 10M da los mismos 200.000 / 5.000 de siempre, asi que el benchmark de
    /// referencia no cambia.
    pub fn con_transacciones(n: u32) -> Self {
        Self {
            n_transactions: n,
            n_customers: (n / 50).clamp(1_000, 200_000),
            n_products: (n / 2_000).clamp(100, 5_000),
            seed: 42,
            dirty_pct: 0.02,
        }
    }
}

impl Dataset {
    /// Genera los 3 datasets en `env` (input/). Devuelve las filas escritas.
    pub fn generate(&self, env: &DataEnv) -> Result<u64> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(self.seed);
        let base_day = NaiveDate::from_ymd_opt(2025, 1, 1).context("fecha base")?;

        // ---- customers ----
        let mut c_ids = Vec::with_capacity(self.n_customers as usize);
        let mut c_pais = Vec::with_capacity(self.n_customers as usize);
        let mut c_ciudad = Vec::with_capacity(self.n_customers as usize);
        let mut c_seg = Vec::with_capacity(self.n_customers as usize);
        for i in 0..self.n_customers {
            c_ids.push(i as i64);
            c_pais.push(PAISES[rng.random_range(0..PAISES.len())]);
            c_ciudad.push(CIUDADES[rng.random_range(0..CIUDADES.len())]);
            c_seg.push(SEGMENTOS[rng.random_range(0..SEGMENTOS.len())]);
        }
        let customers = anyhow::Context::context(
            df!(
                "customer_id" => c_ids,
                "pais" => c_pais,
                "ciudad" => c_ciudad,
                "segmento" => c_seg,
            ),
            "customers",
        )?;

        // ---- products ----
        let mut p_ids = Vec::with_capacity(self.n_products as usize);
        let mut p_nom = Vec::with_capacity(self.n_products as usize);
        let mut p_pre = Vec::with_capacity(self.n_products as usize);
        let mut p_cat = Vec::with_capacity(self.n_products as usize);
        for i in 0..self.n_products {
            let cat = CATEGORIAS[rng.random_range(0..CATEGORIAS.len())];
            p_ids.push(i as i64);
            p_nom.push(format!("producto-{}", i));
            p_pre.push(rng.random_range(1.0..500.0));
            p_cat.push(cat);
        }
        let products = anyhow::Context::context(
            df!(
                "product_id" => p_ids,
                "nombre" => p_nom,
                "precio_base" => p_pre,
                "categoria" => p_cat,
            ),
            "products",
        )?;

        // ---- transactions (en bloques, se concatenan al final) ----
        // Ojo: los bloques acotan el pico del generador de valores, pero el
        // DataFrame final igual se arma entero en RAM antes de escribirse.
        const BLOCK: u32 = 250_000;
        let n = self.n_transactions;
        let mut chunks: Vec<DataFrame> = Vec::new();
        for start in (0..n).step_by(BLOCK as usize) {
            let count = (n - start).min(BLOCK) as usize;
            let mut t_id = Vec::with_capacity(count);
            let mut t_ord = Vec::with_capacity(count);
            let mut t_cust = Vec::with_capacity(count);
            let mut t_prod = Vec::with_capacity(count);
            let mut t_monto = Vec::with_capacity(count);
            let mut t_cat = Vec::with_capacity(count);
            let mut t_est = Vec::with_capacity(count);
            let mut t_ts = Vec::with_capacity(count);
            for i in 0..count {
                let idx = (start + i as u32) as i64;
                t_id.push(idx);
                let dirty = rng.random::<f64>() < self.dirty_pct;
                t_ord.push(if dirty && rng.random::<bool>() {
                    None
                } else {
                    Some(idx / 10)
                });
                t_cust.push(rng.random_range(0..self.n_customers as i64));
                t_prod.push(rng.random_range(0..self.n_products as i64));
                t_monto.push(if dirty && rng.random::<bool>() {
                    -rng.random_range(1.0..100.0)
                } else {
                    rng.random_range(1.0..1000.0)
                });
                t_cat.push(CATEGORIAS[rng.random_range(0..CATEGORIAS.len())]);
                t_est.push(ESTADOS[rng.random_range(0..ESTADOS.len())]);
                let day = base_day
                    .checked_add_signed(chrono::Duration::days(rng.random_range(0..180) as i64))
                    .unwrap();
                let ts = day
                    .and_hms_opt(
                        rng.random_range(0..24),
                        rng.random_range(0..60),
                        rng.random_range(0..60),
                    )
                    .unwrap();
                t_ts.push(ts);
            }
            let chunk = anyhow::Context::context(
                df!(
                    "id" => t_id,
                    "order_id" => t_ord,
                    "customer_id" => t_cust,
                    "product_id" => t_prod,
                    "monto" => t_monto,
                    "categoria" => t_cat,
                    "estado" => t_est,
                    "ts" => t_ts,
                ),
                "transactions chunk",
            )?;
            chunks.push(chunk);
        }
        // concatenar chunks sin tocar la API de concat (vstack en loop)
        let mut acc = chunks
            .first()
            .cloned()
            .context("no hay chunks de transactions")?;
        for c in chunks.iter().skip(1) {
            acc = anyhow::Context::context(acc.vstack(c), "vstack transactions")?;
        }
        let transactions = acc;

        // ---- escribir ----
        let mut total = 0u64;
        for (name, df) in [
            ("input/transactions.parquet", &transactions),
            ("input/customers.parquet", &customers),
            ("input/products.parquet", &products),
        ] {
            let p = env.path(name);
            std::fs::create_dir_all(p.parent().context("dir input")?)?;
            let rows = write_parquet_rows(df.clone(), &p)
                .with_context(|| format!("escribiendo {}", p.display()))?;
            total += rows;
        }
        Ok(total)
    }
}
