//! Registro de DAGs disponibles. Cada entrada mapea un nombre a un builder.
//! Para agregar un DAG nuevo: se crea su builder (un fn que devuelve Pipeline)
//! y se agrega una entrada al vec en `all()`.

use serde_json::json;
use zeruel_data::DataEnv;

use crate::pipeline::Pipeline;

/// Spec de un DAG registrado: nombre + builder.
pub struct DagSpec {
    pub name: &'static str,
    pub build: fn(&DataEnv) -> Pipeline,
}

/// Todos los DAGs registrados. Agregar nuevos aquí.
pub fn all() -> Vec<DagSpec> {
    vec![
        DagSpec {
            name: "ecommerce-etl",
            build: crate::pipeline::build_ecommerce,
        },
        // Ejemplo para agregar otro DAG:
        // DagSpec { name: "scraping_precios", build: crate::dags_scraping::build_precios },
    ]
}

/// Busca un DAG por nombre.
pub fn find(name: &str) -> Option<DagSpec> {
    all().into_iter().find(|d| d.name == name)
}

/// Lista los nombres de todos los DAGs registrados.
pub fn names() -> Vec<&'static str> {
    all().iter().map(|d| d.name).collect()
}

/// Grafo JSON de un DAG (para /api/graph?dag=X).
pub fn graph_for(name: &str) -> Option<serde_json::Value> {
    let spec = find(name)?;
    let p = (spec.build)(&DataEnv::new("unused"));
    let d = &p.dag;
    let ns: Vec<_> = d.tasks.iter().map(|t| json!({"id": t.id.0})).collect();
    let es: Vec<_> = d
        .tasks
        .iter()
        .flat_map(|t| {
            t.deps
                .iter()
                .map(move |dep| json!({"from": dep.0, "to": t.id.0}))
        })
        .collect();
    Some(json!({"name": d.name, "nodes": ns, "edges": es}))
}

/// Schedule cron de un DAG (para el scheduler tick_cron).
pub fn dag_cron(name: &str) -> Option<(String, String)> {
    let spec = find(name)?;
    let p = (spec.build)(&DataEnv::new("unused"));
    p.dag
        .schedule
        .as_ref()
        .map(|s| (p.dag.name.clone(), s.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiene_ecommerce() {
        let names = names();
        assert!(names.contains(&"ecommerce-etl"));
    }

    #[test]
    fn graph_devuelve_nodos() {
        let g = graph_for("ecommerce-etl").unwrap();
        let nodes = g["nodes"].as_array().unwrap();
        assert!(nodes.len() >= 6);
    }

    #[test]
    fn find_desconocido_es_none() {
        assert!(find("no-existe").is_none());
        assert!(graph_for("no-existe").is_none());
    }
}
