//! Varios pipelines en un mismo binario.
//!
//! Un binario por pipeline funciona bien cuando cada uno es sustancial. Deja de
//! funcionar cuando son muchos y chicos: un proyecto que genera un DAG por cada fuente de datos puede tener
//! decenas de DAGs —unos pocos fijos y uno por fuente— y casi todos
//! ejecutan una sola linea de shell. Sesenta binarios de 3 MB para eso no tiene
//! sentido.
//!
//! Un bundle agrupa pipelines que comparten dominio y ciclo de vida:
//!
//! ```no_run
//! use zeruel::prelude::*;
//!
//! # fn armar(_n: &str) -> zeruel::Pipeline { unimplemented!() }
//! fn main() -> zeruel::Result<()> {
//!     let mut b = zeruel::bundle();
//!     for fuente in ["norte", "sur", "este"] {
//!         b = b.con(armar(fuente));
//!     }
//!     b.start()
//! }
//! ```
//!
//! El contrato con `zeruel serve` se extiende de forma compatible: `--describe`
//! devuelve `pipelines: [...]` y `run` acepta `--dag <nombre>` para elegir cual.
//! Un binario de un solo pipeline sigue funcionando sin cambios — y de hecho un
//! `Pipeline` suelto es un bundle de uno.
//!
//! **La granularidad la elegis vos.** Un bundle por dominio (todas las fuentes
//! juntas) hace que desplegar sea copiar un archivo, pero recompilar uno los
//! recompila a todos y un panic en la carga los saca a todos de la lista. Un
//! binario por pipeline es lo opuesto. Para pipelines chicos que se despliegan
//! juntos, el bundle gana.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{Error, Pipeline, Result, Store, CONTRATO, EXIT_CANCELADO, EXIT_FALLO};

/// Agrupa varios pipelines en un binario. Ver el modulo.
#[derive(Default)]
pub struct Bundle {
    pipelines: BTreeMap<String, Pipeline>,
}

/// Empieza a armar un bundle.
pub fn bundle() -> Bundle {
    Bundle::default()
}

impl Bundle {
    /// Suma un pipeline ya construido.
    ///
    /// Si dos comparten nombre, gana el ultimo — y se avisa, porque un nombre
    /// repetido casi siempre es un error de generacion (dos fuentes que
    /// derivan el mismo id, por ejemplo).
    pub fn con(mut self, p: Pipeline) -> Self {
        let nombre = p.dag().name.clone();
        if self.pipelines.insert(nombre.clone(), p).is_some() {
            eprintln!("aviso: hay dos pipelines llamados '{nombre}'; queda el ultimo");
        }
        self
    }

    /// Cuantos pipelines tiene.
    pub fn len(&self) -> usize {
        self.pipelines.len()
    }

    /// ¿Esta vacio?
    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty()
    }

    /// Los nombres, en orden.
    pub fn nombres(&self) -> Vec<&str> {
        self.pipelines.keys().map(String::as_str).collect()
    }

    /// El JSON de `--describe`: todos los pipelines del binario.
    pub fn describe(&self) -> serde_json::Value {
        serde_json::json!({
            "zeruel_contract": CONTRATO,
            "pipelines": self
                .pipelines
                .values()
                .map(|p| p.describe_uno())
                .collect::<Vec<_>>(),
        })
    }

    /// Implementa el contrato del binario leyendo `std::env::args`.
    pub fn start(self) -> Result<()> {
        self.main_con(std::env::args().skip(1).collect())
    }

    /// Igual que [`Bundle::start`] pero con los argumentos explicitos (testeable).
    pub fn main_con(&self, args: Vec<String>) -> Result<()> {
        if self.is_empty() {
            return Err(Error::Uso(
                "el bundle no tiene ningun pipeline: revisá que la generación encuentre algo"
                    .into(),
            ));
        }

        match args.first().map(String::as_str) {
            Some("--describe") => {
                println!("{}", self.describe());
                Ok(())
            }
            // Igual que en un pipeline suelto, pero hay que decir de cual: un
            // bundle expone varios y cada uno empota su propio codigo.
            Some("--fuente") => {
                let (nombre, _) = self.elegir_dag(&args[1..])?;
                let p = self.pipelines.get(&nombre).ok_or_else(|| {
                    crate::Error::Uso(format!(
                        "este binario no expone '{nombre}'. Tiene: {}",
                        self.nombres().join(", ")
                    ))
                })?;
                println!("{}", p.fuente_json());
                Ok(())
            }
            Some("run") => {
                let (nombre, resto) = self.elegir_dag(&args[1..])?;
                let p = self.pipelines.get(&nombre).ok_or_else(|| {
                    Error::Uso(format!(
                        "este binario no tiene un pipeline '{nombre}'. Tiene: {}",
                        self.nombres().join(", ")
                    ))
                })?;
                p.correr_desde_cli(resto)
            }
            Some("--version") | Some("-V") => {
                println!("bundle de {} pipelines (contrato {CONTRATO})", self.len());
                Ok(())
            }
            Some("--help") | Some("-h") | None => {
                println!("{}", self.ayuda());
                Ok(())
            }
            Some(otro) => Err(Error::Uso(format!(
                "no entiendo '{otro}'.\n\n{}",
                self.ayuda()
            ))),
        }
    }

    /// Saca `--dag <nombre>` de los argumentos y devuelve el resto.
    ///
    /// Con un solo pipeline el flag es opcional: no hay ambiguedad que resolver.
    fn elegir_dag(&self, args: &[String]) -> Result<(String, Vec<String>)> {
        let mut nombre: Option<String> = None;
        let mut resto = Vec::new();
        let mut i = 0;
        while i < args.len() {
            if args[i] == "--dag" {
                nombre =
                    Some(args.get(i + 1).cloned().ok_or_else(|| {
                        Error::Uso("a --dag le falta el nombre del pipeline".into())
                    })?);
                i += 2;
            } else {
                resto.push(args[i].clone());
                i += 1;
            }
        }

        match nombre {
            Some(n) => Ok((n, resto)),
            None if self.len() == 1 => Ok((self.nombres()[0].to_string(), resto)),
            None => Err(Error::Uso(format!(
                "este binario tiene {} pipelines: hace falta --dag <nombre>. Tiene: {}",
                self.len(),
                self.nombres().join(", ")
            ))),
        }
    }

    fn ayuda(&self) -> String {
        let listado: Vec<String> = self
            .pipelines
            .values()
            .map(|p| {
                format!(
                    "  {:28} {:>2} tareas  {}",
                    p.dag().name,
                    p.dag().tasks.len(),
                    p.dag().schedule.as_deref().unwrap_or("(bajo demanda)")
                )
            })
            .collect();
        format!(
            "bundle de {} pipelines de zeruel\n\
             \n\
             USO:\n  \
               <bin> run --dag <NOMBRE> --param <VALOR> --state <DIR>\n     \
                                 [--workers N] [--ram-max 2G] [--run-id UUID]\n  \
               <bin> --describe        todos los pipelines, como JSON\n  \
               <bin> --version\n\
             \n\
             PIPELINES:\n{}\n\
             \n\
             Si esta en el directorio de pipelines, `zeruel serve` los lanza solos.",
            self.len(),
            listado.join("\n")
        )
    }
}

impl Pipeline {
    /// Corre este pipeline con los argumentos de `run` (sin el `--dag`).
    pub(crate) fn correr_desde_cli(&self, args: Vec<String>) -> Result<()> {
        let opts = crate::RunArgs::parse(&args)?;
        crate::init_logs();
        let store = Arc::new(
            Store::open(&opts.state).map_err(|e| Error::Ejecucion(e.context("abrir el state")))?,
        );
        let rep = self.run_con(
            store,
            &opts.param,
            opts.workers,
            opts.run_id,
            opts.ram_max_mb,
        )?;
        println!(
            "{}",
            serde_json::json!({
                "run_id": rep.run_id.to_string(),
                "dag": self.dag().name,
                "ok": rep.success,
                "cancelado": rep.cancelled,
                "por_techo_de_memoria": rep.paso_el_techo,
                "filas": rep.total_rows,
                "reutilizadas": rep.skipped,
            })
        );
        if rep.success {
            Ok(())
        } else if rep.cancelled {
            std::process::exit(EXIT_CANCELADO);
        } else {
            std::process::exit(EXIT_FALLO);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pipeline, Task, TaskContext, TaskId, TaskOutput, TaskResult};

    struct Falsa(&'static str);
    impl Task for Falsa {
        fn id(&self) -> TaskId {
            TaskId(self.0.into())
        }
        fn run(&self, _c: &TaskContext) -> TaskResult {
            Ok(TaskOutput {
                rows: 1,
                message: "ok".into(),
            })
        }
    }

    fn uno(nombre: &str) -> Pipeline {
        let dag = crate::Dag {
            name: nombre.into(),
            tasks: vec![crate::TaskDef::new("a", vec![])],
            schedule: None,
            max_retries: 3,
        };
        pipeline(dag).register(Falsa("a")).build().unwrap()
    }

    #[test]
    fn describe_lista_todos_los_pipelines() {
        let b = bundle().con(uno("norte")).con(uno("sur"));
        let d = b.describe();
        assert_eq!(d["zeruel_contract"], CONTRATO);
        let ps = d["pipelines"].as_array().unwrap();
        assert_eq!(ps.len(), 2);
        // Orden estable (BTreeMap): el scheduler no ve el listado bailando.
        assert_eq!(ps[0]["name"], "norte");
        assert_eq!(ps[1]["name"], "sur");
    }

    #[test]
    fn con_varios_pipelines_hace_falta_elegir() {
        let b = bundle().con(uno("norte")).con(uno("sur"));
        let e = b
            .main_con(vec!["run".into(), "--param".into(), "p".into()])
            .unwrap_err()
            .to_string();
        assert!(e.contains("--dag"), "{e}");
        assert!(e.contains("norte"), "lista los que tiene: {e}");
    }

    #[test]
    fn con_uno_solo_el_flag_es_opcional() {
        let b = bundle().con(uno("norte"));
        let (n, resto) = b.elegir_dag(&["--param".into(), "p".into()]).unwrap();
        assert_eq!(n, "norte");
        assert_eq!(resto, vec!["--param", "p"]);
    }

    #[test]
    fn un_dag_que_no_existe_lista_los_que_si() {
        let b = bundle().con(uno("norte"));
        let e = b
            .main_con(vec![
                "run".into(),
                "--dag".into(),
                "no-existe".into(),
                "--param".into(),
                "p".into(),
                "--state".into(),
                "s".into(),
            ])
            .unwrap_err()
            .to_string();
        assert!(e.contains("no-existe"), "{e}");
        assert!(e.contains("Tiene: norte"), "{e}");
    }

    #[test]
    fn dag_sin_valor_lo_dice() {
        let b = bundle().con(uno("norte")).con(uno("otro"));
        let e = b.elegir_dag(&["--dag".into()]).unwrap_err().to_string();
        assert!(e.contains("le falta el nombre"), "{e}");
    }

    #[test]
    fn un_bundle_vacio_no_arranca_en_silencio() {
        let e = bundle()
            .main_con(vec!["--describe".into()])
            .unwrap_err()
            .to_string();
        assert!(e.contains("no tiene ningun pipeline"), "{e}");
        assert!(e.contains("generación"), "sugiere donde mirar: {e}");
    }

    #[test]
    fn nombres_repetidos_no_duplican_entradas() {
        let b = bundle().con(uno("norte")).con(uno("norte"));
        assert_eq!(b.len(), 1, "queda uno solo");
    }

    #[test]
    fn la_ayuda_lista_los_pipelines_con_su_schedule() {
        let b = bundle().con(uno("norte"));
        let h = b.ayuda();
        assert!(h.contains("norte"), "{h}");
        assert!(h.contains("--dag"), "{h}");
        assert!(h.contains("bajo demanda"), "{h}");
    }
}

#[cfg(test)]
mod tests_fuente {
    use super::*;
    use crate::{dag, pipeline, Task, TaskContext, TaskId, TaskOutput, TaskResult};

    struct Falsa(&'static str);
    impl Task for Falsa {
        fn id(&self) -> TaskId {
            TaskId(self.0.into())
        }
        fn run(&self, _c: &TaskContext) -> TaskResult {
            Ok(TaskOutput {
                rows: 1,
                message: "ok".into(),
            })
        }
    }

    fn armar() -> Bundle {
        let uno = pipeline(dag!("uno", "t" => []))
            .fuente(("uno.rs", "fn main() { /* el de uno */ }"))
            .register(Falsa("t"))
            .build()
            .unwrap();
        let dos = pipeline(dag!("dos", "t" => []))
            .fuente(("dos.rs", "fn main() { /* el de dos */ }"))
            .register(Falsa("t"))
            .build()
            .unwrap();
        bundle().con(uno).con(dos)
    }

    // Un bundle expone varios pipelines y cada uno empota SU codigo. Sin
    // --dag contestaba por el primero: mostrar el codigo equivocado al lado
    // de una corrida es peor que no mostrar nada.
    #[test]
    fn fuente_devuelve_la_del_dag_pedido() {
        let b = armar();
        for (nombre, marca) in [("uno", "el de uno"), ("dos", "el de dos")] {
            let p = b.pipelines.get(nombre).unwrap();
            let j = p.fuente_json();
            let txt = serde_json::to_string(&j).unwrap();
            assert!(txt.contains(marca), "{nombre} devolvio: {txt}");
        }
    }

    // El harness tiene que ACEPTAR --fuente. Antes caia en "comando
    // desconocido" y el scheduler lo reportaba como binario viejo.
    #[test]
    fn el_bundle_entiende_fuente() {
        let err = armar()
            .main_con(vec!["--fuente".into(), "--dag".into(), "nohay".into()])
            .unwrap_err()
            .to_string();
        assert!(
            !err.contains("desconocido") || err.contains("nohay"),
            "--fuente no llego al handler: {err}"
        );
    }
}
