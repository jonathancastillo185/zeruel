//! `HttpTask`: bajar de una API o una URL a un archivo, en streaming.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use zeruel_sdk::{Task, TaskContext, TaskId, TaskOutput};

/// Baja el contenido de una URL a un archivo.
///
/// **Va en streaming, de a pedazos.** Una respuesta de 3 GB se escribe al disco
/// sin pasar entera por la memoria, que es lo que permite que un pipeline con
/// un techo de RAM chico igual baje archivos grandes.
///
/// La URL y el destino admiten `{param}`, que se reemplaza por el parametro del
/// run — que en un ETL diario suele ser la fecha:
///
/// ```no_run
/// use zeruel_tasks::HttpTask;
///
/// let t = HttpTask::new("bajar", "https://api.ejemplo.com/ventas?dia={param}")
///     .a_archivo("data/crudo-{param}.json")
///     .header_de_entorno("Authorization", "API_TOKEN")
///     .timeout_secs(120);
/// ```
///
/// Los secretos se leen del entorno con [`HttpTask::header_de_entorno`]: un
/// token escrito en el codigo termina en el repositorio y en los logs.
pub struct HttpTask {
    id: TaskId,
    url: String,
    metodo: String,
    destino: Option<PathBuf>,
    headers: Vec<(String, String)>,
    headers_env: Vec<(String, String)>,
    cuerpo: Option<String>,
    timeout: Duration,
    codigos_ok: Vec<u16>,
    min_bytes: u64,
}

impl HttpTask {
    /// Una tarea que baja `url`.
    pub fn new(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: TaskId(id.into()),
            url: url.into(),
            metodo: "GET".into(),
            destino: None,
            headers: Vec::new(),
            headers_env: Vec::new(),
            cuerpo: None,
            timeout: Duration::from_secs(120),
            codigos_ok: Vec::new(),
            min_bytes: 0,
        }
    }

    /// Metodo HTTP (por defecto `GET`).
    pub fn metodo(mut self, m: impl Into<String>) -> Self {
        self.metodo = m.into();
        self
    }

    /// Archivo donde escribir la respuesta. Admite `{param}`.
    pub fn a_archivo(mut self, p: impl Into<PathBuf>) -> Self {
        self.destino = Some(p.into());
        self
    }

    /// Un header fijo.
    pub fn header(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.headers.push((k.into(), v.into()));
        self
    }

    /// Un header cuyo valor sale de una variable de entorno.
    ///
    /// Para tokens y claves: no quedan en el codigo, ni en el repositorio, ni
    /// en el mensaje de error si la tarea falla.
    pub fn header_de_entorno(mut self, header: impl Into<String>, var: impl Into<String>) -> Self {
        self.headers_env.push((header.into(), var.into()));
        self
    }

    /// Cuerpo de la request (para POST/PUT). Admite `{param}`.
    pub fn cuerpo(mut self, c: impl Into<String>) -> Self {
        self.cuerpo = Some(c.into());
        self
    }

    /// Tope de tiempo (por defecto 120 s).
    pub fn timeout_secs(mut self, s: u64) -> Self {
        self.timeout = Duration::from_secs(s);
        self
    }

    /// Codigos HTTP que cuentan como exito. Por defecto, cualquier 2xx.
    pub fn codigos_ok(mut self, c: impl IntoIterator<Item = u16>) -> Self {
        self.codigos_ok = c.into_iter().collect();
        self
    }

    /// Falla si la respuesta trae menos bytes que esto.
    ///
    /// Sirve contra el fallo silencioso mas comun de un ETL: la API contesta
    /// 200 con un cuerpo vacio o un `[]`, el pipeline sigue contento, y recien
    /// se descubre dias despues que faltan datos.
    pub fn minimo_bytes(mut self, n: u64) -> Self {
        self.min_bytes = n;
        self
    }

    fn expandir(&self, s: &str, param: &str) -> String {
        s.replace("{param}", param)
    }

    /// Pone los headers en la request, venga con cuerpo o sin el.
    fn headers<B>(
        &self,
        mut req: ureq::RequestBuilder<B>,
        param: &str,
    ) -> anyhow::Result<ureq::RequestBuilder<B>> {
        for (k, v) in &self.headers {
            req = req.header(k, &self.expandir(v, param));
        }
        for (header, var) in &self.headers_env {
            let valor = std::env::var(var).map_err(|_| {
                anyhow::anyhow!("falta la variable de entorno {var}, que aporta el header {header}")
            })?;
            req = req.header(header, &valor);
        }
        Ok(req)
    }
}

impl Task for HttpTask {
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    fn run(&self, ctx: &TaskContext) -> anyhow::Result<TaskOutput> {
        let url = self.expandir(&self.url, &ctx.param);
        let t0 = Instant::now();

        let agente = ureq::Agent::config_builder()
            .timeout_global(Some(self.timeout))
            .build()
            .new_agent();

        // ureq 3 usa tipos distintos para una request con cuerpo y sin cuerpo,
        // asi que las dos ramas se arman por separado y los headers se ponen
        // con un helper generico.
        let cuerpo = self.cuerpo.as_ref().map(|c| self.expandir(c, &ctx.param));
        let metodo = self.metodo.to_uppercase();
        let respuesta = match (metodo.as_str(), cuerpo) {
            ("GET", None) => {
                let req = self.headers(agente.get(&url), &ctx.param)?;
                req.call()
            }
            ("DELETE", None) => {
                let req = self.headers(agente.delete(&url), &ctx.param)?;
                req.call()
            }
            ("POST", c) => {
                let req = self.headers(agente.post(&url), &ctx.param)?;
                req.send(c.unwrap_or_default())
            }
            ("PUT", c) => {
                let req = self.headers(agente.put(&url), &ctx.param)?;
                req.send(c.unwrap_or_default())
            }
            (m, Some(_)) => anyhow::bail!("{m} con cuerpo no esta soportado"),
            (m, None) => anyhow::bail!("metodo HTTP no soportado: {m}"),
        };

        let mut respuesta = match respuesta {
            Ok(r) => r,
            Err(ureq::Error::StatusCode(code)) => {
                anyhow::bail!("{} {url} devolvio HTTP {code}", self.metodo)
            }
            Err(e) => anyhow::bail!("{} {url} fallo: {e}", self.metodo),
        };

        let codigo = respuesta.status().as_u16();
        let ok = if self.codigos_ok.is_empty() {
            (200..300).contains(&codigo)
        } else {
            self.codigos_ok.contains(&codigo)
        };
        if !ok {
            anyhow::bail!("{} {url} devolvio HTTP {codigo}", self.metodo);
        }

        // Streaming: de a 64 KB, mirando la cancelacion entre pedazo y pedazo.
        // Asi una descarga larga se corta al instante en vez de tener que
        // terminar antes de enterarse.
        let destino = self
            .destino
            .as_ref()
            .map(|d| PathBuf::from(self.expandir(&d.to_string_lossy(), &ctx.param)));
        if let Some(d) = &destino {
            if let Some(padre) = d.parent() {
                std::fs::create_dir_all(padre).ok();
            }
        }

        let mut salida: Box<dyn Write> = match &destino {
            Some(d) => Box::new(std::io::BufWriter::new(
                std::fs::File::create(d)
                    .map_err(|e| anyhow::anyhow!("no puedo escribir {}: {e}", d.display()))?,
            )),
            None => Box::new(std::io::sink()),
        };

        let mut lector = respuesta.body_mut().as_reader();
        let mut buf = vec![0u8; 64 * 1024];
        let mut bytes = 0u64;
        loop {
            if ctx
                .cancel
                .as_ref()
                .is_some_and(|c| c.load(Ordering::SeqCst))
            {
                anyhow::bail!("cancelado tras bajar {bytes} bytes");
            }
            let n = lector.read(&mut buf)?;
            if n == 0 {
                break;
            }
            salida.write_all(&buf[..n])?;
            bytes += n as u64;
        }
        salida.flush()?;

        if bytes < self.min_bytes {
            anyhow::bail!(
                "la respuesta trajo {bytes} bytes y se esperaban al menos {}. \
                 Suele ser una API que contesta 200 con el cuerpo vacio.",
                self.min_bytes
            );
        }

        Ok(TaskOutput {
            rows: bytes,
            message: match &destino {
                Some(d) => format!(
                    "{} bytes en {:.1}s -> {}",
                    bytes,
                    t0.elapsed().as_secs_f64(),
                    d.display()
                ),
                None => format!("{bytes} bytes en {:.1}s", t0.elapsed().as_secs_f64()),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn ctx() -> TaskContext {
        TaskContext {
            run_id: uuid::Uuid::new_v4(),
            param: "2026-08-19".into(),
            cancel: Some(Arc::new(AtomicBool::new(false))),
        }
    }

    #[test]
    fn expande_el_param_en_la_url_y_en_el_destino() {
        let t = HttpTask::new("t", "https://x.com/v?d={param}").a_archivo("out/{param}.json");
        assert_eq!(
            t.expandir(&t.url, "2026-08-19"),
            "https://x.com/v?d=2026-08-19"
        );
        assert_eq!(
            t.expandir("out/{param}.json", "2026-08-19"),
            "out/2026-08-19.json"
        );
    }

    #[test]
    fn un_metodo_raro_se_rechaza_antes_de_salir_a_la_red() {
        let e = HttpTask::new("t", "https://x.invalido")
            .metodo("TELEPORT")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("no soportado"), "{e}");
        assert!(e.contains("TELEPORT"), "{e}");
    }

    #[test]
    fn falta_el_token_lo_dice_por_su_nombre() {
        std::env::remove_var("TOKEN_QUE_NO_EXISTE_XYZ");
        let e = HttpTask::new("t", "https://x.invalido")
            .header_de_entorno("Authorization", "TOKEN_QUE_NO_EXISTE_XYZ")
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("TOKEN_QUE_NO_EXISTE_XYZ"), "{e}");
        assert!(
            e.contains("Authorization"),
            "dice que header queda sin valor: {e}"
        );
    }

    #[test]
    fn un_host_que_no_resuelve_da_un_error_con_la_url() {
        let e = HttpTask::new("t", "https://host-que-no-existe-jamas-12345.invalid/x")
            .timeout_secs(5)
            .run(&ctx())
            .unwrap_err()
            .to_string();
        assert!(e.contains("host-que-no-existe"), "{e}");
        assert!(e.contains("GET"), "dice el metodo: {e}");
    }
}
