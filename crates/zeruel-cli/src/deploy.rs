//! Fase 7 — distribucion portable
//!
//!   zeruel self-test  : verifica el binario end-to-end (gen -> run -> status -> gc)
//!                     en un directorio temporal. Sale 0 si todo funciona.
//!   zeruel deploy     : genera una carpeta portable (binario + wrappers + docs
//!                     + opcional dataset/estado/Dockerfile/zip) lista para
//!                     copiar a otra maquina y usar en <10 min.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::dataset::Dataset;
use crate::pipeline;
use std::io::Write;
use zeruel_core::Store;
use zeruel_data::DataEnv;

// ---------------------------------------------------------------- self-test

pub fn self_test(n: u32, workers: usize, keep: bool) -> Result<()> {
    let dir = std::env::temp_dir().join(format!("zeruel-self-test-{}", uuid::Uuid::new_v4()));
    let root = dir.join("data");
    let state = dir.join("state");
    std::fs::create_dir_all(&state).context("crear dir temporal")?;
    let mut t = Vec::new();

    // 1. dataset sintetico chico
    let t0 = std::time::Instant::now();
    let env = DataEnv::new(&root);
    let ds = Dataset::con_transacciones(n);
    let filas = ds.generate(&env).context("gen dataset")?;
    t.push(format!(
        "gen: {filas} filas en {:.2}s",
        t0.elapsed().as_secs_f64()
    ));

    // 2. pipeline completo
    let t0 = std::time::Instant::now();
    let store = std::sync::Arc::new(Store::open(&state).context("abrir store")?);
    let rep = pipeline::run(&env, &store, workers, "self-test", None, None).context("pipeline")?;
    t.push(format!(
        "run: {} en {:.2}s ({} filas, {} skip)",
        if rep.success { "OK" } else { "FAIL" },
        t0.elapsed().as_secs_f64(),
        rep.total_rows,
        rep.skipped
    ));

    // 3. verificar contra el store (lo que da `zeruel status`)
    let run = store
        .recent_runs(1, 0)?
        .into_iter()
        .next()
        .context("run no guardado en el store")?;
    if !rep.success || run.state != zeruel_core::store::RunState::Success {
        anyhow::bail!("self-test FAIL: run {} no termino en Success", run.id);
    }
    let tasks = store.task_runs(run.id)?;
    if tasks.len() != 6 {
        anyhow::bail!("self-test FAIL: se esperaban 6 tasks, hay {}", tasks.len());
    }
    let ok_tasks = tasks
        .iter()
        .filter(|x| matches!(x.state, zeruel_sdk::TaskState::Success))
        .count();
    if ok_tasks != 6 {
        anyhow::bail!("self-test FAIL: solo {ok_tasks}/6 tasks en Success");
    }
    t.push(format!("status: run {} con 6/6 tasks OK", run.id));

    // 4. GC dry-run (verifica integridad de tablas de higiene)
    let gcrep = store.gc(true).context("gc")?;
    t.push(format!(
        "gc: db={:.1}MB (dry-run)",
        gcrep.db_size_bytes as f64 / 1_048_576.0
    ));

    if !keep {
        std::fs::remove_dir_all(&dir).ok();
        eprintln!("self-test dir limpiado (--keep para inspeccionar)");
    } else {
        eprintln!("self-test dir: {}", dir.display());
    }
    eprintln!(
        "SELF-TEST OK: binario integro, todo verde\n  - {}",
        t.join("\n  - ")
    );
    Ok(())
}

// ----------------------------------------------------------------- deploy

pub struct DeployArgs {
    pub dest: PathBuf,
    pub data: Option<PathBuf>,
    pub state: Option<PathBuf>,
    pub docker: bool,
    pub zip: bool,
}

pub fn deploy(a: DeployArgs) -> Result<()> {
    std::fs::create_dir_all(&a.dest).context("crear dest")?;
    let is_win = cfg!(windows);
    let bin_name = if is_win { "zeruel.exe" } else { "zeruel" };

    // 1. binario (el propio que corre este comando: portable, 0 deps)
    let exe = std::env::current_exe().context("current_exe")?;
    let bin_dest = a.dest.join(bin_name);
    std::fs::copy(&exe, &bin_dest).with_context(|| format!("copiar {}", exe.display()))?;
    let bin_mb = std::fs::metadata(&bin_dest)?.len() as f64 / 1_048_576.0;
    println!("[1/4] binario: {} ({:.0} MB)", bin_name, bin_mb);

    // 2. wrappers: SIEMPRE los dos (N-11). Antes se escribia solo el del SO
    //    actual, pero el README que se genera y el mensaje final ofrecen los
    //    dos, y un paquete armado en Windows terminaba sin `run.sh`.
    let run_name = if is_win { "run.ps1" } else { "run.sh" };
    std::fs::write(a.dest.join("run.ps1"), RUN_PS1)?;
    std::fs::write(a.dest.join("run.sh"), RUN_SH)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for f in ["run.sh"] {
            let p = a.dest.join(f);
            if p.exists() {
                let mut perms = std::fs::metadata(&p)?.permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&p, perms)?;
            }
        }
    }

    // 3. dataset de prueba y estado (opcionales)
    if let Some(d) = &a.data {
        if d.exists() {
            copiar_contenido(d, &a.dest.join("data"))?;
            println!("[2/4] data: copiada de {}", d.display());
        } else {
            println!(
                "[2/4] data: la carpeta no existe ({}) — se genera con 'zeruel gen'",
                d.display()
            );
        }
    } else {
        println!("[2/4] data: omitida (--data para incluirla)");
    }
    if let Some(s) = &a.state {
        if s.exists() {
            copiar_contenido(s, &a.dest.join("state"))?;
            println!("[3/4] state: copiado de {}", s.display());
        } else {
            println!(
                "[3/4] state: la carpeta no existe ({}) — se crea con el primer run",
                s.display()
            );
        }
    } else {
        println!("[3/4] state: omitido (--state para incluirlo)");
    }

    // 4. docs + docker
    let readme = readme_deport(
        bin_name,
        bin_mb,
        a.data.as_deref().map(|_| true).unwrap_or(false),
    );
    std::fs::write(a.dest.join("README.md"), readme)?;
    println!("[4/4] README.md: quickstart <10 min");
    if a.docker {
        // La imagen es Linux: el binario que copia tiene que ser el ELF, no el
        // .exe de Windows (N-11). Antes se usaba `bin_name`, asi que un deploy
        // hecho en Windows generaba un Dockerfile que copiaba `zeruel.exe` a
        // debian y la imagen no arrancaba.
        std::fs::write(a.dest.join("Dockerfile"), dockerfile("zeruel"))?;
        std::fs::write(a.dest.join("docker-run.sh"), DOCKER_RUN_SH)?;
        println!("      Dockerfile + docker-run.sh (imagen runtime)");
        if is_win {
            println!("      OJO: el Dockerfile espera un binario Linux llamado `zeruel`.");
            println!("           El que se copio aca es {bin_name} (Windows): para armar");
            println!("           la imagen, compila el target linux (build_releases.ps1).");
        }
    }
    if a.zip {
        let nombre = format!("zeruel-portable-{}.zip", std::env::consts::OS);
        let out = a.dest.parent().unwrap_or(Path::new(".")).join(nombre);
        let res = zip_carpetas(&a.dest, &out);
        match res {
            Ok(sz) => println!(
                "[5/5] zip: {} ({:.0} MB)",
                out.display(),
                sz as f64 / 1_048_576.0
            ),
            Err(e) => eprintln!("      aviso: no se pudo crear el zip: {e}"),
        }
    }
    println!();
    println!("deploy listo en {}", a.dest.display());
    println!("  primer paso:  .\\{run_name} self-test   (o  bash run.sh self-test)");
    Ok(())
}

/// Copia el CONTENIDO de src a dst (no anida: src/* -> dst/*).
fn copiar_contenido(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        if e.file_type()?.is_dir() {
            copiar_contenido(&e.path(), &dst.join(e.file_name()))?;
        } else {
            std::fs::copy(e.path(), dst.join(e.file_name()))?;
        }
    }
    Ok(())
}

fn zip_carpetas(carpeta: &Path, out: &Path) -> Result<u64> {
    let f = std::fs::File::create(out).context("crear zip")?;
    let mut z = zip::ZipWriter::new(f);
    let opciones: zip::write::FileOptions<()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    let base = carpeta.parent().unwrap_or(Path::new("."));
    let mut stack = vec![carpeta.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(d)? {
            let e = e?;
            let rel = e.path().strip_prefix(base)?.to_string_lossy().to_string();
            if e.file_type()?.is_dir() {
                stack.push(e.path());
            } else {
                z.start_file(rel.as_str(), opciones)?;
                std::io::copy(&mut std::fs::File::open(e.path())?, z.by_ref())?;
            }
        }
    }
    let f = z.finish()?;
    let sz = f.metadata()?.len();
    Ok(sz)
}

fn readme_deport(bin: &str, bin_mb: f64, con_data: bool) -> String {
    let first = if con_data {
        "bash run.sh once   # hay dataset incluido en data/"
    } else {
        "bash run.sh gen 10000000   # dataset sintetico de 10M (~60s)\nbash run.sh once"
    };
    format!(
        "# zeruel — distribucion portable\n\n\
Binario unico ({bin}, {bin_mb:.0} MB), cero dependencias. Cero setup: \
lo que ves es todo lo que necesita.\n\n\
## Quickstart (<10 min)\n\n\
```bash\n{first}\n```\n\n\
O desde PowerShell (Windows):\n\n\
```powershell\n{ps_first}\n```\n\n\
## Comandos utiles\n\n\
| comando | que hace |\n\
|---|---|\n\
| `{bin} gen --root data --n 10000000` | dataset sintetico (10M, seed fija) |\n\
| `{bin} run --root data --state state` | pipeline ETL completo |\n\
| `{bin} status --state state` | ultimo run + metricas por task + higiene |\n\
| `{bin} list --state state` | runs recientes (paginado) |\n\
| `{bin} gc --state state` | purga por retencion (30d tasks / 1a runs / agregados) |\n\
| `{bin} enqueue --dag ecommerce-etl --param X --priority 5` | encolar para el scheduler |\n\
| `{bin} serve --root data --state state` | daemon scheduler (cola + GC diaria) |\n\
| `{bin} web --port 8787` | UI web embebida |\n\
| `{bin} mcp` | MCP server por stdio (IA) |\n\
| `{bin} self-test` | verifica el binario end-to-end |\n\n\
## Estructura\n\n\
```\n\
data/input/*.parquet   entrada (gen)\n\
data/output/*.parquet  intermedias + rollups\n\
state/zeruel.db          metadata SQLite (runs, tasks, cola, agregados)\n\
state/logs/zeruel.log.*  logs rotados (14d)\n\
```\n\n\
## Higiene (por defecto, no configurable)\n\n\
- Retencion: task_runs 30d, runs 1a, agregados mensuales permanentes\n\
- GC: manual (`{bin} gc`) o automatica diaria dentro de `serve`\n\
- Logs: rotacion diaria, 14d de retencion\n",
        ps_first = if con_data {
            ".\\run.ps1 once   # hay dataset incluido en data\\"
        } else {
            ".\\run.ps1 gen 10000000   # dataset sintetico de 10M (~60s)\n.\\run.ps1 once"
        },
    )
}

fn dockerfile(bin: &str) -> String {
    format!(
        "# zeruel — imagen runtime (el binario ya viene compilado)\n\
# build:  docker build -t zeruel .\n\
# correr: docker run -v $PWD:/zeruel -p 8787:8787 zeruel\n\
FROM debian:bookworm-slim\n\
WORKDIR /zeruel\n\
COPY {bin} /usr/local/bin/zeruel\n\
RUN chmod +x /usr/local/bin/zeruel && mkdir -p /zeruel/data /zeruel/state\n\
VOLUME [\"/zeruel/data\", \"/zeruel/state\"]\n\
EXPOSE 8787\n\
HEALTHCHECK --interval=30s CMD zeruel --json status --state /zeruel/state > /dev/null 2>&1 || exit 1\n\
ENTRYPOINT [\"zeruel\"]\n\
CMD [\"serve\", \"--root\", \"/zeruel/data\", \"--state\", \"/zeruel/state\"]\n"
    )
}

const RUN_SH: &str = r#"#!/bin/bash
# zeruel — wrapper (Fase 7). Uso:
#   bash run.sh self-test            verificar instalacion
#   bash run.sh gen 10000000         dataset sintetico
#   bash run.sh once                 un run ETL y sale
#   bash run.sh serve                scheduler daemon
#   bash run.sh web                  UI web (http://localhost:8787)
#   bash run.sh <cualquier comando de zeruel>
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
NODO="$DIR/zeruel"
case "$1" in
  once)
    shift
    exec "$NODO" run --root "$DIR/data" --state "$DIR/state" "$@"
    ;;
  self-test)
    shift
    exec "$NODO" self-test "$@"
    ;;
  *)
    exec "$NODO" "$@"
    ;;
esac
"#;

const RUN_PS1: &str = r#"# zeruel — wrapper (Fase 7). Uso:
#   .\run.ps1 self-test            verificar instalacion
#   .\run.ps1 gen 10000000         dataset sintetico
#   .\run.ps1 once                 un run ETL y sale
#   .\run.ps1 serve                scheduler daemon
#   .\run.ps1 web                  UI web (http://localhost:8787)
#   .\run.ps1 <cualquier comando de zeruel>
$ErrorActionPreference = "Stop"
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$zeruel = Join-Path $dir "zeruel.exe"
$first = $args[0]
$rest = @()
if ($args.Count -ge 2) { $rest = $args[1..($args.Count-1)] }
if ($first -eq 'once') {
    & $zeruel run --root (Join-Path $dir 'data') --state (Join-Path $dir 'state') @rest
    exit $LASTEXITCODE
}
if ($first -eq 'self-test') {
    & $zeruel self-test @rest
    exit $LASTEXITCODE
}
& $zeruel @args
exit $LASTEXITCODE
"#;

const DOCKER_RUN_SH: &str = r#"#!/bin/bash
# build + corrida rapida de la imagen runtime
docker build -t zeruel .
docker run --rm -it -v "$PWD:/zeruel" -p 8787:8787 zeruel "$@"
"#;
