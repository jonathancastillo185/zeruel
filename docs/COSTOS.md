# Qué consume zeruel

Todo medido, no estimado. La idea es que nada sorprenda: si vas a poner esto en
un VPS chico, acá está lo que ocupa antes de procesar un solo dato.

Medido el 2026-08-20, Windows 11, binarios release.

## En reposo (el costo de estar prendido)

Con `zeruel serve` y `zeruel web` levantados y **ningún run corriendo**:

| Proceso | RAM | Qué hace mientras no pasa nada |
|---|---:|---|
| `zeruel serve` | **38,7 MB** | Escanea el directorio de pipelines, evalúa cron, consulta la cola |
| `zeruel web` | **10,4 MB** | Espera requests HTTP |
| **Total** | **49,0 MB** | |

El estado en disco arranca en **0,2 MB** (SQLite + WAL) y crece con el historial;
la GC automática lo poda (30 días de detalle, 1 año de runs, agregados mensuales
para siempre).

Para poner el número en contexto: un Airflow mínimo —scheduler, webserver,
worker, Postgres y Redis— arranca en el orden de **1 a 2 GB** antes de correr
nada. Acá son 49 MB.

### Qué hace en cada tick

`serve` despierta cada `tick_ms` (1000 ms por defecto). En cada vuelta:

1. Recoge subprocesos terminados (`try_wait`, no bloquea).
2. Re-escanea el directorio de pipelines. **Los `--describe` van a caché por
   fecha de modificación**: sólo le pregunta a un binario si el archivo cambió.
   Sin ese caché serían N procesos por segundo.
3. Evalúa el cron de cada pipeline contra el minuto actual.
4. Consulta la cola (`pick_next`, una query).

Con runs en curso el tick baja a 250 ms para reaccionar rápido a una
cancelación. Sin nada que hacer, se queda en el tick configurado.

## Tamaño de los binarios

| Binario | Tamaño | Qué trae |
|---|---:|---|
| `zeruel` (scheduler + web + MCP + CLI) | **97,2 MB** | Todos los features: polars, wasmtime, SQLite |
| Un pipeline sin polars (`--features shell`) | **3,0 MB** | Engine, store y tareas de shell |
| Un bundle de 8 pipelines | **3,0 MB** | El mismo: agrupar no cuesta tamaño |

Los 97 MB del binario grande son casi todos polars y wasmtime. Un pipeline que
no toca dataframes pesa **32 veces menos**, y ese es el que se copia al servidor
por cada automatización.

## Costo por dependencia

Medido con `cargo tree`, dependencias únicas:

| Configuración | Deps | Contra el default |
|---|---:|---:|
| `zeruel` sin features | **64** | — |
| `+ shell` | 65 | +1 |
| `+ http` | 86 | +23 |
| `+ postgres` | 117 | +53 |
| `+ polars` | 295 | +231 |
| `+ cloud` (S3/Azure/GCS) | 297 | +2 |
| `+ snowflake` | — | +2 |
| `full` | 367 | +303 |

Dos cosas que valen la pena:

- **La nube sale casi gratis** (+5 crates contando los transitivos): polars ya
  compila `object_store` para leer Parquet, actives o no el feature.
- **Postgres cuesta caro** (+53, y trae tokio) porque no hay cliente maduro en
  Rust sin él. Sólo lo paga quien lo activa.

## Tiempos

| Operación | Tiempo |
|---|---:|
| Arranque de un proceso de pipeline | **170 ms** |
| Un `--describe` (sólo si el binario cambió) | ~170 ms |
| Compilar un pipeline nuevo desde cero (sin polars) | **80 s** |
| Recompilar ese pipeline tras un cambio | segundos |
| Pipeline ETL de 10M de filas | **6,5 s**, pico de 833 MB |

Los 170 ms de arranque son el precio de un proceso por run. Sobre un ETL de
segundos o minutos es ruido; sería un problema sólo si alguna vez se quisieran
tareas de milisegundos.

## Cómo re-medirlo

Los números de arriba salen de:

```bash
# reposo: levantar serve y web, esperar, y mirar el working set
zeruel serve --state ./state --pipelines ./pipelines &
zeruel web   --state ./state --port 9876 &
tasklist /FI "IMAGENAME eq zeruel.exe"        # Windows
ps -o rss=,comm= -C zeruel                     # Linux

# deps por configuración
cargo tree -p zeruel --features <combo> --edges normal | grep -oE "[a-z0-9_-]+ v[0-9]" | sort -u | wc -l

# un run, con su pico por tarea
zeruel --json status --state state --run latest
```

**Tomá la mediana de varias corridas.** El pico de RAM de un mismo pipeline varió
entre 679 y 1252 MB en tres mediciones seguidas: el RSS depende de cuándo el
allocator devuelve memoria y de qué más esté haciendo la máquina.
