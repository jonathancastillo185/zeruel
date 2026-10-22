# Changelog

Los cambios notables de cada versión. Formato basado en
[Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/); el proyecto usa
[versionado semántico](https://semver.org/lang/es/) (en 0.x la API puede
cambiar entre versiones menores).

La historia detallada del desarrollo previo a la publicación está en
[docs/HISTORIAL.md](docs/HISTORIAL.md).

## [Sin publicar]

## [0.1.0] — primera versión pública

### Incluye

- Librería `zeruel` para declarar DAGs en Rust (`dag!`, `pipeline()`,
  `bundle()`), con *features* opcionales: `shell`, `http`, `polars`, `sql`,
  `wasm`, `postgres`, `snowflake`, `cloud`, `full`.
- Binario `zeruel`: scheduler multiproceso (`serve`), panel web (`web`),
  servidor MCP (`mcp`), instalador y servicios de Windows, `deploy`,
  `self-test`, `doctor` y `config`.
- Durabilidad por checkpoint, reintentos con backoff, cron, backfill, SLA de
  espera, cola con prioridad y envejecimiento, control de admisión por RAM y
  techo de memoria por corrida (Windows).
- Cancelación entre procesos que corta el árbol de procesos de la tarea.
- Métricas por tarea con agregados mensuales y limpieza automática por
  retención.
- Pipeline ETL de ejemplo con generador de datos sintéticos y ejemplos de
  pipelines independientes.

### Cambios respecto del desarrollo privado

- El workspace pasó a la raíz del repositorio y los documentos internos se
  movieron a `docs/`.
- Los ejemplos, tests y documentos usan nombres genéricos de DAG; se
  quitaron los bocetos de diseño y los planes internos de una instalación
  concreta.
- `packaging/zeruel.service`: la unidad systemd había quedado a medio
  renombrar (usuario y binario `nodo`, ejecutable `zeruel`) y no arrancaba.
- `check.ps1` no corría en PowerShell 5.1 por un carácter no ASCII.
- Se agregó CI en GitHub Actions (fmt, clippy y tests en Linux, Windows y
  macOS).
