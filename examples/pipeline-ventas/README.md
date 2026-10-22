# pipeline-ventas — un pipeline de zeruel, de punta a punta

Ejemplo mínimo pero completo de lo que es un pipeline con zeruel: **un crate
propio, con su propio binario**, que usa `zeruel` como librería.

```
  bajar ──► limpiar ──► cargar
```

Es un crate independiente a propósito (tiene `[workspace]` vacío en su
`Cargo.toml`, que lo desengancha del workspace de zeruel). Así se ve un proyecto
tuyo, no una parte de zeruel.

## Probarlo

```bash
cd examples/pipeline-ventas

# correrlo suelto
cargo run -- run --param 2026-08-19 --state ./state

# el grafo, como lo ve el scheduler
cargo run -- --describe

# la ayuda
cargo run -- --help
```

Sale un `datos/totales-2026-08-19.csv` y el estado del run queda en `./state`.
Si lo corrés dos veces con el mismo `--param`, la segunda no re-ejecuta nada:
`skipped` en el JSON de salida muestra las tareas reutilizadas del run anterior.
Eso es la durabilidad por checkpoint, y no hay que hacer nada para tenerla.

## Ponerlo a correr solo

```bash
cargo build --release
cp target/release/pipeline-ventas ../../pipelines/
zeruel serve --state ./state
```

No hay que registrarlo en ningún lado ni recompilar zeruel. El scheduler escanea
el directorio, le pregunta a cada binario quién es (`--describe`) y lo lanza
cuando corresponde — por cron o porque alguien lo encoló.

## Lo que hay que mirar del código

**El grafo se declara y las tareas se registran por separado**, pero el id sale
del propio `Task`, así que no hay forma de asociar una implementación a la clave
equivocada:

```rust
zeruel::pipeline(dag)
    .register(Bajar { dir: dir.clone() })
    .register(Limpiar { dir: dir.clone() })
    .register(Cargar { dir })
    .start()
```

Si falta una tarea o el grafo tiene un ciclo, falla **al construir**, con el
nombre de lo que falta — no a mitad del run y no después de haber escrito datos.

**`start()` es lo que le da al binario su interfaz.** Vos no escribís CLI: el
harness implementa `--describe` y `run --param ... --state ...`, que es el
contrato que usa `zeruel serve` para orquestarlo sin saber nada de tu código.

**Una tarea es un `impl Task`**: devuelve cuántas filas procesó y un mensaje.
Esos dos valores son los que después se ven en `zeruel status`, en la UI y en las
métricas históricas.

## Lo que NO hace falta

- Declarar el DAG en YAML o TOML. El grafo y la lógica van en Rust, con el
  compilador chequeando. La configuración que cambia entre entornos (conexiones,
  rutas, horarios) sí va en archivo.
- Registrar el pipeline en zeruel. Es un binario suelto en un directorio.
- Recompilar zeruel, ni los otros pipelines, para agregar este.
