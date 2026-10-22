# bench — scripts del benchmark contra Python

Los scripts que produjeron las tablas comparativas del `README.md` de la raiz.

Hasta 2026-08-18 estaban en `.gitignore` (`bench_*.py`), asi que los numeros
publicados no se podian reproducir ni auditar desde el repo (N-50). Ahora estan
versionados: si un numero se cita, el script que lo midio esta al lado.

## Que hay

| Archivo | Que hace |
|---|---|
| `bench_direct.py` | El pipeline ETL completo en pandas y en polars-python. Mide wall por etapa y total. |
| `bench_direct_pandas.py` | La variante solo-pandas. |
| `bench_mem.py` | Sampler de RAM: suma el `VmRSS` de **todos** los procesos cada 100ms y guarda el pico. |
| `bench_sampler.ps1` | Pico de RAM de un contenedor Docker visto desde Windows (`docker stats`). |

## Como se corrieron (y por que las tablas no son comparables todavia)

Esto es lo que hay que arreglar antes de volver a citar los numeros (N-19):

- **Los scripts de Python corrieron dentro del contenedor Linux de Airflow.**
  Se ve en las rutas: `BASE = "/opt/airflow/bench_data"`. No es un detalle
  menor: contenedor vs nativo, y otro sistema de archivos.
- **zeruel se midio nativo en Windows 11.** Distinto SO, distinto I/O.
- **La RAM se midio distinto en cada lado**: `bench_mem.py` suma todos los
  procesos del contenedor; el pico de zeruel es el working set de un proceso.
- **`zeruel puro` usaba streaming y el pipeline real no**, asi que la conclusion
  de "el overhead de la orquestacion es 0.62s (11%)" mezclaba dos cosas. Desde
  2026-08-18 los dos usan streaming (N-18), asi que **ese numero hay que
  volver a medirlo**.

## Como re-medir en serio

1. Un solo sistema para los tres (o Linux nativo, o los tres en contenedor).
2. Mismo dataset, generado una vez: `zeruel gen --root data --n 10000000`.
3. Misma medicion de memoria para todos (el working set del proceso que hace
   el trabajo, o la suma de todos, pero igual en los tres).
4. Varias corridas, y reportar la mediana — no la mejor.
5. Recien ahi, actualizar la tabla del README y anotar las condiciones.
