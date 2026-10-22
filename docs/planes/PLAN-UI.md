# Plan: completar la interfaz de zeruel

Relevado el 2026-08-24 mirando la vista de DAG contra la instalación real de
`C:\zeruel` (56 DAGs, 100+ corridas). No está escrito sobre supuestos: cada
punto sale de algo que se verificó en el código o en la API.

El disparador fue abrir `scraping_precios` y ver la pantalla vacía. Investigando
eso aparecieron **dos bugs** que ninguna funcionalidad nueva arregla, y por eso
van primero.

---

## Índice

1. [Los dos bugs que hay que arreglar antes](#1-los-dos-bugs-que-hay-que-arreglar-antes)
2. [Qué falta, y cuánto cuesta cada cosa](#2-qué-falta-y-cuánto-cuesta-cada-cosa)
3. [Fase 1 — Que la vista de DAG diga la verdad](#fase-1--que-la-vista-de-dag-diga-la-verdad)
4. [Fase 2 — Los controles que faltan](#fase-2--los-controles-que-faltan)
5. [Fase 3 — Pausar un DAG](#fase-3--pausar-un-dag)
6. [Fase 4 — Ver por qué falló](#fase-4--ver-por-qué-falló)
7. [Fase 5 — Ver el código](#fase-5--ver-el-código)
8. [Lo que dejo afuera y por qué](#lo-que-dejo-afuera-y-por-qué)
9. [Cómo verificar cada fase](#cómo-verificar-cada-fase)

---

## 1. Los dos bugs que hay que arreglar antes

### Bug A — Un DAG con corridas se ve como si nunca hubiera corrido

**Dónde:** `web.rs:409`, función `runs()`.

```rust
let r = store.recent_runs(100, 0).unwrap_or_default();   // las ultimas 100 de TODO
let arr = r.iter()
    .filter(|r| dag.map(|d| r.dag == d).unwrap_or(true))  // y recien aca filtra
```

**Qué pasa:** trae las últimas 100 corridas **de todos los DAGs** y recién
después filtra por el que se pidió. Con 56 DAGs, las 100 más recientes las
copan los que corren cada 10 minutos. Cualquier DAG cuya última corrida quedó
fuera de esa ventana devuelve `[]`.

**Verificado:** `/api/runs?dag=scraping_precios` → `[]`, con 100 corridas en la
base y el DAG desplegado y agendado.

**Por qué importa más de lo que parece:** no es una tabla vacía, es una
**mentira**. La pantalla dice "este DAG nunca corrió" cuando en realidad corrió
y falló. Y es justo el momento en que alguien la mira.

**Arreglo:** que el filtro baje al SQL. `recent_runs(limit, offset)` pasa a
aceptar un `dag: Option<&str>` y filtra en el `WHERE`. El límite se aplica
después del filtro, no antes.

### Bug B — El grafo de la vista de DAG queda vacío

**Verificado:** `/api/graph?dag=scraping_precios` devuelve los 3 nodos y sus 2
aristas. O sea que **el dato está**; lo que no lo dibuja es la interfaz.

La causa más probable es que `pintarLandingDag()` corta antes de llegar a
`pintarGrafo()` cuando algo falla arriba — y con el bug A, la lista de corridas
viene vacía. Hay que confirmarlo con la consola del navegador antes de tocar
nada: puede ser una excepción silenciada.

---

## 2. Qué falta, y cuánto cuesta cada cosa

Ordenado por lo que ya existe abajo, que es lo que decide el esfuerzo real:

| Falta | ¿Hay backend? | Esfuerzo |
|---|---|---|
| Filtro de corridas por DAG | hay que bajarlo al SQL | **chico** |
| Botón de cancelar | **sí**, `POST /api/run/<id>/cancel` | **chico** |
| Reintentar una corrida | **sí**, `POST /api/retry` | **chico** |
| Ver el log de una corrida | los archivos existen, falta el endpoint | **medio** |
| Pausar / activar un DAG | **no existe nada** | **medio** |
| Ver el código del pipeline | **no existe nada** | **grande** |

Lo que más sorprende del relevamiento: **cancelar y reintentar ya funcionan por
API y no tienen botón**. Son las dos cosas más baratas de la lista.

---

## Fase 1 — Que la vista de DAG diga la verdad

*Arregla los dos bugs. Sin esto, todo lo demás se construye sobre una pantalla
que miente.*

1. `Store::recent_runs()` acepta un filtro por DAG y lo aplica en el `WHERE`.
   Un test que lo fije: insertar 150 corridas de `a` y 1 de `b`, pedir las de
   `b`, y que aparezca. Hoy ese test falla.
2. `web.rs::runs()` pasa el DAG al store en vez de filtrar después.
3. Diagnosticar el grafo vacío con la consola abierta y arreglar lo que salga.
4. La vista de DAG deja de filtrar sobre `d.runs` (la lista global) y pide
   `/api/runs?dag=X`, que ahora sí contesta bien.

**Se termina cuando:** abrir `scraping_precios` muestra sus corridas y su grafo.

---

## Fase 2 — Los controles que faltan

*Todo esto es interfaz sobre API que ya existe. Es la fase con mejor relación
entre lo que cuesta y lo que da.*

### En la tabla de corridas

| Control | Cuándo aparece | Qué hace |
|---|---|---|
| **Cancelar** | solo si está corriendo | `POST /api/run/<id>/cancel` |
| **Reintentar** | solo si falló o se canceló | `POST /api/retry` |
| **Ver detalle** | siempre | despliega la fila |

La regla: **un control que no se puede usar no se muestra**. Un botón de
cancelar sobre una corrida terminada, aunque esté deshabilitado, obliga a
leerlo para descubrir que no sirve.

### En el encabezado del DAG

Además de "Ejecutar", que ya está:

- **Pausar / Activar** (fase 3)
- **Ver el código** (fase 5)

### Detalle a cuidar

La cancelación **viaja por la base**, no por el proceso: el scheduler la ve en
su próximo tick. O sea que el botón no puede quedar en "cancelando…" para
siempre si el scheduler está caído. Después de pedirla, la fila pasa a
"cancelación pedida" y se resuelve sola en el refresco siguiente.

---

## Fase 3 — Pausar un DAG

*Lo primero de la lista que necesita backend nuevo. No hay nada parecido hoy:
se buscó `pausado`, `activo`, `enabled` y `habilitado` en el store y no existe
el concepto.*

### Qué significa "pausado"

Hay que definirlo antes de escribirlo, porque las tres opciones se comportan
distinto y solo una es la que se espera:

| Interpretación | Qué pasa con el cron | Qué pasa con lo encolado | Qué pasa con lo que corre |
|---|---|---|---|
| **A. No se agenda** | no dispara | **sigue** | **sigue** |
| B. No se agenda ni se lanza | no dispara | espera en cola | sigue |
| C. Todo frenado | no dispara | espera | se cancela |

**Se recomienda la A**, que es lo que hace Airflow al pausar un DAG. Pausar es
"dejá de agendarlo", no "matá lo que está haciendo". Para eso último está
cancelar, que es una acción distinta y ya existe.

### Implementación

- Una clave en la tabla `meta` (`pausado:<dag>`), que es donde ya viven las
  cancelaciones. No hace falta una tabla nueva.
- `tick_cron()` saltea los pausados.
- `/api/dags` devuelve `pausado: bool`.
- `POST /api/dag/<nombre>/pausar` y `.../activar`.
- En la interfaz: un interruptor en la fila y en el encabezado del DAG. Un DAG
  pausado se ve **atenuado** en la lista, no escondido: esconderlo hace que
  alguien lo busque diez minutos.

### Lo que no puede pasar

Que pausar un DAG **borre** su agenda. El cron vive en el `--describe` del
binario; la pausa es un estado aparte que lo suspende. Al activarlo de nuevo,
vuelve el horario que tenía sin que nadie lo reescriba.

---

## Fase 4 — Ver por qué falló

*La pregunta que más se hace frente a esta pantalla, y hoy hay que ir al
servidor a responderla.*

### El log de cada corrida

Los archivos ya existen: `state/logs/run-<uuid>.log`, uno por corrida. Lo que
falta es servirlos.

- `GET /api/run/<uuid>/log` con las últimas N líneas y un `?desde=` para
  paginar hacia atrás.
- **Se lee desde el final.** Un log de un scraper de dos horas puede pesar
  decenas de MB, y lo que se busca está siempre al final. Leer el archivo
  entero para mostrar 50 líneas es la forma de que la interfaz se cuelgue justo
  cuando más se la necesita.
- En la interfaz: el log se despliega en la fila de la corrida, con filtro por
  texto y un botón para descargarlo entero.

### El error, arriba de todo

Cuando una corrida falla, lo que importa es **la línea del error**, no las
15.000 anteriores. La vista de detalle tiene que abrir mostrando el error y la
tarea que lo tiró, con el log completo abajo para el que quiera mirar.

---

## Fase 5 — Ver el código · HECHA

*Era la más cara y la que tenía la decisión de fondo pendiente.*

La maqueta ya está diseñada (`design/Codigo.dc.html`): el código del pipeline a
la izquierda, el grafo a la derecha, y al tocar un nodo se marcan sus renglones.

**El problema es de dónde sale el código.** El binario del pipeline no lo lleva
adentro. Tres caminos:

| Cómo | A favor | En contra |
|---|---|---|
| **Empotrarlo en el binario** (`include_str!`) | siempre coincide con lo que corrió | el autor del pipeline tiene que acordarse |
| Guardarlo al lado del `.exe` | no toca la librería | se puede desincronizar del binario |
| Leerlo del repositorio | cero despliegue | muestra lo de HOY, no lo que corrió |

**La tercera es la peligrosa** y se descartó: mostrar el código actual al lado
de una corrida de hace tres días es peor que no mostrar nada, porque parece
confiable.

**Se implementó la primera.** El pipeline captura su código al compilar con una
línea:

```rust
pipeline(dag).fuente(zeruel::fuente!())
```

El macro expande a `include_str!` contra `CARGO_MANIFEST_DIR`, así que son
exactamente los bytes que el compilador leyó para producir ese ejecutable: no
puede mentir. El binario gana `--fuente`, el scheduler lo sirve en
`/api/codigo?dag=X`, y la interfaz lo muestra numerado.

Es **opcional**: sin esa línea el pipeline anda igual y la interfaz dice que no
hay código empotrado, con la línea que hay que agregar. No se hizo automático
porque empotrar el código de todos los archivos sin que nadie lo pida engorda
cada binario sin avisar.

---

## Lo que dejo afuera y por qué

- **Editar el cron desde la interfaz.** El horario vive en el código del
  pipeline. Cambiarlo desde la web crearía una segunda fuente de verdad que se
  desincroniza del binario en la primera recompilada.
- **Crear un DAG desde la interfaz.** Un pipeline es un crate de Rust que se
  compila. No hay forma honesta de hacerlo desde un formulario.
- **Editar el código y recompilar.** Sería una IDE. No es esto.
- **Autenticación por usuario.** Hoy hay un token para todo o nada. Usuarios y
  permisos es otro proyecto, y solo hace falta si esto se expone a un equipo.

---

## Cómo verificar cada fase

| Fase | Se termina cuando |
|---|---|
| 1 | `scraping_precios` muestra sus corridas y su grafo |
| 2 | Se cancela una corrida en curso desde la interfaz y la fila lo refleja |
| 3 | Un DAG pausado no dispara por cron, y al activarlo vuelve a su horario |
| 4 | Se ve el error de una corrida fallida sin abrir una terminal |
| 5 | El código que se ve es el que corrió, no el del repositorio de hoy |

La verificación que vale para todas: **abrir la interfaz sin saber nada del
sistema y poder contestar "¿por qué falló esto?"** sin entrar al servidor. Hoy
no se puede, y ese es el punto de todo el plan.

---

## Orden sugerido

1 y 2 primero, juntas: arreglan la mentira y aprovechan API que ya existe. Con
eso la pantalla ya es útil. 3, 4 y 5 después, en ese orden, cada una con más
backend nuevo que la anterior.
