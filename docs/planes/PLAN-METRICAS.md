# Plan: que la pestaña de Métricas sirva para algo

Relevado el 2026-08-24 contra la instalación real de `C:\zeruel` (783 corridas,
2.050 tareas, 47 scrapers, 14 horas de historia). Cada número de acá salió de
consultar esa base o de medir los procesos vivos, no de leer el código y
suponer.

El disparador fue la pregunta obvia: **por qué la tabla principal está vacía.**
Investigando eso aparecieron cinco problemas más, y uno de ellos **bloquea el
filtro de RAM** que hay que agregar.

---

## Índice

1. [Lo que muestra hoy, medido](#1-lo-que-muestra-hoy-medido)
2. [Los seis problemas](#2-los-seis-problemas)
3. [El que bloquea todo lo demás: la RAM está mal medida](#3-el-que-bloquea-todo-lo-demás-la-ram-está-mal-medida)
4. [Fase 1 — Que la tabla deje de estar vacía](#fase-1--que-la-tabla-deje-de-estar-vacía)
5. [Fase 2 — Medir la RAM de verdad](#fase-2--medir-la-ram-de-verdad)
6. [Fase 3 — Filtros](#fase-3--filtros)
7. [Fase 4 — Las preguntas que hoy no se pueden contestar](#fase-4--las-preguntas-que-hoy-no-se-pueden-contestar)
8. [Fase 5 — Limpiar lo que miente](#fase-5--limpiar-lo-que-miente)
9. [Lo que dejo afuera y por qué](#lo-que-dejo-afuera-y-por-qué)
10. [Cómo verificar cada fase](#cómo-verificar-cada-fase)

---

## 1. Lo que muestra hoy, medido

Tres tablas, ningún filtro. `GET /api/metrics` **no acepta un solo parámetro**.

| Tabla | Qué trae hoy en `C:\zeruel` |
|---|---|
| **Volumen mensual por DAG** | **0 filas** (con 783 corridas en la base) |
| **Estadísticas por tarea** | 7 filas, con 45 DAGs colapsados adentro |
| **SLA configurados** | 0 filas |

O sea: de las tres tablas de la pestaña, **dos están vacías y la tercera
promedia cosas que no se pueden comparar entre sí.**

---

## 2. Los seis problemas

| # | Problema | Evidencia |
|---|---|---|
| **A** | La tabla principal está vacía **por diseño** hasta que cierre un mes | `rollups_monthly` = 0 filas con 783 corridas |
| **B** | **La RAM está 10× mal**: mide el wrapper, no el trabajo | reporta 10 MB; los procesos reales usan 100-169 MB |
| **C** | Una tarea de 45 DAGs distintos colapsa en **una sola fila** | `scrape` = 614 ejecuciones de 45 DAGs → 1 renglón |
| **D** | `filas prom.` es **0 en las 2.050 filas** | columna muerta que ocupa lugar |
| **E** | **Cero filtros**: ni fecha, ni DAG, ni RAM, ni estado | el endpoint no toma parámetros |
| **F** | Los **reintentos consumidos no se registran** | `retries_left = 0` en las 2.050 filas |

### A — Vacía por diseño

`rollups()` lee `rollups_monthly`, una tabla pre-agregada que llena `zeruel gc`
con un `WHERE mes < mes_actual`. **El mes en curso está excluido a propósito.**

Consecuencia: una instalación nueva no muestra nada **hasta que termine el mes**
—que es exactamente lo que notaste—, y en régimen tampoco muestra nunca el mes
que estás viviendo, que es el que importa.

Hay un comentario en `store.rs:893` que dice que esto ya se arregló (N-40).
Se arregló **a medias**: antes hacía falta un año de historia, ahora hace falta
un mes. El bug que quedó es el mismo, más corto.

**El error de fondo es la granularidad.** El mes es la unidad correcta para
*archivar* historia vieja; es la unidad equivocada para *mirar* qué está
pasando. Tus datos duran 14 horas y ya tenés preguntas que hacerles.

### C — El promedio que no se puede usar

```
tarea 'scrape':   aparece en 45 DAGs distintos, 614 ejecuciones -> 1 sola fila
tarea 'init_db':  aparece en 45 DAGs distintos, 614 ejecuciones -> 1 sola fila
tarea 'avisar':   aparece en 42 DAGs distintos, 653 ejecuciones -> 1 sola fila
```

Todos tus scrapers tienen las mismas tres tareas. Promediar `scrape` sobre 45
sitios distintos mezcla uno que tarda 4.400 s con otro que tarda 30 s. El
promedio resultante no describe a ninguno.

Y es justo el dato que se necesita: **cuál scraper es el pesado.**

### F — Los reintentos son invisibles

`retries_left` sólo se escribe en el registro final de la tarea, y ahí siempre
vale 0 (o salió bien, o agotó los intentos). Una tarea que **falló dos veces y
salió a la tercera** queda registrada igual que una que salió a la primera.

Un DAG que anda "bien" a costa de reintentar siempre es un DAG que está por
romperse, y hoy no hay forma de verlo.

---

## 3. El que bloquea todo lo demás: la RAM está mal medida

Pediste un filtro por consumo de RAM. **No se puede construir sobre el número
que hay hoy**, porque el número está mal.

`metrics.rs:101` samplea `Pid::from_u32(std::process::id())`: **el propio
proceso, nunca los hijos.** Tus pipelines lanzan `python.exe` para hacer el
trabajo real, así que lo que se mide es el wrapper de Rust esperando.

Medido en vivo, con seis scrapers corriendo:

```
  python 29868  RSS  169 MB   (padre: python.exe)
  python  5868  RSS  165 MB   (padre: python.exe)
  python 33416  RSS  150 MB   (padre: python.exe)
  python 40832  RSS  101 MB   (padre: python.exe)
  python 16548  RSS    4 MB   (padre: scraping.exe)

  zeruel reporta para 'scrape':  RAM pico 11 MB
```

Dos cosas para notar:

1. **Más de 10× de diferencia** entre lo reportado y lo real.
2. **El trabajo está en los nietos**, no en los hijos: `scraping.exe` lanza un
   `python.exe` de 4 MB que lanza otro `python.exe` de 169 MB. Recorrer sólo
   los hijos directos tampoco alcanza — hay que bajar el árbol entero.

**Esto además hace que el techo de memoria no proteja nada.** El `--tope-mb`
compara contra el RSS del wrapper, que nunca se mueve de 10 MB. Un pipeline que
se come toda la RAM de la máquina jamás va a cruzar su propio techo.

Por eso la Fase 2 va antes que el filtro de RAM: filtrar por una columna que
miente sólo sirve para tomar decisiones equivocadas con más confianza.

---

## Fase 1 — Que la tabla deje de estar vacía · HECHA

*Sin esto no hay nada que filtrar.*

El cambio de fondo: **las métricas se calculan sobre `runs` y `task_runs`, no se
leen de una tabla pre-agregada.** `rollups_monthly` deja de ser la fuente y pasa
a ser lo que siempre debió ser: el archivo de lo que ya se borró.

1. `metricas(desde, hasta, …)` en el store, agregando por **día** con
   `strftime('%Y-%m-%d', …)`, filtrando en el `WHERE`. El agregado por mes se
   deriva de ahí cuando el rango es largo, no al revés.
2. Para rangos que se pasan del año de retención, **unir** con
   `rollups_monthly`: el detalle ya no existe, el agregado sí. La UI marca esas
   filas como archivadas para que nadie las confunda con datos vivos.
3. La tabla principal pasa a llamarse **Volumen por período**, y el período lo
   elige el filtro (Fase 3), con **hoy** como valor por defecto.

**Se termina cuando:** una instalación recién creada, con una corrida, muestra
esa corrida en la tabla. Hoy muestra el cartel de "sin meses cerrados todavía".

---

## Fase 2 — Medir la RAM de verdad · HECHA

*Prerrequisito del filtro de RAM, y arregla el techo de memoria de paso.*

1. El sampler recorre el **árbol de descendientes**, no sólo el proceso propio.
   `sysinfo` ya expone `p.parent()` y `salud.rs` ya arma parentesco así; acá hay
   que hacerlo **transitivo** (hijos, nietos, bisnietos) porque el caso real es
   `scraping.exe → python.exe → python.exe`.
2. El pico pasa a ser **la suma del RSS del árbol** en cada muestra, no el
   máximo de cada rama por separado: lo que importa es cuánta RAM tenía tomada
   la máquina por culpa de ese run en el peor instante.
3. **Cuidado con el doble conteo en Linux**: padre e hijo comparten páginas
   copy-on-write después de un `fork`, así que sumar RSS sobrevalúa. Se
   documenta como cota superior; medir PSS es caro y no vale la pena acá.
4. El techo de memoria (`--tope-mb`) empieza a comparar contra ese número. **Esto
   cambia el comportamiento en producción**: un pipeline que hoy nunca cruza su
   techo puede empezar a cortarse. Va con una nota en el manual y conviene
   desplegarlo mirando.
5. Un test que fije la regresión: lanzar un hijo que reserve N MB y verificar que
   el pico medido lo incluya. Hoy ese test falla.

**Se termina cuando:** un scraper de 169 MB se reporta con tres cifras, no con
dos.

---

## Fase 3 — Filtros · HECHA

*Lo que pediste. Va después de 1 y 2 porque filtrar datos vacíos o falsos no
sirve.*

Una sola fila de controles arriba de las tres tablas, que aplica a todas:

| Filtro | Opciones | Por qué |
|---|---|---|
| **Fecha** | hoy · 7 días · 30 días · este mes · rango a mano | lo que pediste; **hoy** por defecto |
| **DAG** | buscador con autocompletado | con 47 DAGs, una lista desplegable no sirve |
| **RAM** | mayor a N MB | lo que pediste; recién tiene sentido con la Fase 2 |
| **Estado** | todas · sólo con fallos | ir directo a lo que está roto |
| **Duración** | mayor a N segundos | encontrar al lento sin ordenar 47 filas a ojo |

Decisiones:

- **El filtro viaja en la URL** (`#met?desde=…&dag=…`). Un enlace a "los fallos
  de ayer de scraping_a" tiene que poder pegarse en un chat.
- **El filtro va al SQL, no al JavaScript.** Es el mismo error que ya arreglamos
  dos veces (N-36, y el bug A de `PLAN-UI.md`): traer 2.000 filas para mostrar
  10 funciona hasta que deja de funcionar, y falla justo cuando hay volumen.
- **El período por defecto es hoy**, no todo. Abrir la pestaña tiene que ser
  barato y contestar "¿cómo venimos?", no "¿qué pasó desde que existe esto?".

---

## Fase 4 — Las preguntas que hoy no se pueden contestar · HECHA

*Todo esto sale de datos que **ya están en la base**. No hace falta medir nada
nuevo: hace falta mostrarlo.*

### 4.1 Cuál DAG es el lento

```
scraping_d        1 corrida    prom 4407s   peor 4407s
scraping_e      6 corridas   prom 3983s   peor 4458s
scraping_f      1 corrida    prom 3508s   peor 3508s
```

Duración promedio y peor caso por DAG. Con **p50 y p95** en vez de sólo el
promedio: un scraper que normalmente tarda 30 s y a veces 40 minutos tiene un
promedio que no describe ninguno de los dos casos.

### 4.2 Cuál DAG falla más

```
scraping_b           6/6 = 100% de fallo
scraping_a           2/2 = 100% de fallo
scraping_c           2/2 = 100% de fallo
financiacion         2/3 =  67% de fallo
```

**Esos cuatro están rotos ahora mismo en tu servidor y la pestaña de Métricas no
lo dice.** Tasa de fallo por DAG, ordenada por la peor, es probablemente la
tabla más útil de todo el plan.

### 4.3 Tarea por DAG, no tarea sola

Partir la tabla de tareas por `(dag, tarea)` en vez de por `tarea`. Eso convierte
7 filas inútiles en la respuesta a "qué parte de un scraper tarda 4.000
segundos".

Con 47 DAGs × 3 tareas son ~140 filas: por eso el filtro de la Fase 3 y el
orden por defecto de peor a mejor.

### 4.4 Cómo viene el día

```
15h: #########   78 corridas, 0 fallos
17h: ########### 91 corridas, 0 fallos
21h: ##########  80 corridas, 0 fallos
```

Corridas por hora/día, con los fallos apilados. Es el único elemento **visual**
del plan y va con una condición: que se lea de un vistazo o no va. La barra de
"cuándo se rompió" contra "cuándo corrimos más" es la que contesta sin leer
números.

*(Nota: acá se aplica la guía de `dataviz` — una escala, sin doble eje, paleta
validada. El grafo del panel se sacó por inútil; este gráfico entra sólo si
contesta algo que la tabla no.)*

### 4.5 Reintentos

Requiere **una columna nueva** (`intentos` en `task_runs`), porque hoy el dato
se pierde: ver el problema F. Es el único punto del plan que toca el esquema.

Con eso se puede mostrar "salió bien, pero al tercer intento", que es el aviso
temprano de un DAG que se está por romper.

---

## Fase 5 — Limpiar lo que miente · HECHA

- **Sacar la columna `filas prom.`** o llenarla. Está en 0 en las 2.050 filas
  porque las tareas que envuelven Python nunca reportan filas. Una columna que
  siempre dice 0 entrena a la gente a ignorar la tabla entera.
  - Si se llena: el wrapper de Python tiene que devolver el conteo, y eso es
    trabajo en el proyecto que lo usa en producción, no acá.
  - **Recomendado**: sacarla del proyecto genérico y que aparezca sólo cuando
    hay algún valor distinto de 0.
- **`SLA configurados`, vacía**, sin decir cómo se llena. El cartel de vacío
  tiene que traer el comando (`zeruel sla --dag X --max-wait N`), como ya hace
  el subtítulo.
- **Los subtítulos mencionan la implementación**: "agregados de
  `rollups_monthly`" no le dice nada a nadie que no haya leído el código.

---

## Lo que dejo afuera y por qué

- **Exportar a CSV / Prometheus.** Vale la pena, pero es otra conversación:
  primero que los números sean correctos. Exportar un número mal medido lo
  propaga a un dashboard donde nadie lo va a auditar.
- **Alertas por umbral** ("avisame si un DAG tarda más que X"). Se pisa con el
  SLA que ya existe; conviene extender eso, no abrir un mecanismo nuevo.
- **Retención configurable.** Los 30 días / 1 año están fijos en `gc()`. Es
  razonable hacerlo configurable, pero no es lo que falta para que la pestaña
  informe.
- **Comparar contra el período anterior** ("20% más lento que la semana pasada").
  Es lo natural después de la Fase 3, pero antes hay que tener un período que
  funcione.

---

## Cómo verificar cada fase

| Fase | Se termina cuando |
|---|---|
| 1 | Una instalación nueva con una corrida la muestra en la tabla |
| 2 | Un scraper de 169 MB se reporta con tres cifras |
| 3 | Un enlace a "fallos de ayer de scraping_a" se puede pegar en un chat |
| 4 | La pestaña dice que `scraping_b` falla 6 de 6 |
| 5 | Ninguna columna visible está siempre en 0 |

La verificación que vale para todas: **abrir Métricas y poder decidir qué
arreglar primero.** Hoy la pestaña no contesta eso — muestra dos tablas vacías y
un promedio de 45 cosas distintas.

---

## Orden sugerido

**1 y 2 primero**, porque son los que hacen que los datos existan y sean
ciertos. **3 después**, que es lo que pediste y ya tiene sobre qué operar. **4
al final**, que es donde está casi todo el valor pero también casi todo el
trabajo — y sus subfases son independientes entre sí, así que 4.2 (tasa de
fallo) se puede adelantar: es la más barata y la que más informa.

La 5 es media hora en cualquier momento.


---

## Lo que apareció al implementarlo

Tres cosas que el relevamiento no había previsto, todas encontradas **mirando la
pantalla**, no leyendo el código:

### El eje de tiempo mentía

La consulta devolvía sólo los períodos **con** corridas. Como el gráfico dibuja
una barra por elemento, un día con datos dentro de una ventana de siete se
mostraba como *una sola barra ocupando la semana entera*. Parecía que se había
trabajado los siete días.

Ahora la consulta rellena la rejilla completa: los períodos vacíos van en cero.
El avance es por fecha de calendario y no sumando 86.400 segundos, porque en los
cambios de horario un día dura 23 o 25 horas y el paso fijo se desalinea.

### La página medía 10.000 píxeles

Con 47 DAGs, las tres tablas sumaban 237 filas desplegadas. Quedó en 2.334 px
acotando cada tabla y aprovechando que el encabezado ya era `sticky`.

### Un nombre de clase pisado

La barra de tasa de fallo salía dorada en vez de roja: `.barra` ya existía en la
hoja de estilos para otra cosa, con el color de señal, y la regla de más abajo
ganaba. Renombrada a `.barra-fallo`.

---

## Lo que queda pendiente

- **Los números de RAM históricos siguen siendo los viejos.** El arreglo del
  sampler mide de acá en adelante; las corridas ya registradas conservan lo que
  midió el sampler anterior (9-11 MB donde había cientos). No se pueden
  recalcular: el dato correcto nunca se guardó.
- **El techo de memoria ahora compara contra el árbol.** Un pipeline que hoy
  nunca cruzaba su techo puede empezar a cortarse. Conviene mirarlo los primeros
  días.
- **`filas prom.` sigue sin llenarse** en el proyecto que lo usa en producción: la columna ahora
  aparece sólo si algo la llena, pero para que la llene hay que hacer que el
  wrapper de Python devuelva el conteo, y eso es trabajo del otro repo.
