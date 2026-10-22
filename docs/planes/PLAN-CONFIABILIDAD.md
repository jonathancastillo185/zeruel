# Plan: por qué zeruel se sigue cayendo, y qué hay que arreglar

Escrito el 2026-09-01, después de tres intentos fallidos de dejar zeruel
corriendo de forma estable en una instalación real. Cada intento arregló una
capa y ninguno tocó la causa.

Este documento existe porque el patrón importa más que cada bug suelto: **las
tres veces el sistema falló en silencio y culpó a otra cosa.**

---

## 1. Qué pasó, en orden

| Fecha | Cómo corría | Cómo murió | Cuánto estuvo caído |
|---|---|---|---|
| 30/08 18:42 | `zeruel serve` en una terminal | se cerró la terminal | 13 h |
| 31/08 20:54 | Tarea programada `-LogonType Interactive` | `0xC000013A` (CTRL_CLOSE) al cerrarse la consola que la hospedaba | 21 min de vida |
| 01/09 | Servicio de Windows | **no murió**: corre, pero la mitad de las corridas falla | en curso |

Los dos primeros son el mismo error con distinto disfraz: **el proceso colgaba
de una consola**. Pasar de una terminal a una tarea programada Interactive no
cambió nada de fondo — una tarea Interactive no es un servicio, corre dentro de
la sesión de escritorio y recibe `CTRL_CLOSE` igual.

El tercero es distinto y es el que importa.

---

## 2. La causa raíz del fallo actual

Medido, no deducido. La cadena completa:

```
  1. El servicio corre como NT SERVICE\zeruel  ✔ verificado
  2. C:\zeruel\zeruel.toml TIENE [entorno] con SCRAPER_BUN_DIR correcto  ✔
  3. Las variables de MÁQUINA también están bien puestas  ✔
  4. …pero el proceso NO las tiene
  5. → el pipeline cae a su valor por defecto: "/opt/scraper-bun"
  6. → ruta de Linux en Windows → ERROR_DIRECTORY (os error 267)
  7. → el mensaje dice: "no puedo ejecutar 'bun.exe': El nombre del
       directorio no es válido. ¿Está instalado y en el PATH?"
```

**Dos fallas independientes se suman, y la segunda esconde a la primera.**

### Falla A — el servicio ignora su propia configuración

`zeruel servicio correr` cargaba el `zeruel.toml` para sacar `workers`,
`paralelo` y `ram_max`, pero **nunca llamaba a `aplicar_entorno()`**. La sección
`[entorno]` —que es justamente donde viven las rutas de los intérpretes— se
leía y se tiraba.

Se intentó suplirlo con variables de entorno de máquina. No alcanza, y por una
razón que conviene dejar escrita porque no es obvia: **el Administrador de
Servicios de Windows cachea el bloque de entorno al arrancar la máquina.** Una
variable de máquina que se fija hoy no le llega a un servicio hasta el próximo
reinicio. Por eso el paso 3 y el paso 4 pueden ser ciertos al mismo tiempo.

O sea: para un servicio, **el `[entorno]` del archivo de configuración no es una
comodidad, es el único camino que funciona sin reiniciar la máquina.**

### Falla B — el mensaje de error acusa al inocente

El error real era el directorio de trabajo. El mensaje hablaba del programa y
preguntaba por el PATH. Con ese texto se revisó, en este orden: que `bun.exe`
existiera (existía), que estuviera en el PATH (irrelevante, la ruta era
absoluta), y los permisos de la cuenta del servicio (estaban bien).

El sistema **tenía el dato exacto** —`ERROR_DIRECTORY`, y el `cwd` que iba a
usar— y lo escondió detrás de una pregunta que no venía al caso.

Para un orquestador esto no es un detalle de cortesía: el mensaje de una tarea
fallida suele ser lo único que alguien va a leer.

---

## 3. El patrón, que es lo que hay que romper

Las tres caídas comparten forma:

1. **Falla en silencio.** El scheduler caído no reporta nada: no falla ningún
   run, simplemente no corre ninguno. Desde afuera se ve igual que "no había
   trabajo". Es la peor forma de romperse que tiene un orquestador.
2. **Nadie lo nota hasta que alguien mira.** Las 13 horas del 30/08 se
   descubrieron de casualidad.
3. **El diagnóstico apunta al lugar equivocado**, y se pierde tiempo en lo que
   estaba bien.

Un sistema que se propone ser confiable tiene que fallar **ruidosamente y
señalando**. Lo demás es suerte.

---

## Fase 1 — Que el servicio no dependa del ambiente · HECHA

1. `servicio instalar` **guarda la ruta del `zeruel.toml`** en los argumentos
   del servicio. Un servicio arranca desde `System32`, así que buscar el
   archivo "en el directorio actual" no encuentra nada, nunca.
2. Si no se pasa `--config`, se busca al lado del ejecutable, que es como queda
   una instalación normal.
3. `servicio correr` **aplica `[entorno]`** antes de lanzar nada.
4. Con `--config` explícito, si el archivo no se puede leer **el servicio no
   arranca**, en vez de arrancar con la configuración vacía. Un servicio que
   ignora su config en silencio es peor que uno que no arranca: los pipelines
   fallan uno por uno y el motivo aparece a nombre de ellos.
5. `zeruel web` también aplica `[entorno]`: arma su lista corriendo cada binario
   con `--describe`, y sin las variables un pipeline puede contestarle una cosa
   a la web y otra al scheduler.
6. El instalador le da a la cuenta del servicio permiso de **lectura** sobre el
   archivo de configuración, y avisa fuerte si no encontró ninguno.

## Fase 2 — Que el error señale al culpable · HECHA

`ShellTask` diagnostica antes de repetir el error del sistema:

| Situación | Antes | Ahora |
|---|---|---|
| `cwd` no existe | "no puedo ejecutar 'X'… ¿está en el PATH?" | "el directorio de trabajo no existe: '…'. El programa puede estar perfecto" |
| ruta absoluta que no existe | "…¿está instalado y en el PATH?" | "no existe el programa '…' (la ruta es absoluta, no se busca en el PATH)" |
| sin permisos | el error crudo | "sin permiso… si esto corre como servicio, su cuenta puede no tener acceso" |

Cuatro tests lo fijan, incluido uno que reproduce el caso exacto: un programa
que existe con un `cwd` que no.

## Fase 3 — Que el SCM lo levante solo · HECHA

`servicio instalar` configura el reinicio automático (5 s, 10 s, después cada
minuto; el contador se resetea cada día). Si falla, avisa y **no aborta la
instalación**: el servicio quedó bien, lo que falta es una comodidad.

---

## Fase 4 — Que una caída se note · A MEDIAS

### 4.0 Agrupar los fallos idénticos · HECHO

Cuando algo del entorno se rompe, *todos* los pipelines que lo usan fallan con
el mismo error. Reportados de a uno son 51 fallos sueltos y la sensación de que
"todo está roto"; agrupados son **un** problema con un mensaje concreto.

Contra los datos reales, la alerta nueva dice:

```
27 fallos en 27 DAGs distintos
   no puedo ejecutar 'bun.exe': El nombre del directorio no es válido
52 fallos en 6 DAGs
   python.exe termino con codigo 103 — No Python at '...'
```

El corte por **DAGs distintos** es lo que importa: repetido en uno solo es un
pipeline con un bug —y eso ya se ve en su tira de corridas del panel—; repetido
en varios es el entorno, y eso no lo mostraba nada.

### Lo que sigue faltando

El problema: hoy nada distingue "el scheduler está caído" de "no había trabajo".
Las dos cosas se ven igual — cero corridas.

Lo que haría falta, en orden de esfuerzo:

1. **Un latido.** El scheduler escribe su marca de vida en la base en cada tick.
   La web muestra en rojo "el scheduler no da señales desde hace N minutos".
   Es barato y convierte una falla invisible en una visible.
2. **Un chequeo de arranque.** Al levantar, el scheduler verifica que cada
   pipeline desplegado pueda al menos lanzarse —programa presente, `cwd`
   existente— y lo reporta de una vez, en vez de descubrirlo run por run
   durante horas. Esto habría atajado el fallo actual en el primer minuto.
3. **Un aviso hacia afuera.** Un webhook o un mail cuando el latido se corta.
   Sin esto, "se nota" sigue dependiendo de que alguien abra el panel.

## Fase 5 — Que la instalación sea de una sola vez · PENDIENTE

Hoy dejar zeruel andando en una máquina nueva son varios pasos, en orden, y
saltearse uno deja el sistema roto de una forma que no se ve. Debería ser:

```
zeruel init          # ya existe: pregunta los límites y escribe zeruel.toml
zeruel instalar      # NO existe: deja los servicios andando, con su config
```

`zeruel servicio instalar` ya hace casi todo, pero sigue pidiendo que quien
instala sepa que hay dos servicios (scheduler y web), que hay que pasarles
`--config`, y que hay que arrancarlos. Un comando que haga los dos, verifique
que arrancaron y **corra el chequeo del punto 4.2** cerraría el círculo.

---

## Lo que hay que hacer ahora, en la instalación real

Los servicios corren con el binario viejo. El binario nuevo ya está desplegado
en `C:\zeruel\zeruel.exe`, pero un servicio no cambia de binario hasta que se
lo reinicia. Hace falta una consola de administrador:

```powershell
Restart-Service zeruel, zeruel-web
```

Y después, para confirmar que el `[entorno]` llegó:

```powershell
# no deberia haber ningun "/opt/scraper-bun" en los fallos nuevos
zeruel status --state C:/zeruel/state
```

Si los servicios se instalaron antes de este arreglo, además hay que
reinstalarlos para que quede guardada la ruta del `zeruel.toml`:

```powershell
zeruel servicio desinstalar --nombre zeruel
zeruel servicio instalar --nombre zeruel `
    --state C:/zeruel/state --pipelines C:/zeruel/pipelines `
    --config C:/zeruel/zeruel.toml --gracia-secs 60
```

---

## Cómo saber que esto quedó resuelto

| Fase | Se termina cuando |
|---|---|
| 1 | Un servicio recién instalado corre un pipeline que usa `[entorno]`, sin reiniciar la máquina |
| 2 | Un `cwd` inexistente da un error que nombra al directorio y no al programa |
| 3 | Matar el proceso del servicio a mano y verlo volver solo |
| 4 | Apagar el scheduler y que el panel lo diga, sin que nadie mire los procesos |
| 5 | Instalar en una máquina limpia con dos comandos y que la primera corrida salga bien |

La prueba que vale para todas: **apagar la máquina, prenderla, y que zeruel esté
corriendo sin que nadie inicie sesión ni abra una consola.** Eso todavía no se
verificó.

---

## Apéndice — la tercera causa, encontrada el 01/09

Después de arreglar lo del `[entorno]`, los fallos cambiaron de mensaje pero no
se terminaron:

```
'.../venv/Scripts/python.exe' termino con codigo 103.
  No Python at 'C:\Users\<usuario>\AppData\Local\Programs\Python\Python312\python.exe'
```

El archivo **existía**. Lo que faltaba era el permiso, y sólo en un eslabón:

```
bun.exe                    -> NT SERVICE\zeruel   OK
venv/Scripts/python.exe    -> NT SERVICE\zeruel   OK
Python312\python.exe       -> NO LO TENIA                <-- aca
```

Al venv se le había dado acceso, pero **el venv es sólo un lanzador**: delega en
el Python base, al que nadie se lo dio. Windows reporta esa falta de permiso
como "No Python at…", que se lee como "no está".

Es la contracara de una decisión correcta de zeruel: el servicio corre con una
cuenta virtual sin privilegios, y sólo se le concede el directorio de estado y
el de pipelines. Cualquier herramienta que un pipeline invoque **fuera de esos
dos** hay que concedérsela a mano:

```powershell
icacls "C:\ruta\al\interprete" /grant "NT SERVICE\<servicio>:(OI)(CI)RX" /T
```

**Esto es lo que justifica el chequeo de arranque del punto 4.2.** zeruel no
puede adivinar qué programas va a invocar un pipeline, pero sí puede intentar
lanzarlos una vez al arrancar y decir cuáles no puede. Habría convertido tres
horas de búsqueda en una línea de log.
