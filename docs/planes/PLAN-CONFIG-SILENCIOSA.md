# Plan: por qué zeruel no ejecuta lo que debería

Diagnóstico del 2026-09-03, con la cola en 1.400 ítems y esperas de 22 horas.

**No estaba roto: estaba corriendo a un tercio de velocidad, sin ningún tope, y
sin decirlo.** Es la misma clase de falla que las tres anteriores —silenciosa y
apuntando a otro lado— pero esta vez la causa es distinta y hay que arreglarla
en zeruel, no en la instalación.

---

## 1. La evidencia

El sistema **sí ejecutaba**. En el momento del diagnóstico la última corrida
había arrancado y terminado hacía segundos, y el log mostraba trabajo continuo.
Lo que estaba mal era el ritmo:

```
>>> lanzo: dag=scraping_g ... en_cola=79984s (ram_disp=36638MB, 2/2 en curso)
                                    ↑ 22 horas         ↑ el techo es DOS
```

Y la línea de arranque del scheduler:

```
zeruel serve: paralelo=2 workers=4 pipelines=C:\zeruel/pipelines techo=sin tope (max=0)
```

Contra lo que dice el `zeruel.toml` de esa máquina:

| Ajuste | El archivo dice | El servicio usó | Consecuencia |
|---|---|---|---|
| `paralelo` | 6 | **2** | un tercio del rendimiento |
| `ram_max` | `2500M` | **sin tope** | ningún techo de memoria |
| `timeout_secs` | 10800 (3 h) | **0 = sin tope** | una corrida quedó 4,9 h sin que nadie la cortara |
| retención | 30 / 365 días | los del código | (sin efecto visible) |

Con 47 scrapers que se encolan cada 10-30 minutos y sólo dos corriendo a la vez,
la cola crece más rápido de lo que drena. 1.400 ítems es el resultado de dos
días así, y las "22 horas de espera" son la profundidad del atraso, no un
proceso colgado.

---

## 2. La causa

El servicio quedó registrado con estos argumentos:

```
zeruel.exe servicio correr --nombre zeruel --modo scheduler
           --puerto 9876 --state ... --pipelines ... --gracia-secs 60
```

**No hay `--config`.** Se instaló antes de que ese parámetro existiera, y un
registro de servicio no se actualiza solo: sigue siendo el mismo hasta que
alguien lo reinstala.

Sin `--config`, el código hace `Config::cargar(None)`, que busca `zeruel.toml`
**en el directorio actual**. El directorio actual de un servicio es `System32`.
No hay ningún `zeruel.toml` ahí, y nunca lo va a haber.

Entonces:

```rust
None => config::Config::cargar(None).unwrap_or_default(),
//                                    ^^^^^^^^^^^^^^^^^^
//                                    acá se pierde todo, sin una palabra
```

La sección `[entorno]` sí funciona, y eso es lo que despistó: viene de las
variables de máquina, que se fijaron aparte. Como los pipelines encontraban sus
intérpretes y las corridas salían bien, todo parecía normal.

---

## 3. Por qué esto es un defecto de zeruel y no de la instalación

Un servicio corrió **dos días** con su configuración vacía y no lo dijo nunca.
Ni un aviso, ni un warning. El `unwrap_or_default()` convirtió "no encontré tu
configuración" en "arrancá con los valores de fábrica".

Es exactamente el patrón que ya identificamos en `docs/PLAN-CONFIABILIDAD.md`:
**falla en silencio y el síntoma aparece lejos de la causa.** Acá el síntoma fue
una cola de 1.400 ítems dos días después, y la causa fue una línea que decidió
seguir adelante sin avisar.

Peor: yo mismo dejé este agujero. Al arreglar lo del `[entorno]` hice que el
servicio **fallara fuerte si `--config` era explícito y no se podía leer**, pero
dejé el caso "sin `--config`" cayendo al default en silencio. Cubrí la mitad que
no era.

---

## Fase 1 — Que un servicio sin configuración lo grite · HECHA

1. Cuando `servicio correr` arranca **sin `--config`**, buscar el `zeruel.toml`
   al lado del ejecutable —que es donde queda en una instalación normal— y
   **decir en el log cuál cargó**.
2. Si no encuentra ninguno, registrar un **error**, no un silencio: "arrancando
   con la configuración de fábrica (paralelo=2, sin techo de RAM, sin tope de
   tiempo) porque no encontré ningún zeruel.toml". Los valores de fábrica en el
   mensaje, para que se vea que no son los que el operador escribió.
3. Que el servicio se registre **siempre** con la ruta del config, aunque haya
   que deducirla: un registro sin `--config` es un servicio que va a hacer esto
   otra vez.

## Fase 2 — Que el arranque diga qué configuración está usando, y de dónde · HECHA

El log dice `paralelo=2 workers=4 techo=sin tope`. Eso alcanzaba para
diagnosticarlo **si alguien lo leía**, pero no dice de dónde salió cada valor.
Debería:

```
zeruel serve: config=C:/zeruel/zeruel.toml
  paralelo=6      (del archivo)
  workers=4       (del archivo)
  ram_max=2500M   (del archivo)
  timeout=10800s  (del archivo)
```

y, cuando corresponda:

```
  paralelo=2      (DE FÁBRICA — no habia archivo)
```

Decir el origen es lo que convierte una línea de log en un diagnóstico. Así
quedó, verificado contra la configuración real:

```
INFO  zeruel serve: configuracion de C:/zeruel/zeruel.toml
INFO    paralelo=6 (runs a la vez)  workers=4 (tareas por run)
INFO    techo de RAM por run: 2500 MB
INFO    tope por corrida: 10800 s
```

y sin archivo:

```
ERROR zeruel serve: SIN ARCHIVO DE CONFIGURACION. Todo lo de abajo son valores
      de fabrica, no los que hayas escrito en un zeruel.toml.
INFO    paralelo=2 (runs a la vez)  workers=4 (tareas por run)
INFO    techo de RAM por run: SIN TOPE — un pipeline que se desmadre se lleva la maquina
INFO    tope por corrida: SIN TOPE — una corrida colgada ocupa un lugar para siempre
```

## Fase 3 — Que el panel muestre la configuración efectiva

Lo anterior sigue dependiendo de que alguien abra un log. La pestaña **Sistema**
debería mostrar con qué está corriendo el scheduler —los cuatro valores y de
dónde salen— y marcar en rojo cuando esté con los de fábrica.

Es la diferencia entre "el dato existe" y "el dato se ve".

## Fase 4 — Que una cola que no drena avise sola · HECHA

Esto es lo que habría atajado el problema **el primer día**, sin importar la
causa: una cola que crece de forma sostenida es un síntoma que no depende de por
qué crece.

Una alerta con dos condiciones, para no gritar por un pico normal:

- la cola creció durante las últimas N horas, **y**
- el ítem más viejo espera más que, digamos, seis veces la duración típica de su
  DAG.

El texto da el número accionable: *"1.400 corridas esperando, la más vieja hace
22 h; terminan 20/h, al ritmo actual tardaría 70 h en ponerse al día"*.

**Un detalle que encontró un test**: la tasa de entrada NO sirve como
disparador, aunque sea lo primero que uno escribe. Los ítems consumidos se
**borran** de la cola, así que "cuántos entraron en la última hora" da cero justo
cuando el atraso es viejo — que es cuando más importa. El disparador son el
tamaño y la espera, que son el síntoma; el drenaje va en el mensaje, porque ése
sí se puede medir bien y es el que dice cuánto falta.

---

## Lo que hay que hacer ahora, en la instalación real

La cola ya se vació (1.398 ítems cancelados). Falta **reinstalar los servicios**
para que quede grabada la ruta del config; sin eso vuelven a arrancar con los
valores de fábrica y la cola se llena de nuevo. Necesita consola de
administrador:

```powershell
zeruel servicio desinstalar --nombre zeruel
zeruel servicio desinstalar --nombre zeruel-web

zeruel servicio instalar --nombre zeruel `
    --state C:/zeruel/state --pipelines C:/zeruel/pipelines `
    --config C:/zeruel/zeruel.toml --gracia-secs 60

zeruel servicio instalar --nombre zeruel-web --modo web --puerto 9876 `
    --state C:/zeruel/state --pipelines C:/zeruel/pipelines `
    --config C:/zeruel/zeruel.toml

Start-Service zeruel, zeruel-web
```

Y para confirmar que quedó bien, en el log tiene que decir `paralelo=6` y
`techo=2500 MB`:

```powershell
Select-String -Path C:\zeruel\state\logs\zeruel.log.* -Pattern "zeruel serve:" |
  Select-Object -Last 1
```

---

## Cómo saber que esto quedó resuelto

| Fase | Se termina cuando |
|---|---|
| 1 | Un servicio sin `zeruel.toml` a la vista lo dice en el log, con los valores de fábrica que va a usar |
| 2 | La línea de arranque dice de dónde salió cada valor |
| 3 | El panel muestra la configuración efectiva sin abrir un log |
| 4 | Una cola que no drena genera una alerta antes de que alguien la note |

La prueba que vale para todas: **instalar el servicio con una configuración que
no se pueda leer y darse cuenta en el primer minuto**, no en dos días.
