# Plan: instalar y actualizar zeruel como un programa normal

Escrito el 2026-09-05, después de que instalar zeruel en una máquina real
requiriera cinco comandos en orden, con rutas absolutas, en una consola de
administrador — y de que saltearse uno dejara el sistema roto **en silencio**.

El pedido es concreto: **un `.exe` que se encargue de todo**, y un proceso de
actualización como el de cualquier programa que se instala.

---

## 1. Lo que hoy hay que saber para instalar zeruel

```powershell
C:\zeruel\zeruel.exe servicio desinstalar --nombre zeruel
C:\zeruel\zeruel.exe servicio desinstalar --nombre zeruel-web
C:\zeruel\zeruel.exe servicio instalar --nombre zeruel `
    --state C:/zeruel/state --pipelines C:/zeruel/pipelines `
    --config C:/zeruel/zeruel.toml --gracia-secs 60
C:\zeruel\zeruel.exe servicio instalar --nombre zeruel-web --modo web `
    --puerto 9876 --state ... --pipelines ... --config ...
Start-Service zeruel, zeruel-web
```

Cada línea es una oportunidad de equivocarse, y **la evidencia es que se
equivocaron todas**:

| Lo que pasó | Consecuencia |
|---|---|
| Se escribió `zeruel` sin la ruta | "no se reconoce el término" — zeruel no está en el PATH |
| `desinstalar` no paró el servicio | quedó marcado para eliminación; `instalar` falló con 1072 y el sistema quedó sin servicio y sin forma de rehacerlo |
| Se instaló sin `--config` (una versión anterior no lo tenía) | dos días con `paralelo=2` y la cola en 1.400 ítems |
| Nadie concedió permisos sobre los intérpretes | los pipelines fallaban con "No Python at…" |

Ninguno de esos cuatro es un error del usuario: son **un procedimiento que pide
demasiado**.

---

## 2. La forma correcta: el propio binario es el instalador

No hace falta MSI, ni WiX, ni Inno Setup. zeruel ya es **un ejecutable único y
autocontenido**, que es la condición que hace posible lo más simple:

```
zeruel.exe            → doble clic: menú interactivo
zeruel.exe instalar   → instala todo
zeruel.exe actualizar → actualiza en su lugar
```

Es lo que hacen `rustup`, `deno`, `bun` y `gh`. Y tiene una ventaja concreta
sobre un instalador tradicional: **el instalador y el programa son la misma
versión, siempre**, porque son el mismo archivo.

### Por qué no un MSI

- Agrega una cadena de build (WiX/cargo-wix) que hay que mantener y que sólo
  sirve en Windows, cuando zeruel tiene que andar igual en Linux.
- No resuelve nada que esto no resuelva: el registro en "Agregar o quitar
  programas" se puede escribir directo, y son cuatro claves.
- El punto del pedido no es "que aparezca en el Panel de Control": es **no
  tener que tipear comandos**.

---

## Fase 1 — `zeruel instalar`: un comando que deja todo andando

Hace, en orden, y **verificando cada paso**:

1. **Elegir destino.** `C:\zeruel` por defecto, o el que se pase. Se pregunta.
2. **Copiarse ahí.** El binario que estás ejecutando se copia al destino. Si ya
   hay uno, es una actualización (Fase 2), no una instalación.
3. **Crear la configuración** con `init` si no existe. Si existe, **no se toca**:
   los valores de producción no están en ningún otro lado.
4. **Crear los directorios** `state/` y `pipelines/`.
5. **Registrar los dos servicios** con `--config` apuntando al archivo real.
6. **Conceder permisos** a las cuentas virtuales sobre `state`, `pipelines`, el
   config **y las herramientas de `[entorno]`** — que es lo que faltó y costó
   horas.
7. **Agregar el destino al PATH** de la máquina, para que `zeruel` funcione sin
   ruta absoluta.
8. **Registrar la desinstalación** en `Agregar o quitar programas`.
9. **Arrancar los servicios y verificar** que quedaron corriendo.
10. **Correr `doctor`** y mostrar el resultado: si una herramienta no responde,
    se dice ahora y no en la primera corrida.

Todo bajo **un solo UAC**: si no es administrador, se auto-eleva una vez al
principio.

### La regla que ordena todo esto

**Cada paso verifica que pasó, y si no, aborta diciendo qué quedó a medias.** El
antipatrón a evitar es el que ya nos mordió: un script que imprime "listo"
cuando el comando de abajo falló.

---

## Fase 2 — `zeruel actualizar`: como cualquier programa

```
zeruel actualizar                 # usa el binario desde el que se ejecuta
zeruel actualizar --desde <ruta>  # o uno que le digas
```

1. **Comparar versiones.** Si es la misma, no hace nada y lo dice.
2. **Parar los servicios**, esperando a los runs en curso (la gracia
   configurada). Un ETL de dos horas a la mitad no se corta por una
   actualización.
3. **Guardar el binario viejo** como `zeruel.exe.anterior`. En Windows un `.exe`
   en uso no se puede sobreescribir pero **sí renombrar**, que es lo que hace
   posible el reemplazo sin reiniciar la máquina.
4. **Copiar el nuevo** y arrancar.
5. **Verificar que arrancó.** Si no, **volver al anterior automáticamente** y
   decirlo. Una actualización que deja el sistema caído es peor que no
   actualizar.
6. **Guardar la versión** instalada, para que `zeruel version` y el panel puedan
   decir qué está corriendo.

### Lo que NO va a hacer

- **Descargar actualizaciones solo.** Un orquestador que se auto-actualiza desde
  internet es una superficie de ataque y una fuente de sorpresas a las 3 de la
  mañana. Se le pasa el binario nuevo; él se encarga del reemplazo seguro.
- **Migrar la configuración.** Si un ajuste cambia de nombre, se avisa al
  arrancar; reescribir el archivo del usuario sin pedirlo es peor.

---

## Fase 3 — Doble clic

Al ejecutarse **sin argumentos y con una consola interactiva**, mostrar un menú:

```
zeruel 0.1.0

  1) Instalar en esta máquina
  2) Actualizar la instalación existente  (0.1.0 → 0.2.0)
  3) Ver el estado
  4) Salir
```

Con dos cuidados:

- **Sólo si hay terminal.** Sin `stdin` interactivo —un script, una tarea
  programada, una tubería— se comporta como siempre y muestra la ayuda. Cambiar
  eso rompería a quien ya lo automatizó.
- **La ventana no se cierra sola** al terminar. Un instalador que hace doble clic,
  parpadea y desaparece no deja leer si salió bien.

---

## Fase 4 — Desinstalar de verdad

`zeruel desinstalar` para los servicios, los saca, quita el PATH, borra la
entrada de "Agregar o quitar programas" y **pregunta explícitamente** qué hacer
con `state/` — que tiene el historial y no se borra sin decirlo.

---

## Cómo se verifica cada fase

| Fase | Se termina cuando |
|---|---|
| 1 | En una máquina limpia, un doble clic deja los servicios corriendo y la primera corrida sale bien |
| 2 | Actualizar con los servicios corriendo no pierde ningún run, y una versión rota vuelve sola a la anterior |
| 3 | Doble clic muestra el menú; `zeruel --help` en un script sigue funcionando igual |
| 4 | Desinstalar deja la máquina como estaba, y avisa antes de tocar el historial |

La prueba que vale para todas: **alguien que nunca vio zeruel lo instala sin
leer documentación.** Hoy hacen falta cinco comandos y saber por qué cada uno.
