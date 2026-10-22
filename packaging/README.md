# Poner zeruel a correr como servicio

Tres formas, según dónde vaya. Todas dependen de lo mismo: el scheduler **apaga
ordenado** cuando le piden parar — deja de lanzar, espera a los runs en curso
hasta `--gracia-secs`, y lo que tenga que cortar **vuelve a la cola** para que
el arranque siguiente lo retome.

Eso significa que hay un número que hay que mirar en las tres:

> **El plazo que le da el supervisor tiene que ser mayor que `--gracia-secs`.**
> Si mata el proceso antes de que termine el apagado, se pierde exactamente el
> trabajo que el apagado existe para no perder.

| Entorno | Cómo pide parar | Plazo | Por defecto | Con `--gracia-secs 30` |
|---|---|---|---:|---|
| systemd | SIGTERM | `TimeoutStopSec` | 90 s | alcanza; la unidad lo fija en 60 |
| Docker | SIGTERM | `--stop-timeout` | **10 s** | **no alcanza**: hay que subirlo |
| Windows | control `Stop` del SCM | lo informa el servicio | — | el servicio le avisa a Windows |

Docker es el que muerde: 10 segundos por defecto, menos que la gracia.

---

## Linux — systemd

```bash
sudo useradd --system --home /var/lib/zeruel --shell /usr/sbin/nologin zeruel
sudo mkdir -p /var/lib/zeruel/{state,pipelines}
sudo chown -R zeruel:zeruel /var/lib/zeruel

sudo cp zeruel /usr/local/bin/
sudo cp packaging/zeruel.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now zeruel
```

Ver qué está haciendo:

```bash
systemctl status zeruel
journalctl -u zeruel -f
```

La unidad viene con aislamiento puesto (`ProtectSystem=strict`, `ProtectHome`,
`NoNewPrivileges`). El scheduler solo puede escribir en `/var/lib/zeruel`, que es
lo único que necesita.

**Desplegar un pipeline no requiere reiniciar el servicio.** Copiar el binario
a `/var/lib/zeruel/pipelines` y listo: el scheduler re-escanea el directorio en
cada tick y lo levanta. Esa es toda la ventaja de un proceso por pipeline.

---

## Contenedor

```bash
docker build -f packaging/Dockerfile -t zeruel .

docker run -d --name zeruel \
  -v zeruel-state:/var/lib/zeruel/state \
  -v "$PWD/pipelines:/var/lib/zeruel/pipelines:ro" \
  -p 127.0.0.1:9876:9876 \
  --stop-timeout 60 \
  zeruel
```

**`--stop-timeout 60` no es opcional.** El default de Docker son 10 segundos:
manda SIGTERM, espera 10, y manda SIGKILL. Con `--gracia-secs 30` el apagado
todavía está esperando a los runs cuando llega el SIGKILL, y ahí muere sin
reencolar. Si preferís no acordarte del flag, bajá la gracia: `--gracia-secs 8`.

La imagen corre como usuario `zeruel`, sin privilegios. El estado va a un volumen
para que sobreviva a un `docker rm`.

El build es multi-etapa y copia los manifiestos antes que el código, así que
mientras las dependencias no cambien no vuelve a compilar polars — que es la
diferencia entre un rebuild de segundos y uno de varios minutos.

---

## Windows — servicio nativo

No hace falta ningún envoltorio: `zeruel` conversa directamente con el
Administrador de servicios.

**Son dos servicios, no uno.** El scheduler y la interfaz web se instalan por
separado a propósito: el scheduler es lo que no puede caerse, y reiniciar la
web para cambiarle el puerto no tiene por qué interrumpir un ETL de dos horas
que está a la mitad. Es el mismo principio que pone cada pipeline en su propio
proceso.

Desde una consola **como administrador**:

```powershell
New-Item -ItemType Directory -Force "C:/zeruel/state", "C:/zeruel/pipelines"
Copy-Item zeruel.exe "C:/zeruel/"

# 1. El scheduler: consume la cola y lanza los pipelines
C:/zeruel/zeruel.exe servicio instalar `
    --nombre zeruel `
    --state "C:/zeruel/state" `
    --pipelines "C:/zeruel/pipelines" `
    --gracia-secs 30

# 2. La interfaz: solo lee y encola
C:/zeruel/zeruel.exe servicio instalar `
    --nombre zeruel-web --modo web --puerto 9876 `
    --state "C:/zeruel/state" `
    --pipelines "C:/zeruel/pipelines"

sc.exe start zeruel
sc.exe start zeruel-web
```

Los dos apuntan al **mismo** `--state` y al mismo `--pipelines`: comparten la
base y ven los mismos binarios. Si le das rutas distintas, la web va a listar
DAGs que el scheduler no puede ejecutar.

Manejo cotidiano, sin abrir una consola de administrador (pide los permisos
cuando hacen falta):

```powershell
zeruel.exe servicio estado    --nombre zeruel
zeruel.exe servicio parar     --nombre zeruel
zeruel.exe servicio arrancar  --nombre zeruel
zeruel.exe servicio desinstalar --nombre zeruel
```

**Las rutas tienen que ser absolutas.** Un servicio arranca desde `System32`,
no desde donde lo instalaste: si le pasás `state` a secas, el comando la
convierte a absoluta y te avisa con qué ruta quedó.

**Cada instancia queda aislada.** El servicio corre bajo su propia cuenta
virtual (`NT SERVICE\<nombre>`), no como LocalSystem, y solo recibe permiso de
modificación sobre su `state` y de lectura sobre sus `pipelines`. Dos
instalaciones —una de desarrollo y otra de producción— no pueden tocarse el
estado entre sí.

Sobre el apagado vale lo mismo que en systemd y en Docker, con una diferencia a
favor: el servicio le informa a Windows cuánto va a tardar (`--gracia-secs` más
un margen) en vez de dejar que lo adivine, así que no hay ningún número que
ajustar a mano.

`servicio correr` existe pero es para Windows, no para vos: es el modo que
conversa con el SCM y falla si lo ejecutás a mano. Para primer plano está
`zeruel serve`.

---

## Verificar que el apagado funciona

Con el servicio corriendo y algún run en curso:

```bash
# Linux
sudo systemctl stop zeruel && journalctl -u zeruel -n 30

# Docker
docker stop zeruel && docker logs --tail 30 zeruel
```

```powershell
# Windows
sc.exe stop zeruel ; Get-Content "C:/zeruel/state/logs/zeruel.log" -Tail 30
```

En el log tiene que aparecer, en este orden:

```
apagando: no lanzo nada nuevo y espero a lo que esta corriendo
apagado: espero hasta 30s a 2 run(s) en curso
apagado: todos los runs terminaron solos
```

o, si alguno no llegó:

```
apagado: 1 run(s) no terminaron en 30s, los corto y los devuelvo a la cola
apagado: ventas-diarias [2026-08-22] vuelve a la cola
```

Al levantarlo de nuevo, ese run tiene que estar otra vez en la cola. Si en vez
de eso ves un run colgado en `Running`, el supervisor mató el proceso antes de
tiempo: revisá el plazo de la tabla de arriba.

---

## Qué anda en cada sistema

El techo duro de memoria es lo único que se implementa distinto en cada uno, y
por eso conviene tenerlo a mano:

| | Linux / macOS | Windows |
|---|---|---|
| Scheduler y runs | sí | sí |
| Apagado ordenado | SIGTERM / SIGINT / SIGHUP | control del SCM, Ctrl+C |
| Correr como servicio | systemd | servicio nativo |
| Techo blando (muestreo) | sí | sí |
| Techo duro (lo impone el sistema) | `setrlimit(RLIMIT_DATA)` | Job Object |

Los dos mecanismos del techo duro miden lo mismo —lo que el proceso reserva— y
ninguno cuenta los archivos mapeados en memoria, que es la distinción que
importa: un parquet de 10 GB mapeado no consume del techo, un `Vec` de 10 GB sí.
El detalle de por qué `RLIMIT_DATA` y no `RLIMIT_AS` ni cgroups está en
`crates/zeruel-core/src/limite.rs`.
