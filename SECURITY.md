# Seguridad

## Reportar una vulnerabilidad

**No abras un issue público.** Usá el
[reporte privado de vulnerabilidades de GitHub](../../security/advisories/new)
(pestaña *Security* → *Report a vulnerability*).

Contá qué encontraste, cómo reproducirlo y qué impacto creés que tiene. Vas a
recibir una respuesta lo antes posible. Es un proyecto mantenido por una sola
persona en su tiempo libre, así que no hay un plazo garantizado. Cuando haya un
arreglo, se publica junto con el crédito para quien lo reportó, si así lo
quiere.

## Versiones con soporte

Solo la última versión publicada en `main`. zeruel está en 0.x: los arreglos de
seguridad no se portan a versiones anteriores.

## Modelo de seguridad: lo que hay que saber antes de usarlo

zeruel ejecuta **los pipelines que vos le das**. No es un sandbox para código
de terceros: un binario en `pipelines/` corre con los permisos del servicio.
Tratá esa carpeta como tratarías un `crontab`.

### El panel web (`zeruel web`)

- Escucha en **`127.0.0.1` por defecto**. Desde ahí permite encolar, reintentar
  y cancelar corridas, y leer los logs.
- Tiene un **token opcional** (`--token`, la variable `ZERUEL_TOKEN` o `[web]
  token` en `zeruel.toml`). Se acepta como `Authorization: Bearer <token>` o
  como `?token=<token>`, y se compara en tiempo constante.
- **No tiene HTTPS.** Con `--bind 0.0.0.0` el token viaja en texto plano, y si
  va en la URL puede quedar en el historial del navegador o en los logs de un
  proxy.
- Hoy **nada obliga** a configurar un token cuando se escucha fuera de
  `127.0.0.1`: solo se muestra un aviso.

**Recomendación:** dejalo en `127.0.0.1`. Si necesitás verlo desde otra
máquina, usá un túnel (SSH, una VPN como WireGuard o Tailscale) o un proxy
inverso con TLS, **y además** configurá un token largo y aleatorio.

### Secretos

- `zeruel.toml` admite referencias a variables de entorno (`"${ZERUEL_TOKEN}"`)
  para que el archivo diga *de dónde* sale un secreto y no *cuál es*. Usalas en
  vez de escribir contraseñas en el archivo.
- `zeruel doctor` no prueba ni nombra las variables de `[entorno]` cuyo nombre
  contiene `PASSWORD`, `SECRET` o `TOKEN`, para que su valor no termine en un
  log.
- No commitees `zeruel.toml` con credenciales, ni las carpetas `state/` o
  `data/`, que ya están en `.gitignore`.

### Tareas WASM

Las tareas WASM corren en el sandbox de wasmtime con límites de *fuel* y
memoria, y se pueden cortar a mitad de una instrucción. Es la única forma de
tarea pensada para código no confiable, y aun así conviene revisarla.

### Conexiones

- **Postgres** hoy no soporta TLS: usalo solo en la misma máquina, la misma red
  privada o por un túnel.
- **HTTP / Snowflake / nube** usan TLS por medio de sus librerías.

## Mejoras de seguridad pendientes

Son buenas contribuciones:

- Exigir un token cuando `--bind` no es local.
- HTTPS propio en el panel, o una guía oficial de proxy inverso.
- TLS para Postgres.
- Techo de memoria en Linux (cgroups v2), para que un pipeline no pueda agotar
  la RAM de la máquina.
