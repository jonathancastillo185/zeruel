# Cómo contribuir a zeruel

Gracias por querer ayudar. Esta guía explica cómo preparar el entorno, qué se
espera de un cambio y cómo mandarlo.

## Antes de empezar

- **Para algo chico** (un typo, un test, una aclaración en la doc): mandá el PR
  directamente.
- **Para algo grande** (una feature, un conector nuevo, un cambio de API): abrí
  primero un issue contando qué querés hacer. Así evitamos que trabajes en algo
  que choca con el [roadmap](docs/ROADMAP.md) o que ya está en marcha.
- Revisá [docs/IRREGULARIDADES.md](docs/IRREGULARIDADES.md) y
  [docs/HISTORIAL.md](docs/HISTORIAL.md) antes de reabrir una discusión: muchas
  decisiones ya se tomaron y ahí está el porqué.

## Preparar el entorno

```bash
# Rust estable: https://rustup.rs
rustup component add rustfmt clippy

git clone <url-de-tu-fork> zeruel
cd zeruel
cargo check --workspace      # segundos: usalo antes de compilar en serio
cargo test --workspace       # todos los tests
```

La primera compilación tarda varios minutos (Polars y wasmtime son grandes) y
`target/` puede ocupar varios GB. Un `cargo clean` de vez en cuando ayuda.

## Antes de cada commit

Corré el chequeo completo, que es lo mismo que corre el CI:

```powershell
.\check.ps1          # Windows: fmt + clippy (warnings = error) + tests
.\check.ps1 -Fix     # aplica el formato en vez de solo chequearlo
```

En Linux o macOS:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

**Siempre todo el workspace**, nunca solo el crate que tocaste: un test de otro
crate estuvo en rojo meses porque se corrían solo tests puntuales.

## Qué se espera de un cambio

- **Un cambio, un propósito.** Un PR que arregla un bug no reformatea medio
  repositorio.
- **Con test.** Si arreglás un bug, sumá el test que lo habría detectado. Si
  agregás algo, sumá el test que demuestra que anda.
- **Verificado, no supuesto.** No marques algo como resuelto sin correr el
  comando que lo prueba, y decí en el PR qué corriste. Si un cambio puede afectar
  el rendimiento, medilo: si el pipeline de ejemplo empeora más de un 10 % o
  cambia la cantidad de filas, hay que explicarlo.
- **Documentación al día.** Si cambiás un comando, un flag o un comportamiento,
  actualizá [docs/MANUAL.md](docs/MANUAL.md) (y el HTML si corresponde) y el
  [CHANGELOG.md](CHANGELOG.md).
- **Bugs encontrados al pasar**: si tocás el área, arreglalos; si no, anotalos en
  [docs/IRREGULARIDADES.md](docs/IRREGULARIDADES.md) con prioridad o abrí un
  issue.

## Estilo

- `cargo fmt` manda. Clippy sin warnings.
- El código, los comentarios y la documentación están **en español**. Los
  comentarios explican el *porqué* (la decisión, la trampa que evitan), no lo
  que el código ya dice.
- Las dependencias se declaran **solo** en el `Cargo.toml` del workspace; los
  crates usan `{ workspace = true }`.
- Una dependencia nueva se justifica: zeruel apuesta a ser un binario chico. Si
  es opcional, va detrás de una *feature*. [docs/COSTOS.md](docs/COSTOS.md)
  muestra cuánto cuesta cada una.
- Los scripts `.ps1` quedan en ASCII (PowerShell 5.1 los lee como ANSI).

## Mensajes de commit

Formato tipo [Conventional Commits](https://www.conventionalcommits.org/es/),
en español:

```
fix(scheduler): una medicion mala de RAM bloqueaba un DAG para siempre
feat(panel): filtro por fecha en Corridas
docs(manual): explicar el contrato --describe
```

Tipos habituales: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`.

## Mandar el PR

1. Hacé un fork y una rama desde `main`.
2. Commiteá con el chequeo en verde.
3. Abrí el PR completando la plantilla: qué cambia, por qué y cómo lo
   verificaste.
4. El CI tiene que pasar. Si falla en una plataforma que no tenés, avisá en el
   PR y lo vemos juntos.

## Trabajar con agentes de IA

Está bien usar asistentes de IA. [AGENTS.md](AGENTS.md) les da el contexto del
proyecto. Vos sos responsable de lo que mandás: revisá y corré todo antes de
abrir el PR.

## Licencia de las contribuciones

Al mandar un PR aceptás que tu contribución se publique bajo la
[licencia MIT](LICENSE) del proyecto.
