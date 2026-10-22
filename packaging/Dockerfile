# Imagen del scheduler. Multi-etapa: se compila con el toolchain completo y se
# corre sobre una base minima, para no cargar el compilador en produccion.
#
#   docker build -f packaging/Dockerfile -t zeruel .
#   docker run -d --name zeruel \
#     -v zeruel-state:/var/lib/zeruel/state \
#     -v ./pipelines:/var/lib/zeruel/pipelines:ro \
#     -p 127.0.0.1:9876:9876 \
#     --stop-timeout 60 \
#     zeruel

FROM rust:1-slim-bookworm AS builder
WORKDIR /src

# cmake y la libc de desarrollo las necesita el arbol de polars.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev cmake \
    && rm -rf /var/lib/apt/lists/*

# Primero solo los manifiestos: mientras las dependencias no cambien, esta capa
# se reusa y un cambio de codigo no vuelve a compilar polars entero.
COPY Cargo.toml Cargo.lock ./
COPY crates/zeruel-sdk/Cargo.toml   crates/zeruel-sdk/
COPY crates/zeruel-core/Cargo.toml  crates/zeruel-core/
COPY crates/zeruel-data/Cargo.toml  crates/zeruel-data/
COPY crates/zeruel-tasks/Cargo.toml crates/zeruel-tasks/
COPY crates/zeruel/Cargo.toml       crates/zeruel/
COPY crates/zeruel-cli/Cargo.toml   crates/zeruel-cli/
RUN for d in crates/*/; do mkdir -p "$d/src" && echo "fn main(){}" > "$d/src/main.rs" \
      && echo "" > "$d/src/lib.rs"; done \
    && cargo build --release -p zeruel-cli 2>/dev/null || true

COPY . .
# Cargo no reconstruye si la fecha del archivo no cambio respecto del stub.
RUN find crates -name '*.rs' -exec touch {} + \
    && cargo build --release -p zeruel-cli \
    && strip target/release/zeruel

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/zeruel --shell /usr/sbin/nologin zeruel \
    && mkdir -p /var/lib/zeruel/state /var/lib/zeruel/pipelines \
    && chown -R zeruel:zeruel /var/lib/zeruel

COPY --from=builder /src/target/release/zeruel /usr/local/bin/zeruel

USER zeruel
WORKDIR /var/lib/zeruel
VOLUME ["/var/lib/zeruel/state"]
EXPOSE 9876

# `docker stop` manda SIGTERM y espera 10 s por defecto — menos que los 30 s de
# --gracia-secs. Sin subirlo, Docker mata el proceso a mitad del apagado y se
# pierde justo el reencolado que el apagado existe para hacer. Por eso el
# STOPSIGNAL explicito y el aviso de --stop-timeout 60 arriba.
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/zeruel"]
CMD ["serve", "--state", "/var/lib/zeruel/state", \
     "--pipelines", "/var/lib/zeruel/pipelines", "--gracia-secs", "30"]
