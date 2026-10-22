#!/usr/bin/env python3
"""Regenera el bloque de fuentes empotradas de `ui.html`.

`zeruel web` no pide las fuentes a un CDN: van adentro del binario. Es una
herramienta de servidor, y un `<link>` a fonts.googleapis.com significaria
una request a un tercero cada vez que alguien abre el panel — mas una
interfaz degradada en cualquier red sin salida a internet.

Uso:

    python scripts/fuentes.py            # reescribe crates/zeruel-cli/src/ui.html
    python scripts/fuentes.py --check    # falla si el archivo quedo viejo

Si se agrega un peso nuevo al CSS hay que sumarlo a PESOS de abajo: pedir un
peso que no esta empotrado hace que el navegador lo sintetice, y una negrita
falsa se nota enseguida en una tipografia ancha como Michroma.
"""

import argparse
import base64
import pathlib
import re
import sys
import urllib.request

# Los pesos que el CSS usa de verdad. Ver el comentario del modulo antes de
# tocar esto.
PESOS = {
    # Michroma viene en un solo peso y es muy ancha: sirve para titulos en
    # mayusculas espaciadas y nada mas. Esa limitacion es la que le da el tono
    # de consola; forzarla a texto corrido la arruina.
    "Michroma": [400],
    "Saira": [400, 600, 700],
    "JetBrains Mono": [400, 500],
}

# Solo el subset latino. latin-ext, cyrillic y vietnamese triplican el peso y
# esta interfaz no los usa.
SUBSET = "latin"

# Google devuelve woff2 solo si el User-Agent parece un navegador moderno; con
# el de urllib manda TTF, que pesa varias veces mas.
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

INICIO = "/* ── Fuentes empotradas ─"
FIN = "@font-face"


def bajar_css() -> str:
    familias = "&".join(
        "family=%s:wght@%s" % (fam.replace(" ", "+"), ";".join(str(p) for p in pesos))
        for fam, pesos in PESOS.items()
    )
    url = f"https://fonts.googleapis.com/css2?{familias}&display=swap"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    return urllib.request.urlopen(req).read().decode("utf-8")


def armar_bloque(css: str) -> str:
    bloques = re.findall(r"/\*\s*([a-z-]+)\s*\*/\s*(@font-face\s*\{[^}]*\})", css)
    if not bloques:
        sys.exit("Google no devolvio ningun @font-face: cambio el formato del CSS?")

    salida, total = [], 0
    for subset, blk in bloques:
        if subset != SUBSET:
            continue
        fam = re.search(r"font-family:\s*'([^']+)'", blk).group(1)
        peso = re.search(r"font-weight:\s*(\d+)", blk).group(1)
        url = re.search(r"url\((https://fonts\.gstatic\.com/[^)]+\.woff2)\)", blk).group(1)
        datos = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": UA})
        ).read()
        total += len(datos)
        b64 = base64.b64encode(datos).decode()
        salida.append(
            "@font-face{font-family:'%s';font-style:normal;font-weight:%s;"
            "font-display:swap;src:url(data:font/woff2;base64,%s) format('woff2')}"
            % (fam, peso, b64)
        )
        print(f"  {fam} {peso}: {len(datos) / 1024:.1f} KB", file=sys.stderr)

    esperados = sum(len(p) for p in PESOS.values())
    if len(salida) != esperados:
        sys.exit(
            f"esperaba {esperados} caras y saque {len(salida)}: "
            "algun peso de PESOS no existe en esa familia"
        )
    print(f"  total {total / 1024:.1f} KB\n", file=sys.stderr)
    return "\n".join(salida)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="no escribe; falla si cambio")
    args = ap.parse_args()

    ui = pathlib.Path(__file__).resolve().parent.parent / "crates/zeruel-cli/src/ui.html"
    texto = ui.read_text(encoding="utf-8")

    i = texto.index(INICIO)
    j = texto.index(FIN, i)
    k = texto.rindex("@font-face")
    k = texto.index("}", texto.index("format('woff2')", k)) + 1

    nuevo = texto[:j] + armar_bloque(bajar_css()) + texto[k:]
    if nuevo == texto:
        print("ui.html ya esta al dia")
        return 0
    if args.check:
        print("ui.html quedo viejo: corre scripts/fuentes.py", file=sys.stderr)
        return 1
    ui.write_text(nuevo, encoding="utf-8")
    print("ui.html actualizado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
