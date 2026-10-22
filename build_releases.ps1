# Builds cross-platform de zeruel
#
# Compila el binario release para cada plataforma soportada y empaqueta
# cada una con `zeruel deploy` (carpeta portable).
#
# Uso:
#   (desde la raiz del repo)
#   .\build_releases.ps1              # solo la plataforma nativa (windows)
#   .\build_releases.ps1 -All         # todas (usa el binario nativo para
#                                     # empaquetar;
#                                     # los otros targets necesitan su toolchain)
#
# Targets:
#   windows-x86_64   x86_64-pc-windows-msvc
#   linux-x86_64     x86_64-unknown-linux-musl  (estatico, sin glibc)
#   macos-aarch64    aarch64-apple-darwin        (Apple Silicon)
#   macos-x86_64     x86_64-apple-darwin
#
# Todavia no hay workflow de releases: cada target se compila con su toolchain;
# este script arma los paquetes a partir de los binarios de target/*/release.

param(
    [switch]$All
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$targets = @{}
$targets["windows-x86_64"] = "x86_64-pc-windows-msvc"
$targets["linux-x86_64"] = "x86_64-unknown-linux-musl"
$targets["macos-aarch64"] = "aarch64-apple-darwin"
$targets["macos-x86_64"] = "x86_64-apple-darwin"

$native = if ($IsWindows -or $env:OS -eq "Windows_NT") {
    "windows-x86_64"
} elseif ($env:OSTYPE -eq "darwin" -and [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq "Arm64") {
    "macos-aarch64"
} elseif ($env:OSTYPE -eq "darwin") {
    "macos-x86_64"
} else {
    "linux-x86_64"
}

$lista = if ($All) { $targets.Keys } else { @($native) }

foreach ($nombre in $lista) {
    $t = $targets[$nombre]
    Write-Host "`n=== $nombre ($t) ===" -ForegroundColor Cyan

    # 1. toolchain + target
    rustup target add $t | Out-Null
    if ($nombre -ne $native) {
        if ($t -eq "x86_64-unknown-linux-musl") {
            Write-Host "  nota: musl requiere 'rustup toolchain link' o musl-gcc" -ForegroundColor Yellow
        } elseif ($t -like "*apple*") {
            Write-Host "  nota: darwin se compila en una Mac (no hay workflow de releases todavia)" -ForegroundColor Yellow
        }
        continue
    }

    # 2. build release
    cargo build --release
    if ($LASTEXITCODE -ne 0) { throw "cargo build fallo" }

    # 3. paquete portable
    $dest = Join-Path $PSScriptRoot "dist\release\bin-$nombre"
    .\target\release\zeruel.exe deploy --dest $dest --docker --zip
    if ($LASTEXITCODE -ne 0) { throw "zeruel deploy fallo" }
}

Write-Host "`ndone. Paquetes en dist\release\" -ForegroundColor Green