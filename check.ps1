# check.ps1 - lo mismo que corre el CI (.github/workflows/ci.yml), en local.
#
# fmt + clippy (warnings = error) + tests de todo el workspace. Correlo ANTES
# de cada commit, desde la raiz del repo:
#     .\check.ps1
#
# Sale 0 si esta todo bien. Con -Fix aplica el formato en vez de solo chequearlo.
#
# Este archivo tiene que quedar en ASCII: PowerShell 5.1 lee los .ps1 sin BOM
# como ANSI, y un caracter como el guion largo rompe el parser.

param([switch]$Fix)

$ErrorActionPreference = "Stop"
$fallos = @()

Write-Host "== 1/3 formato ==" -ForegroundColor Cyan
if ($Fix) {
    cargo fmt --all
    Write-Host "   formato aplicado" -ForegroundColor Green
} else {
    cargo fmt --all --check
    if ($LASTEXITCODE -ne 0) {
        $fallos += "cargo fmt (correr: .\check.ps1 -Fix)"
    } else {
        Write-Host "   ok" -ForegroundColor Green
    }
}

Write-Host "== 2/3 clippy ==" -ForegroundColor Cyan
cargo clippy --workspace --all-targets -- -D warnings
if ($LASTEXITCODE -ne 0) { $fallos += "cargo clippy" } else { Write-Host "   ok" -ForegroundColor Green }

Write-Host "== 3/3 tests ==" -ForegroundColor Cyan
cargo test --workspace
if ($LASTEXITCODE -ne 0) { $fallos += "cargo test" } else { Write-Host "   ok" -ForegroundColor Green }

Write-Host ""
if ($fallos.Count -gt 0) {
    Write-Host "FALLO: $($fallos -join ', ')" -ForegroundColor Red
    exit 1
}
Write-Host "TODO VERDE - listo para commitear" -ForegroundColor Green
exit 0
