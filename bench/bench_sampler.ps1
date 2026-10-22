# Sampler de RAM de un contenedor Docker: 100ms de intervalo, guarda el maximo
# en un archivo.
#
#   .\bench_sampler.ps1 -Contenedor airflow-apiserver-1 -Salida bench_ram.log
param(
    [Parameter(Mandatory = $true)][string]$Contenedor,
    [string]$Salida = "bench_ram.log",
    [int]$Muestras = 400
)

$maxMiB = 0
$base = (docker stats $Contenedor --no-stream --format '{{.MemUsage}}') | Select-String -Pattern '([\d.]+)MiB' | ForEach-Object { [double]$_.Matches[0].Groups[1].Value }
Write-Host "BASE_MiB=$base"
for ($i = 0; $i -lt $Muestras; $i++) {
    Start-Sleep -Milliseconds 100
    $s = docker stats $Contenedor --no-stream --format '{{.MemUsage}}'
    if ($s -match '([\d.]+)(GiB|MiB)') {
        $v = [double]$Matches[1]
        $v = if ($Matches[2] -eq 'GiB') { $v * 1024 } else { $v }
        if ($v -gt $maxMiB) { $maxMiB = $v }
    }
}
Write-Host "PEAK_MiB=$maxMiB"
Set-Content -Path $Salida -Value "base=$base max=$maxMiB"
