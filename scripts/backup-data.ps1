# Копирует JSON-данные склада в подпапку backups/ с меткой времени.
# Запуск по расписанию: Планировщик заданий Windows или внешний оркестратор.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$data = Join-Path $root "data"
$destRoot = Join-Path $root "backups"
$stamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$dest = Join-Path $destRoot $stamp

if (-not (Test-Path $data)) {
  Write-Host "Папка data не найдена: $data"
  exit 0
}

New-Item -ItemType Directory -Path $dest -Force | Out-Null
$patterns = @("*.json", "*.jsonl")
foreach ($p in $patterns) {
  Get-ChildItem -Path $data -Filter $p -File -ErrorAction SilentlyContinue | ForEach-Object {
    Copy-Item $_.FullName -Destination (Join-Path $dest $_.Name)
  }
}
Write-Host "Сохранено в $dest"
