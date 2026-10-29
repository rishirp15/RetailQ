param([switch]$Demo, [switch]$DashboardOnly, [switch]$Advanced, [switch]$Streaming, [int]$StreamBatches = 6)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$python = Join-Path $PWD '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) { throw 'Run .\scripts\setup.ps1 first.' }
if (-not $env:JAVA_HOME) {
    $jdk = Get-ChildItem 'C:\Program Files\Eclipse Adoptium' -Directory -Filter 'jdk-17*' -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($jdk) { $env:JAVA_HOME = $jdk.FullName }
}
if (-not $env:JAVA_HOME) { throw 'Install JDK 17 and set JAVA_HOME to its installation folder.' }
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = $python
$env:PYSPARK_DRIVER_PYTHON = $python
$env:SPARK_LOCAL_IP = '127.0.0.1'
$output = 'outputs/uci'
if ($Demo) { $output = 'outputs/demo' }
if ($Streaming) {
    & $python -m retailiq.streaming --input "$output/advanced/stream_events.csv" --directory "$output/advanced/streaming" --batches $StreamBatches --interval 3 --watch
    if ($LASTEXITCODE -ne 0) { throw 'Streaming replay failed.' }
    return
}
if (-not $DashboardOnly) {
    if ($Demo) {
        & $python -m retailiq.sample
        if ($LASTEXITCODE -ne 0) { throw 'Demo data creation failed.' }
        & $python -m retailiq.pipeline --input data/raw/sample.csv --output $output --synthetic
    } else {
        if (-not (Test-Path 'data/raw/online_retail.csv')) {
            & $python -m retailiq.data
            if ($LASTEXITCODE -ne 0) { throw 'Dataset preparation failed.' }
        }
        & $python -m retailiq.pipeline --input data/raw/online_retail.csv --output $output
    }
    if ($LASTEXITCODE -ne 0) { throw 'Spark pipeline failed. See the error above.' }
    if ($Advanced) {
        if ($Demo) {
            & $python -m retailiq.advanced --input data/raw/sample.csv --output "$output/advanced" --synthetic
        } else {
            & $python -m retailiq.advanced --input data/raw/online_retail.csv --output "$output/advanced"
        }
        if ($LASTEXITCODE -ne 0) { throw 'Some advanced stages failed. Run python -m retailiq.advanced to resume.' }
    }
}
& $python -m streamlit run app.py -- --results $output
