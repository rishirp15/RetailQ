$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:UV_CACHE_DIR = Join-Path $PWD '.cache\uv'
$env:UV_PYTHON_INSTALL_DIR = Join-Path $PWD '.runtime\python'
$requirements = 'requirements-lock.txt'
if (Get-Command uv -ErrorAction SilentlyContinue) {
    uv python install 3.11
    if ($LASTEXITCODE -ne 0) { throw 'Python runtime installation failed.' }
    uv venv --python 3.11 .venv
    if ($LASTEXITCODE -ne 0) { throw 'Virtual environment creation failed.' }
    uv pip install --python .venv\Scripts\python.exe -r $requirements
} else {
    py -3.11 -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Install Python 3.11 or uv, then rerun setup.' }
    .\.venv\Scripts\python.exe -m pip install -r $requirements
}
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
Write-Host 'Ready. Run .\scripts\run.ps1 -Demo for a synthetic demonstration.'
