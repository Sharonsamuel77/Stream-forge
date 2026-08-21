$project = "E:\stream-forge"
$python = "$project\venv\Scripts\python.exe"

for ($i = 1; $i -le 20; $i++) {

    $port = 8100 + $i

    Start-Process `
        -FilePath "powershell.exe" `
        -ArgumentList @(
            "-NoProfile",
            "-Command",
            "`$env:STREAMFORGE_METRICS_PORT='$port'; Set-Location '$project'; & '$python' -m worker.telemetry_worker"
        )

    Write-Host "Started Worker $i | Metrics Port $port"
}