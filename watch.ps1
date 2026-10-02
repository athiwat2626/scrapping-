# แสดงสถานะการสแคปแบบสด อัปเดตทุก 15 วินาที (Ctrl+C เพื่อออก - การสแคปยังทำงานต่อ)
# ใช้: .\watch.ps1        หรือ  .\watch.ps1 5   (อัปเดตทุก 5 วินาที)
param([int]$Seconds = 15)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location $PSScriptRoot
docker compose exec airflow-scheduler python -u /opt/airflow/scripts/watch.py $Seconds
