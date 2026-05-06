$ErrorActionPreference = "Stop"

Set-Location -Path (Split-Path -Parent $PSScriptRoot)
npm.cmd install
npm.cmd start
