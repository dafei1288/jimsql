param(
  [string]$Config = 'site/tailwind.config.js',
  [string]$In = 'site/styles/input.css',
  [string]$Out = 'site/assets/site.css'
)
$ErrorActionPreference='Stop'
if (-not (Get-Command npx -ErrorAction SilentlyContinue)) { Write-Host 'npx not found. Please install Node.js (includes npx) from https://nodejs.org/' -ForegroundColor Yellow; exit 1 }
& npx tailwindcss -c $Config -i $In -o $Out --minify
Write-Host "Built CSS -> $Out" -ForegroundColor Green
