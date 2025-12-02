param(
  [string]$Branch = 'gh-pages',
  [string]$Origin = 'origin',
  [string]$SiteDir = 'site'
)
$ErrorActionPreference = 'Stop'
if (-not (Get-Command git -ErrorAction SilentlyContinue)) { throw 'git not found' }
if (-not (Test-Path $SiteDir)) { throw "Site directory not found: $SiteDir" }
$repoRoot = (git rev-parse --show-toplevel).Trim()
$work = Join-Path $repoRoot '.gh-pages'
# Ensure latest refs
git fetch $Origin | Out-Null
# Recreate/attach worktree to $Branch at HEAD
if (Test-Path $work) { git worktree remove $work -f | Out-Null }
git worktree add -B $Branch $work HEAD | Out-Null
# Clean worktree (keep .git)
Get-ChildItem -Force $work | Where-Object { $_.Name -ne '.git' } | Remove-Item -Recurse -Force
# Copy site contents
robocopy $SiteDir $work /MIR | Out-Null
# Ensure no Jekyll processing
New-Item -ItemType File -Path (Join-Path $work '.nojekyll') -Force | Out-Null
Push-Location $work
  git add -A
  git commit -m ("Deploy site " + (Get-Date -Format s)) 2>$null
  git push $Origin $Branch
Pop-Location
"Deployed to branch '$Branch' at remote '$Origin'"
