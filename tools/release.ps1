# Cut a release. Reads the version from CMakeLists.txt, so there is nothing to type and nothing to
# get out of sync -- that file is already the single source of truth the bench stamps its output
# with.
#
#   .\tools\release.ps1
#   .\tools\release.ps1 -Message "1.2.1: fix the macOS link failure"
#   .\tools\release.ps1 -WhatIf        # print what would happen and stop
#
# Exists because the manual sequence is four commands, PowerShell 5.1 has no `&&` to chain them, and
# every mistake made so far has been in the gaps between them:
#   * tagged while CHANGELOG.md still said "unreleased" (twice)
#   * tagged, then had to re-point the tag because a later edit belonged in it
#   * created a GitHub Release for a tag that already existed, and got HTTP 422
# Each of those is a guard below, checked BEFORE anything is committed or pushed.

param(
    [string]$Message = "",
    [switch]$WhatIf
)

$ErrorActionPreference = 'Stop'

function Fail($text) { Write-Host "FAILED: $text" -ForegroundColor Red; exit 1 }
function Step($text) { Write-Host $text -ForegroundColor Cyan }

$root = git rev-parse --show-toplevel 2>$null
if (-not $root) { Fail "not inside a git repository" }
Set-Location $root

# ---- 1. version, from the one place it lives --------------------------------------------------
$m = Select-String -Path 'CMakeLists.txt' -Pattern 'set\(JLIBSCHED_VERSION "([0-9]+\.[0-9]+\.[0-9]+)"\)' |
     Select-Object -First 1
if (-not $m) { Fail "no JLIBSCHED_VERSION in CMakeLists.txt" }
$version = $m.Matches[0].Groups[1].Value
$tag = "v$version"
Step "version   : $version"

# ---- 2. the changelog must be DATED, not 'unreleased' -----------------------------------------
# This is the one that has bitten twice. A tag pointing at a commit whose own changelog says the
# release has not happened is worse than useless, and it cannot be fixed after pushing.
$h = Select-String -Path 'CHANGELOG.md' -Pattern "^## $([regex]::Escape($version)) - (.+)$" |
     Select-Object -First 1
if (-not $h) { Fail "CHANGELOG.md has no '## $version - ...' heading" }
$when = $h.Matches[0].Groups[1].Value.Trim()
if ($when -match '(?i)unreleased') {
    Fail "CHANGELOG.md still reads '## $version - unreleased'. Date it (today is $(Get-Date -Format 'yyyy-MM-dd'))."
}
Step "changelog : $version - $when"

# ---- 3. the tag must not already exist, locally or on the remote -------------------------------
if (git tag -l $tag) { Fail "$tag already exists locally. Bump the version, or delete it if it was never pushed." }
if (git ls-remote --tags origin $tag 2>$null) { Fail "$tag already exists on origin. Bump the version." }

# ---- 4. show the damage ------------------------------------------------------------------------
if (-not $Message) { $Message = $version }
$dirty = git status --porcelain
if ($dirty) {
    Step "will commit:"
    git status --short
} else {
    Write-Host "working tree clean, tagging the current commit" -ForegroundColor DarkGray
}
Step "will tag  : $tag  ($Message)"

if ($WhatIf) { Write-Host "-WhatIf: stopping before any change" -ForegroundColor Yellow; exit 0 }

# ---- 5. do it ----------------------------------------------------------------------------------
if ($dirty) {
    git add -A
    git commit -m $Message
    if ($LASTEXITCODE -ne 0) { Fail "commit failed" }
}

# ANNOTATED, not lightweight: an annotated tag carries its own tagger, date and message, and
# `git describe` prefers it without needing --tags. The GitHub Releases UI creates lightweight ones,
# which is why v1.1.0 and v1.1.1 differ from v1.0.0.
git tag -a $tag -m $Message
if ($LASTEXITCODE -ne 0) { Fail "tag failed" }

git push
if ($LASTEXITCODE -ne 0) { Fail "push failed -- the tag exists locally but nothing was published" }

# `git push` never pushes tags. This is the step people forget.
git push origin $tag
if ($LASTEXITCODE -ne 0) { Fail "tag push failed -- the commit IS pushed, so just re-run: git push origin $tag" }

Write-Host ""
Write-Host "released $tag" -ForegroundColor Green
Write-Host "CI: gh run list --limit 3" -ForegroundColor DarkGray
