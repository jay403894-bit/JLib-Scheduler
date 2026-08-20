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
    [switch]$WhatIf,
    # Release anyway when the section is below the threshold. A genuine hotfix is allowed to be
    # three lines; this switch exists so that is a DECISION rather than something nobody noticed.
    [switch]$Small,
    [int]$MinChangelogLines = 20
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

# ---- 2b. the section must actually say something ----------------------------------------------
# Not a style rule. Every correction in 1.5.0 -- three wrong published figures, a bench header
# advertising a gate that had been removed a release earlier -- was found by USING 1.4, after 1.4
# had shipped. That is the normal discovery path, so fixes will always straggle in behind a tag.
# The failure mode is tagging each straggler on its own until the version number stops carrying
# information.
#
# The threshold is calibrated, not invented. Measured across every section in this file: the
# smallest release ever shipped was 1.2.1 at 12 non-blank lines, and everything from 1.2.2 onward
# is 28 or more. So 20 clears the entire recent history and only trips a genuinely thin release.
$clLines   = Get-Content 'CHANGELOG.md'
$bodyLines = 0
for ($i = $h.LineNumber; $i -lt $clLines.Count; $i++) {
    if ($clLines[$i] -match '^## ') { break }
    if ($clLines[$i].Trim().Length -gt 0) { $bodyLines++ }
}
Step "section   : $bodyLines non-blank lines"
if ($bodyLines -lt $MinChangelogLines -and -not $Small) {
    Fail "the $version section is only $bodyLines non-blank lines (threshold $MinChangelogLines).
  Either this release is thinner than it should be -- let main accumulate, the '## Unreleased'
  heading exists for exactly that and nothing forces a tag -- or it is a deliberate hotfix, in
  which case re-run with -Small. Raise the bar for one run with -MinChangelogLines <n>."
}

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

# ---- 6. the GitHub Release itself, not just the tag --------------------------------------------
# This step was MISSING for three releases in a row (2.1.0, 2.2.0, 2.3.0) before anyone noticed --
# the tag existed, CI ran green against it, but the Releases page still said 2.0.0 was "Latest".
# `git push origin $tag` publishes the tag; it does not create a Release. The two are different
# GitHub objects and only one of them is what a reader (or `gh release list`) actually sees as
# "the last release". Same $Message used for the tag's own annotation, since it is already written
# as a one-line description -- see this script's header for the intended shape of that parameter.
# Notes body is the CHANGELOG's own section for $version, extracted the same way the size-guard
# above already parses it, so the two can never say different things about the same release.
$notesLines = @()
for ($i = $h.LineNumber; $i -lt $clLines.Count; $i++) {
    if ($clLines[$i] -match '^## ') { break }
    $notesLines += $clLines[$i]
}
$notesFile = New-TemporaryFile
Set-Content -Path $notesFile -Value ($notesLines -join "`n") -Encoding utf8
try {
    gh release create $tag --title $Message --notes-file $notesFile
    if ($LASTEXITCODE -ne 0) { throw "gh release create exited $LASTEXITCODE" }
}
catch {
    Write-Host "WARNING: tag $tag is pushed and CI will run, but the GitHub Release was NOT created: $_" -ForegroundColor Yellow
    Write-Host "  Fix by hand: gh release create $tag --title `"$Message`" --notes-file $notesFile" -ForegroundColor Yellow
}
finally {
    Remove-Item $notesFile -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "released $tag" -ForegroundColor Green
Write-Host "CI: gh run list --limit 3" -ForegroundColor DarkGray
