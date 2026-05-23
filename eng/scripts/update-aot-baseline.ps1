param(
    [string]$Configuration = 'Release'
)

# Regenerate eng/aot-baseline.txt from a fresh `dotnet publish -p:PublishAot=true` of the runtime
# project. Use after annotating new reflection sites with [RequiresDynamicCode] /
# [RequiresUnreferencedCode] to lock in the reduced diagnostic count, or when a deliberate new
# dynamic-code site is added with an explanation in the same change.

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$runtimeProject = Join-Path $root 'src\nORM.csproj'
$baselinePath = Join-Path $root 'eng\aot-baseline.txt'

Write-Host "Building runtime AOT diagnostics ..."
$out = dotnet publish $runtimeProject -c $Configuration -r linux-x64 --self-contained -p:PublishAot=true --nologo 2>&1 | Out-String

$diagnosticRegex = [regex]'(?m)^(?<file>.+?)\((?<line>\d+),(?<col>\d+)\):\s+(?:error|warning)\s+(?<code>IL\d{4}):'
$prefix = ($root + '\')
$entries = $diagnosticRegex.Matches($out) | ForEach-Object {
    $rel = ($_.Groups['file'].Value -replace [regex]::Escape($prefix), '') -replace '\\', '/'
    '{0}:{1}:{2}' -f $rel, $_.Groups['line'].Value, $_.Groups['code'].Value
} | Sort-Object -Unique

$header = @(
    '# nORM AOT publish diagnostic baseline.',
    '# Format: relative/path/from/repo/root.cs:line:ILxxxx',
    '# Regenerate via eng/scripts/update-aot-baseline.ps1 after annotating new reflection sites.',
    '# The release gate fails on any IL diagnostic NOT in this file.'
)

($header + $entries) | Set-Content -Encoding ASCII -LiteralPath $baselinePath
Write-Host ("Wrote {0} unique diagnostics to {1}" -f @($entries).Count, $baselinePath)
# `dotnet publish` exits non-zero when IL diagnostics are present; that is expected by this
# helper. Reset $LASTEXITCODE so callers see a successful baseline regeneration.
$global:LASTEXITCODE = 0
exit 0
