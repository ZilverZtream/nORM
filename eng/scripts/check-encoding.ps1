param(
    [switch]$Fix
)

# Scan the repo for double-encoded UTF-8 (mojibake) and replacement characters in tracked source
# files. The release gate calls this in scan-only mode. Pass -Fix to repair files in place by
# re-decoding repairable mojibake as Windows-1252 and re-encoding as UTF-8 - the inverse of the
# common double-encoding that produces sequences like 'a-tilde Euro-sign' for an em dash.

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

# The v1-blocker-spec.md and this script document mojibake sequences as examples; skip them.
$exclusionsRelative = @(
    'docs\v1-blocker-spec.md',
    'eng\scripts\check-encoding.ps1'
)

$targets = Get-ChildItem -Path $root -Recurse -File -Include '*.cs','*.md','*.ps1','*.json','*.csproj','*.props','*.targets' -ErrorAction SilentlyContinue |
    Where-Object {
        $relative = $_.FullName.Substring($root.Length + 1)
        $excluded = $false
        foreach ($ex in $exclusionsRelative) {
            if ($relative -ieq $ex) { $excluded = $true; break }
        }
        if ($excluded) { return $false }
        return $_.FullName -notmatch '\\bin\\|\\obj\\|\\TestResults\\|\\\.git\\|\\BenchmarkDotNet\\|\\artifacts\\|node_modules|\\\.dotnet-home\\|\\\.claude\\'
    }

$replacementChar = [string][char]0xFFFD
$replacementSequence = -join ([char]0x00EF, [char]0x00BF, [char]0x00BD)
$repairableLeadChars = @(
    [string][char]0x00E2, # a-tilde: common lead char for double-encoded dashes/arrows
    [string][char]0x00C3  # A-tilde: common lead char for double-encoded accented letters
)

function Test-TextHasReplacementMarker {
    param([string]$Text)

    return $Text.IndexOf($replacementChar, [StringComparison]::Ordinal) -ge 0 -or
        $Text.IndexOf($replacementSequence, [StringComparison]::Ordinal) -ge 0
}

function Test-TextHasRepairableMojibakeMarker {
    param([string]$Text)

    foreach ($lead in $repairableLeadChars) {
        if ($Text.IndexOf($lead, [StringComparison]::Ordinal) -ge 0) { return $true }
    }
    return $false
}

function Test-TextHasMojibakeMarker {
    param([string]$Text)

    return (Test-TextHasReplacementMarker -Text $Text) -or
        (Test-TextHasRepairableMojibakeMarker -Text $Text)
}

$hits = [System.Collections.Generic.List[string]]::new()

foreach ($file in $targets) {
    $bytes = [IO.File]::ReadAllBytes($file.FullName)
    $hadBom = $bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF
    if ($hadBom) {
        $payloadBytes = New-Object byte[] ($bytes.Length - 3)
        [Array]::Copy($bytes, 3, $payloadBytes, 0, $bytes.Length - 3)
    } else {
        $payloadBytes = $bytes
    }

    $text = [Text.Encoding]::UTF8.GetString($payloadBytes)
    $hasRepairable = Test-TextHasRepairableMojibakeMarker -Text $text
    $hasReplacement = Test-TextHasReplacementMarker -Text $text
    if (-not $hasRepairable -and -not $hasReplacement) { continue }

    $relative = $file.FullName.Substring($root.Length + 1)
    if ($Fix -and $hasRepairable) {
        # Preserve a leading UTF-8 BOM (EF BB BF). Without this guard the round-trip via
        # Windows-1252 mangles the BOM bytes into a literal '?' character, which corrupts the
        # file's first line for compilers that distinguish BOM-as-marker from BOM-as-text.
        $reinterpreted = [Text.Encoding]::GetEncoding(1252).GetBytes($text)
        $repaired = [Text.Encoding]::UTF8.GetString($reinterpreted)
        # Write back with the same BOM state the file had on disk.
        $encoding = [Text.UTF8Encoding]::new($hadBom)
        [IO.File]::WriteAllText($file.FullName, $repaired, $encoding)
        $updatedText = [IO.File]::ReadAllText($file.FullName)
        if (-not (Test-TextHasMojibakeMarker -Text $updatedText)) {
            Write-Host "FIXED $relative (BOM preserved: $hadBom)"
            continue
        }
        $hits.Add("$relative (manual cleanup required after repair)")
        continue
    }

    $hits.Add($relative)
}

if ($hits.Count -gt 0) {
    Write-Host "Mojibake or replacement-character markers detected in:"
    $hits | ForEach-Object { Write-Host "  $_" }
    throw "Encoding check failed: $($hits.Count) file(s) contain mojibake or replacement characters. Run eng/scripts/check-encoding.ps1 -Fix for repairable double-encoding; replace U+FFFD markers manually."
}

if (-not $Fix) {
    Write-Host "Encoding check: no mojibake detected."
}
