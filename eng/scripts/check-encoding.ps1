param(
    [switch]$Fix
)

# Scan the repo for double-encoded UTF-8 (mojibake) in tracked source files. The release gate
# calls this in scan-only mode. Pass -Fix to repair files in place by re-decoding their text as
# Windows-1252 and re-encoding as UTF-8 - the inverse of the double-encoding that produces
# sequences like 'a-tilde Euro-sign' for an em dash.
#
# Detection works at the BYTE level using known mojibake byte sequences so the script itself is
# pure ASCII and cannot become double-encoded.

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

# Mojibake byte signatures. The first byte 0xC3 (a-tilde in UTF-8) followed by other multi-byte
# UTF-8 sequences from the 0xC2-0xC5 / 0xE2 ranges is the signature of Windows-1252 text
# mis-decoded as UTF-8 then re-encoded. We look for the most common variants.
function Test-FileHasMojibake {
    param([byte[]]$Bytes)
    # Build signatures as raw byte arrays (PowerShell array-of-arrays syntax is unreliable).
    $sigA = [byte[]](0xC3, 0xA2, 0xE2, 0x82, 0xAC)      # 'a-tilde Euro' prefix of em dash, en dash, etc.
    $sigB = [byte[]](0xC3, 0x83, 0xC2, 0xA9)            # mojibake e-acute
    $sigC = [byte[]](0xC3, 0x83, 0xC2, 0xB6)            # mojibake o-umlaut
    $sigD = [byte[]](0xC3, 0x83, 0xC2, 0xA4)            # mojibake a-umlaut
    $sigE = [byte[]](0xC3, 0x83, 0xC2, 0xBC)            # mojibake u-umlaut
    $signatures = @(,$sigA) + @(,$sigB) + @(,$sigC) + @(,$sigD) + @(,$sigE)
    foreach ($sig in $signatures) {
        if ($Bytes.Length -lt $sig.Length) { continue }
        for ($i = 0; $i -le ($Bytes.Length - $sig.Length); $i++) {
            $match = $true
            for ($j = 0; $j -lt $sig.Length; $j++) {
                if ($Bytes[$i + $j] -ne $sig[$j]) { $match = $false; break }
            }
            if ($match) { return $true }
        }
    }
    return $false
}

$hits = [System.Collections.Generic.List[string]]::new()

foreach ($file in $targets) {
    $bytes = [IO.File]::ReadAllBytes($file.FullName)
    if (-not (Test-FileHasMojibake -Bytes $bytes)) { continue }

    $relative = $file.FullName.Substring($root.Length + 1)
    if ($Fix) {
        # Preserve a leading UTF-8 BOM (EF BB BF). Without this guard the round-trip via
        # Windows-1252 mangles the BOM bytes into a literal '?' character, which corrupts the
        # file's first line for compilers that distinguish BOM-as-marker from BOM-as-text.
        $hadBom = $bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF
        if ($hadBom) {
            # Copy [3..end] into a fresh byte[] using Array.Copy rather than PowerShell range
            # slicing, which silently returned Object[] subsets that broke UTF-8 round-tripping.
            $payloadBytes = New-Object byte[] ($bytes.Length - 3)
            [Array]::Copy($bytes, 3, $payloadBytes, 0, $bytes.Length - 3)
        } else {
            $payloadBytes = $bytes
        }
        $text = [Text.Encoding]::UTF8.GetString($payloadBytes)
        $reinterpreted = [Text.Encoding]::GetEncoding(1252).GetBytes($text)
        $repaired = [Text.Encoding]::UTF8.GetString($reinterpreted)
        # Write back with the same BOM state the file had on disk.
        $encoding = [Text.UTF8Encoding]::new($hadBom)
        [IO.File]::WriteAllText($file.FullName, $repaired, $encoding)
        Write-Host "FIXED $relative (BOM preserved: $hadBom)"
    } else {
        $hits.Add($relative)
    }
}

if (-not $Fix) {
    if ($hits.Count -gt 0) {
        Write-Host "Mojibake sequences detected in:"
        $hits | ForEach-Object { Write-Host "  $_" }
        throw "Encoding check failed: $($hits.Count) file(s) contain double-encoded text. Run eng/scripts/check-encoding.ps1 -Fix to repair."
    }
    Write-Host "Encoding check: no mojibake detected."
}
