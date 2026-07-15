<#
.SYNOPSIS
    Validates a nORM hardening ticket (docs/hardening/tickets/NH-NNNN.md) and, unless -NoDiff,
    the current change set against that ticket. Enforces the no-overclaim rules in
    docs/hardening/README.md. See ticket NH-0001.

.DESCRIPTION
    Fails (exit 1) when:
      - the ticket id is malformed, missing, or Draft;
      - a required ticket section is absent;
      - Status: Verified while any acceptance criterion is open ([ ]) or partial ([~]);
      - a changed file falls outside the ticket's Allowed paths;
      - added source lines introduce restriction-language with no Parity-audit classification.
    Prints HARDENING-GATE lines and exits 0 on success.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Ticket,
    [switch]$NoDiff,
    [switch]$StagedOnly,
    [switch]$IgnoreUntracked
)

$ErrorActionPreference = 'Stop'

function Fail([string]$message) {
    Write-Host "HARDENING-GATE: FAIL: $message"
    exit 1
}
function Note([string]$message) {
    Write-Host "HARDENING-GATE: $message"
}

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)

# --- 1. Ticket id + file ---------------------------------------------------------------
if ($Ticket -notmatch '^NH-\d{4}$') {
    Fail "Ticket id '$Ticket' is invalid (expected NH-NNNN)."
}
$ticketRel = "docs/hardening/tickets/$Ticket.md"
$ticketPath = Join-Path $repoRoot $ticketRel
if (-not (Test-Path -LiteralPath $ticketPath)) {
    Fail "Ticket file $ticketRel does not exist."
}
$lines = @(Get-Content -LiteralPath $ticketPath)
$text = ($lines -join "`n")

# --- 2. Status -------------------------------------------------------------------------
$status = $null
foreach ($l in $lines) {
    if ($l -match '^Status:\s*(.+?)\s*$') { $status = $Matches[1].Trim(); break }
}
if (-not $status) { Fail "$ticketRel has no 'Status:' field." }
$validStatuses = @('Draft', 'In Progress', 'Verified', 'Quarantined')
if ($validStatuses -notcontains $status) {
    Fail "$ticketRel has invalid Status '$status' (Draft|In Progress|Verified|Quarantined)."
}
if ($status -eq 'Draft') {
    Fail "$ticketRel is Draft; a Draft ticket cannot justify a hardening change."
}

# --- 3. Required sections --------------------------------------------------------------
$required = @('## Non-negotiable scope', 'Allowed paths:', 'Acceptance criteria:', '## Parity audit', '## Required verification')
foreach ($sec in $required) {
    if ($text -notlike "*$sec*") { Fail "$ticketRel is missing required section '$sec'." }
}

# --- helper: extract the block of lines under a header until a stop predicate -----------
function Get-Block([string[]]$all, [string]$startPattern, [string]$stopPattern) {
    $start = -1
    for ($i = 0; $i -lt $all.Count; $i++) {
        if ($all[$i] -match $startPattern) { $start = $i + 1; break }
    }
    if ($start -lt 0) { return @() }
    $end = $all.Count
    for ($j = $start; $j -lt $all.Count; $j++) {
        if ($all[$j] -match $stopPattern) { $end = $j; break }
    }
    if ($end -le $start) { return @() }
    return @($all[$start..($end - 1)])
}

# --- 4. Acceptance criteria + no-overclaim ---------------------------------------------
$acLines = Get-Block $lines '^Acceptance criteria:\s*$' '^##\s'
$acChecks = @($acLines | Where-Object { $_ -match '^\s*-\s*\[( |x|~)\]' })
$openAc = @($acLines | Where-Object { $_ -match '^\s*-\s*\[( |~)\]' })
if ($acChecks.Count -eq 0) { Fail "$ticketRel has no acceptance-criteria checkboxes." }
if ($status -eq 'Verified' -and $openAc.Count -gt 0) {
    Fail "$ticketRel is Status: Verified but has $($openAc.Count) open/partial acceptance criteria (no overclaim)."
}
Note "$Ticket structure valid (Status: $status; acceptance criteria: $($acChecks.Count), open/partial: $($openAc.Count))."

if ($NoDiff) {
    Note "$Ticket ticket structure is valid. Diff checks skipped by -NoDiff."
    exit 0
}

# --- 5. Allowed paths ------------------------------------------------------------------
$allowedRaw = Get-Block $lines '^Allowed paths:\s*$' '^Acceptance criteria:\s*$'
$allowed = New-Object System.Collections.Generic.List[string]
foreach ($l in $allowedRaw) {
    if ($l -match '^\s*-\s*(.+?)\s*$') { $allowed.Add(($Matches[1].Trim())) }
}
$allowed.Add($ticketRel) | Out-Null   # the ticket file itself is always allowed

function Convert-GlobToRegex([string]$glob) {
    $g = [regex]::Escape($glob)
    $g = $g -replace '\\\*\\\*', '.*'    # ** -> any (incl. /)
    $g = $g -replace '\\\*', '[^/]*'     # *  -> any non-slash
    return ('^' + $g + '$')
}
function Test-Allowed([string]$file, $allowedList) {
    $f = ($file -replace '\\', '/')
    foreach ($a in $allowedList) {
        $rx = Convert-GlobToRegex ($a -replace '\\', '/')
        if ($f -match $rx) { return $true }
    }
    return $false
}

function Get-ChangedFiles {
    $files = New-Object System.Collections.Generic.List[string]
    if ($StagedOnly) {
        foreach ($f in @(& git -C $repoRoot diff --name-only --cached)) { if ($f) { $files.Add($f) } }
    }
    else {
        foreach ($f in @(& git -C $repoRoot diff --name-only HEAD)) { if ($f) { $files.Add($f) } }
    }
    if (-not $IgnoreUntracked) {
        foreach ($f in @(& git -C $repoRoot ls-files --others --exclude-standard)) { if ($f) { $files.Add($f) } }
    }
    return @($files | Where-Object { $_.Trim().Length -gt 0 } | Sort-Object -Unique)
}

$changed = Get-ChangedFiles
$outside = @($changed | Where-Object { -not (Test-Allowed $_ $allowed) })
if ($outside.Count -gt 0) {
    Fail ("Changed files outside $Ticket Allowed paths:`n  " + ($outside -join "`n  "))
}

# --- 6. Restriction-language scan on added source lines --------------------------------
$restrict = 'unsupported|not implemented|NotImplementedException|\bTODO\b|\bFIXME\b|\bdeferred\b|non-goal|\blimitation\b'
if ($StagedOnly) { $diff = @(& git -C $repoRoot diff --cached --unified=0) }
else { $diff = @(& git -C $repoRoot diff HEAD --unified=0) }
$curFile = $null
$hits = New-Object System.Collections.Generic.List[string]
foreach ($dl in $diff) {
    if ($dl -match '^\+\+\+ b/(.+)$') { $curFile = $Matches[1]; continue }
    if ($dl -match '^\+' -and $dl -notmatch '^\+\+\+') {
        if ($curFile -and ($curFile -like 'src/*') -and ($dl -match $restrict)) {
            $hits.Add(("{0}: {1}" -f $curFile, $dl.Substring(1).Trim()))
        }
    }
}
if ($hits.Count -gt 0) {
    $classified = ($text -match '(IMPLEMENTATION-DEBT|DESIGN-EXCEPTION|CORRECT-REJECT|INTERNAL-INVARIANT)')
    if (-not $classified) {
        Fail ("Added restriction-language in source without a Parity-audit classification in ${Ticket}:`n  " + (($hits) -join "`n  "))
    }
    Note "Restriction-language present but $Ticket carries a Parity-audit classification ($($hits.Count) hit(s))."
}

Note "$Ticket passed hardening gate against the current change set. Changed files checked: $($changed.Count)."
exit 0
