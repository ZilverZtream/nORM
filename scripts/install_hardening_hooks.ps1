<#
.SYNOPSIS
    Wires the nORM hardening pre-commit hook into this clone by pointing git at .githooks.
.DESCRIPTION
    Sets `core.hooksPath = .githooks` so .githooks/pre-commit runs on every commit. That hook
    is a no-op unless NORM_HARDENING_TICKET is set, so ordinary commits are unaffected; set
    NORM_HARDENING_TICKET=NH-NNNN before committing a hardening change to gate it. See NH-0001.
    Reverse with: git config --unset core.hooksPath
#>
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
& git -C $repoRoot config core.hooksPath .githooks
Write-Host "Installed nORM hardening hook: core.hooksPath = .githooks"
Write-Host "Ordinary commits are unaffected. For a hardening change:"
Write-Host "  set NORM_HARDENING_TICKET=NH-NNNN before 'git commit' to gate it against that ticket."
