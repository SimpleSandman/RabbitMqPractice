<#
.SYNOPSIS
    Saves or compares a snapshot of the RabbitMQ topology: exchanges, queues, and bindings.

.DESCRIPTION
    Use this to prove that a refactor did not change the broker's topology.

    1. Wipe the broker:      docker compose down -v ; docker compose up -d
    2. Start the Worker once so it declares every exchange, queue, and binding.
       (The API alone declares only the store.events exchange.)
    3. Save a baseline:      .\scripts\topology-snapshot.ps1 -Mode Save
    4. Refactor, then repeat steps 1 and 2, and compare:
                             .\scripts\topology-snapshot.ps1

    Message counts are left out on purpose, so leftover messages cannot cause a difference.
    The output of each rabbitmqctl command is sorted, so row order does not matter.
    The baseline is written outside the repository, to $HOME\rmq-baseline by default.

    Exit code: 0 when everything matches, 1 when anything differs.

.PARAMETER Mode
    Save writes a baseline. Compare (the default) compares the broker with the saved baseline.

.PARAMETER Dir
    The folder that holds the baseline files.

.PARAMETER Container
    The name of the RabbitMQ container. The Compose file in this repo calls it "rabbitmq".

.PARAMETER Force
    Allows Save to overwrite an existing baseline.
#>
[CmdletBinding()]
param(
    [ValidateSet('Save', 'Compare')]
    [string]$Mode = 'Compare',

    [string]$Dir = (Join-Path $HOME 'rmq-baseline'),

    [string]$Container = 'rabbitmq',

    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# What to capture: the baseline file name, and the rabbitmqctl command with its columns.
$snapshots = [ordered]@{
    'exchanges.txt' = @('list_exchanges', 'name', 'type', 'durable')
    'queues.txt'    = @('list_queues', 'name', 'durable', 'arguments')
    'bindings.txt'  = @('list_bindings', 'source_name', 'destination_name', 'routing_key')
}

function Get-Snapshot {
    param([string[]]$CtlArguments)

    # Windows PowerShell 5.1 turns any stderr line from a native command into a terminating
    # error when 2>&1 is used under 'Stop'. Judge success by the exit code instead.
    $ErrorActionPreference = 'Continue'

    $output = & docker exec $Container rabbitmqctl @CtlArguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "rabbitmqctl $($CtlArguments -join ' ') failed with exit code ${LASTEXITCODE}: $output"
    }

    # Drop the "Timeout:" and "Listing ..." header lines and blank lines, then sort.
    $output |
        ForEach-Object { "$_".TrimEnd() } |
        Where-Object { $_ -and ($_ -notmatch '^(Timeout:|Listing )') } |
        Sort-Object
}

# Fail early, with a useful message, if the container is not running.
$runningContainers = @(& docker ps --format '{{.Names}}')
if ($LASTEXITCODE -ne 0) {
    throw 'Could not run "docker ps". Is Docker Desktop running?'
}
if ($runningContainers -notcontains $Container) {
    throw "Container '$Container' is not running. Start it with: docker compose up -d"
}

if ($Mode -eq 'Save') {
    $existing = @()
    if (Test-Path $Dir) {
        $existing = @(Get-ChildItem -Path $Dir -Filter '*.txt')
    }
    if ($existing.Count -gt 0 -and -not $Force) {
        throw "A baseline already exists in $Dir. Use -Force to overwrite it."
    }

    New-Item -ItemType Directory -Force -Path $Dir | Out-Null

    foreach ($name in $snapshots.Keys) {
        $lines = @(Get-Snapshot $snapshots[$name])
        Set-Content -Path (Join-Path $Dir $name) -Value $lines -Encoding UTF8
        Write-Host ('Saved {0} ({1} lines)' -f $name, $lines.Count)
    }

    Write-Host "Baseline saved to $Dir"
    return
}

# Compare mode.
$differentFiles = 0

foreach ($name in $snapshots.Keys) {
    $path = Join-Path $Dir $name
    if (-not (Test-Path $path)) {
        throw "No baseline file $name in $Dir. Run with -Mode Save first."
    }

    $expected = @(Get-Content -Path $path)
    $actual = @(Get-Snapshot $snapshots[$name])
    $differences = @(Compare-Object -ReferenceObject $expected -DifferenceObject $actual)

    if ($differences.Count -gt 0) {
        $differentFiles++
        Write-Host "DIFFERENT: $name" -ForegroundColor Red
        foreach ($difference in $differences) {
            $where = if ($difference.SideIndicator -eq '<=') { 'only in the baseline' } else { 'only on the broker now' }
            Write-Host ('  {0}: {1}' -f $where, $difference.InputObject)
        }
    }
    else {
        Write-Host "MATCH: $name" -ForegroundColor Green
    }
}

if ($differentFiles -gt 0) {
    exit 1
}
