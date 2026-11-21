param([Parameter(Mandatory=$true)][string]$Task, [int]$Count=1000, [string]$BatchId)
$ErrorActionPreference = 'Stop'
$taskArgs = @('scripts/manage.py', $Task, '--count', $Count)
if ($BatchId) { $taskArgs += @('--batch-id', $BatchId) }
python @taskArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
