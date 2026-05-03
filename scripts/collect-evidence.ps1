param(
  [string]$OutputRoot = "docs/evidence/latest"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$ProgressPreference = "SilentlyContinue"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot

$rabbitCompose = ".\docker-compose.rabbitmq.yml"
$kafkaCompose = ".\docker-compose.kafka.yml"
$fullOutputRoot = Join-Path $repoRoot $OutputRoot

function Ensure-Directory {
  param([string]$Path)

  if ([string]::IsNullOrWhiteSpace($Path)) {
    return
  }

  New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Write-Utf8File {
  param(
    [string]$Path,
    [string]$Content
  )

  $parent = Split-Path -Parent $Path
  Ensure-Directory -Path $parent
  Set-Content -Path $Path -Encoding UTF8 -Value $Content
}

function Clean-NativeOutput {
  param([string]$Text)

  $lines = $Text -split "`r?`n"
  $filtered = foreach ($line in $lines) {
    if (
      $line -match "^docker\s*:\s" -or
      $line -match "^\s*En\s.+\.ps1:" -or
      $line -match "^\s*\+\s" -or
      $line -match "^\s*CategoryInfo\s*:" -or
      $line -match "^\s*FullyQualifiedErrorId\s*:"
    ) {
      continue
    }

    $line
  }

  return ($filtered -join "`r`n").Trim()
}

function Save-CommandOutput {
  param(
    [string]$Path,
    [scriptblock]$Script,
    [switch]$AllowFailure
  )

  $originalErrorActionPreference = $ErrorActionPreference
  try {
    $ErrorActionPreference = "Continue"
    $output = & $Script 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $originalErrorActionPreference
  }
  $cleanOutput = Clean-NativeOutput -Text $output
  Write-Utf8File -Path $Path -Content $cleanOutput

  if (-not $AllowFailure -and $exitCode -ne 0) {
    throw "Command failed for $Path with exit code $exitCode."
  }

  return $cleanOutput
}

function Get-ComposeContainerId {
  param(
    [string]$ComposeFile,
    [string]$Service
  )

  $containerId = (& docker compose -f $ComposeFile ps -q $Service).Trim()
  if ([string]::IsNullOrWhiteSpace($containerId)) {
    throw "Container for service '$Service' was not found in $ComposeFile."
  }

  return $containerId
}

function Wait-ForHealthEndpoint {
  param(
    [string]$Url,
    [int]$TimeoutSec = 180
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      $response = Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 10
      if ($response.brokerReady -eq $true) {
        return $response
      }
    } catch {
      Start-Sleep -Seconds 2
      continue
    }

    Start-Sleep -Seconds 2
  }

  throw "Timed out waiting for healthy endpoint at $Url."
}

function Invoke-JsonPost {
  param(
    [string]$Url,
    [hashtable]$Payload,
    [string]$SavePath
  )

  $tempFile = Join-Path $env:TEMP ("codex-request-" + [guid]::NewGuid().ToString() + ".json")
  try {
    $json = $Payload | ConvertTo-Json -Compress
    Write-Utf8File -Path $tempFile -Content $json

    $rawOutput = & curl.exe --silent --show-error --write-out "`nTIME_TOTAL=%{time_total}`nHTTP_CODE=%{http_code}`n" --header "Content-Type: application/json" --data-binary "@$tempFile" $Url
    $outputText = ($rawOutput | Out-String).TrimEnd()
    Write-Utf8File -Path $SavePath -Content $outputText

    $bodyText = [regex]::Replace($outputText, "(\r?\nTIME_TOTAL=.*\r?\nHTTP_CODE=.*)$", "").Trim()
    $timeTotal = [regex]::Match($outputText, "TIME_TOTAL=([0-9.]+)").Groups[1].Value
    $httpCode = [regex]::Match($outputText, "HTTP_CODE=([0-9]+)").Groups[1].Value

    return [pscustomobject]@{
      raw = $outputText
      bodyText = $bodyText
      body = ($bodyText | ConvertFrom-Json)
      timeTotal = [double]$timeTotal
      httpCode = [int]$httpCode
    }
  } finally {
    Remove-Item -Force -ErrorAction SilentlyContinue $tempFile
  }
}

function Wait-ForLogContains {
  param(
    [string]$ComposeFile,
    [string]$Service,
    [string]$Needle,
    [int]$TimeoutSec = 90
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    $logs = (& docker compose -f $ComposeFile logs --tail=300 $Service 2>&1 | Out-String).TrimEnd()
    if ($logs -match [regex]::Escape($Needle)) {
      return $logs
    }

    Start-Sleep -Seconds 2
  }

  throw "Timed out waiting for '$Needle' in logs for service '$Service'."
}

function Get-CorrelationIdFromLog {
  param(
    [string]$LogText,
    [string]$OrderId
  )

  foreach ($line in ($LogText -split "`r?`n")) {
    if ($line -match [regex]::Escape($OrderId) -and $line -match "correlationId=([0-9a-fA-F-]+)") {
      return $matches[1]
    }
  }

  return $null
}

function Get-RabbitQueueSnapshot {
  param([string]$ComposeFile)

  $rabbitId = Get-ComposeContainerId -ComposeFile $ComposeFile -Service "rabbitmq"
  return (& docker exec $rabbitId rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers 2>&1 | Out-String).TrimEnd()
}

function Wait-ForRabbitQueuePattern {
  param(
    [string]$ComposeFile,
    [string]$Pattern,
    [int]$TimeoutSec = 60
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    $snapshot = Get-RabbitQueueSnapshot -ComposeFile $ComposeFile
    if ($snapshot -match $Pattern) {
      return $snapshot
    }

    Start-Sleep -Seconds 2
  }

  throw "Timed out waiting for RabbitMQ queue pattern '$Pattern'."
}

function Get-KafkaExecOutput {
  param(
    [string]$ComposeFile,
    [string]$CommandLine
  )

  $kafkaId = Get-ComposeContainerId -ComposeFile $ComposeFile -Service "kafka"
  $originalErrorActionPreference = $ErrorActionPreference
  try {
    $ErrorActionPreference = "Continue"
    return (& docker exec $kafkaId /bin/bash -lc $CommandLine 2>&1 | Out-String).TrimEnd()
  } finally {
    $ErrorActionPreference = $originalErrorActionPreference
  }
}

function Save-KafkaExecOutput {
  param(
    [string]$ComposeFile,
    [string]$Path,
    [string]$CommandLine,
    [switch]$AllowFailure
  )

  $output = Get-KafkaExecOutput -ComposeFile $ComposeFile -CommandLine $CommandLine
  Write-Utf8File -Path $Path -Content $output

  if (-not $AllowFailure -and $LASTEXITCODE -ne 0) {
    throw "Kafka exec command failed for $Path with exit code $LASTEXITCODE."
  }

  return $output
}

function New-OrderPayload {
  param(
    [string]$CustomerName,
    [string]$Product,
    [int]$Quantity,
    [int]$UnitPrice = 0
  )

  $payload = [ordered]@{
    customerName = $CustomerName
    product = $Product
    quantity = $Quantity
  }

  if ($UnitPrice -gt 0) {
    $payload.unitPrice = $UnitPrice
  }

  return $payload
}

if (Test-Path $fullOutputRoot) {
  Remove-Item -Recurse -Force $fullOutputRoot
}

Ensure-Directory -Path $fullOutputRoot

$summary = [ordered]@{
  generatedAt = (Get-Date).ToString("s")
  repoRoot = $repoRoot
  evidenceRoot = $OutputRoot
  rabbitmq = [ordered]@{}
  kafka = [ordered]@{}
}

$rabbitDir = Join-Path $fullOutputRoot "rabbitmq"
$kafkaDir = Join-Path $fullOutputRoot "kafka"
Ensure-Directory -Path $rabbitDir
Ensure-Directory -Path $kafkaDir

# RabbitMQ evidence
Save-CommandOutput -Path (Join-Path $rabbitDir "00-down.txt") -Script { docker compose -f $rabbitCompose down --remove-orphans } -AllowFailure
Save-CommandOutput -Path (Join-Path $rabbitDir "01-up.txt") -Script { docker compose -f $rabbitCompose up -d --build }
$rabbitHealth = Wait-ForHealthEndpoint -Url "http://localhost:3000/health"
Write-Utf8File -Path (Join-Path $rabbitDir "02-health.json") -Content (($rabbitHealth | ConvertTo-Json -Depth 5))
Save-CommandOutput -Path (Join-Path $rabbitDir "03-ps.txt") -Script { docker compose -f $rabbitCompose ps }
Save-CommandOutput -Path (Join-Path $rabbitDir "04-orders-api-start.log") -Script { docker compose -f $rabbitCompose logs --tail=80 orders-api }
Save-CommandOutput -Path (Join-Path $rabbitDir "05-notification-start.log") -Script { docker compose -f $rabbitCompose logs --tail=80 notification-worker }
Write-Utf8File -Path (Join-Path $rabbitDir "06-queue-state-initial.txt") -Content (Get-RabbitQueueSnapshot -ComposeFile $rabbitCompose)

$rabbitCp01 = Invoke-JsonPost -Url "http://localhost:3000/orders" -Payload (New-OrderPayload -CustomerName "Pedro" -Product "Libro de arquitectura" -Quantity 1) -SavePath (Join-Path $rabbitDir "07-cp01-response.txt")
$rabbitOrdersLogCp01 = Wait-ForLogContains -ComposeFile $rabbitCompose -Service "orders-api" -Needle $rabbitCp01.body.orderId
$rabbitWorkerLogCp01 = Wait-ForLogContains -ComposeFile $rabbitCompose -Service "notification-worker" -Needle $rabbitCp01.body.orderId
$rabbitQueueAfterCp01 = Wait-ForRabbitQueuePattern -ComposeFile $rabbitCompose -Pattern "pedidos\s+0\s+0\s+1"
Write-Utf8File -Path (Join-Path $rabbitDir "08-orders-api-after-cp01.log") -Content $rabbitOrdersLogCp01
Write-Utf8File -Path (Join-Path $rabbitDir "09-notification-after-cp01.log") -Content $rabbitWorkerLogCp01
Write-Utf8File -Path (Join-Path $rabbitDir "10-queue-state-after-cp01.txt") -Content $rabbitQueueAfterCp01

Save-CommandOutput -Path (Join-Path $rabbitDir "11-stop-worker.txt") -Script { docker compose -f $rabbitCompose stop notification-worker }
$rabbitCp09 = Invoke-JsonPost -Url "http://localhost:3000/orders" -Payload (New-OrderPayload -CustomerName "Laura" -Product "Curso de QA" -Quantity 2) -SavePath (Join-Path $rabbitDir "12-cp09-response.txt")
$rabbitOrdersLogCp09 = Wait-ForLogContains -ComposeFile $rabbitCompose -Service "orders-api" -Needle $rabbitCp09.body.orderId
$rabbitQueueReady = Wait-ForRabbitQueuePattern -ComposeFile $rabbitCompose -Pattern "pedidos\s+1\s+0\s+0"
Write-Utf8File -Path (Join-Path $rabbitDir "13-orders-api-after-cp09.log") -Content $rabbitOrdersLogCp09
Write-Utf8File -Path (Join-Path $rabbitDir "14-queue-state-worker-stopped.txt") -Content $rabbitQueueReady
Save-CommandOutput -Path (Join-Path $rabbitDir "15-start-worker.txt") -Script { docker compose -f $rabbitCompose up -d notification-worker }
$rabbitWorkerRecoveryLog = Wait-ForLogContains -ComposeFile $rabbitCompose -Service "notification-worker" -Needle $rabbitCp09.body.orderId
$rabbitQueueRecovered = Wait-ForRabbitQueuePattern -ComposeFile $rabbitCompose -Pattern "pedidos\s+0\s+0\s+1"
Write-Utf8File -Path (Join-Path $rabbitDir "16-notification-after-recovery.log") -Content $rabbitWorkerRecoveryLog
Write-Utf8File -Path (Join-Path $rabbitDir "17-queue-state-after-recovery.txt") -Content $rabbitQueueRecovered

Save-CommandOutput -Path (Join-Path $rabbitDir "18-scale-workers.txt") -Script { docker compose -f $rabbitCompose up -d --scale notification-worker=2 }
Save-CommandOutput -Path (Join-Path $rabbitDir "19-ps-scaled.txt") -Script { docker compose -f $rabbitCompose ps }
$rabbitElasticResponses = New-Object System.Collections.Generic.List[string]
for ($i = 1; $i -le 4; $i++) {
  $elasticResponse = Invoke-JsonPost -Url "http://localhost:3000/orders" -Payload (New-OrderPayload -CustomerName ("Cliente " + $i) -Product ("Producto " + $i) -Quantity $i) -SavePath (Join-Path $rabbitDir ("20-elastic-response-" + $i + ".txt"))
  $rabbitElasticResponses.Add($elasticResponse.body.orderId)
}
Start-Sleep -Seconds 8
$rabbitElasticLogs = Save-CommandOutput -Path (Join-Path $rabbitDir "21-notification-after-scale.log") -Script { docker compose -f $rabbitCompose logs --tail=120 notification-worker }

$summary.rabbitmq = [ordered]@{
  cp01 = [ordered]@{
    orderId = $rabbitCp01.body.orderId
    httpCode = $rabbitCp01.httpCode
    timeTotal = $rabbitCp01.timeTotal
    correlationId = Get-CorrelationIdFromLog -LogText $rabbitOrdersLogCp01 -OrderId $rabbitCp01.body.orderId
  }
  cp09 = [ordered]@{
    orderId = $rabbitCp09.body.orderId
    httpCode = $rabbitCp09.httpCode
    queueReadyPatternMatched = $true
    correlationId = Get-CorrelationIdFromLog -LogText $rabbitOrdersLogCp09 -OrderId $rabbitCp09.body.orderId
  }
  cp10 = [ordered]@{
    scaledWorkers = 2
    orderIds = $rabbitElasticResponses
    worker1Seen = ($rabbitElasticLogs -match "notification-worker-1")
    worker2Seen = ($rabbitElasticLogs -match "notification-worker-2")
  }
}

# Kafka evidence
Save-CommandOutput -Path (Join-Path $rabbitDir "99-down.txt") -Script { docker compose -f $rabbitCompose down --remove-orphans }
Save-CommandOutput -Path (Join-Path $kafkaDir "00-down.txt") -Script { docker compose -f $kafkaCompose down --remove-orphans } -AllowFailure
Save-CommandOutput -Path (Join-Path $kafkaDir "01-up.txt") -Script { docker compose -f $kafkaCompose up -d --build }
$kafkaHealth = Wait-ForHealthEndpoint -Url "http://localhost:3000/health"
Write-Utf8File -Path (Join-Path $kafkaDir "02-health.json") -Content (($kafkaHealth | ConvertTo-Json -Depth 5))
Save-CommandOutput -Path (Join-Path $kafkaDir "03-ps.txt") -Script { docker compose -f $kafkaCompose ps }
Save-KafkaExecOutput -ComposeFile $kafkaCompose -Path (Join-Path $kafkaDir "04-topic-describe.txt") -CommandLine "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders.events"
Save-CommandOutput -Path (Join-Path $kafkaDir "05-orders-api-start.log") -Script { docker compose -f $kafkaCompose logs --tail=120 orders-api }
Save-CommandOutput -Path (Join-Path $kafkaDir "06-inventory-start.log") -Script { docker compose -f $kafkaCompose logs --tail=120 inventory-consumer }
Save-CommandOutput -Path (Join-Path $kafkaDir "07-billing-start.log") -Script { docker compose -f $kafkaCompose logs --tail=120 billing-consumer }
Save-CommandOutput -Path (Join-Path $kafkaDir "08-notification-start.log") -Script { docker compose -f $kafkaCompose logs --tail=120 notification-consumer }

$kafkaCp01 = Invoke-JsonPost -Url "http://localhost:3000/orders" -Payload (New-OrderPayload -CustomerName "Pedro" -Product "Libro de arquitectura" -Quantity 1 -UnitPrice 85000) -SavePath (Join-Path $kafkaDir "09-cp01-response.txt")
$kafkaOrdersLogCp01 = Wait-ForLogContains -ComposeFile $kafkaCompose -Service "orders-api" -Needle $kafkaCp01.body.orderId
$kafkaInventoryLogCp01 = Wait-ForLogContains -ComposeFile $kafkaCompose -Service "inventory-consumer" -Needle $kafkaCp01.body.orderId
$kafkaBillingLogCp01 = Wait-ForLogContains -ComposeFile $kafkaCompose -Service "billing-consumer" -Needle $kafkaCp01.body.orderId
$kafkaNotificationLogCp01 = Wait-ForLogContains -ComposeFile $kafkaCompose -Service "notification-consumer" -Needle $kafkaCp01.body.orderId
Write-Utf8File -Path (Join-Path $kafkaDir "10-orders-api-after-cp01.log") -Content $kafkaOrdersLogCp01
Write-Utf8File -Path (Join-Path $kafkaDir "11-inventory-after-cp01.log") -Content $kafkaInventoryLogCp01
Write-Utf8File -Path (Join-Path $kafkaDir "12-billing-after-cp01.log") -Content $kafkaBillingLogCp01
Write-Utf8File -Path (Join-Path $kafkaDir "13-notification-after-cp01.log") -Content $kafkaNotificationLogCp01
Save-KafkaExecOutput -ComposeFile $kafkaCompose -Path (Join-Path $kafkaDir "14-consumer-groups-after-cp01.txt") -CommandLine "/opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups"

Save-CommandOutput -Path (Join-Path $kafkaDir "15-stop-notification.txt") -Script { docker compose -f $kafkaCompose stop notification-consumer }
$kafkaCp09 = Invoke-JsonPost -Url "http://localhost:3000/orders" -Payload (New-OrderPayload -CustomerName "Laura" -Product "Curso Kafka" -Quantity 2 -UnitPrice 91000) -SavePath (Join-Path $kafkaDir "16-cp09-response.txt")
$kafkaOrdersLogCp09 = Wait-ForLogContains -ComposeFile $kafkaCompose -Service "orders-api" -Needle $kafkaCp09.body.orderId
$kafkaInventoryLogCp09 = Wait-ForLogContains -ComposeFile $kafkaCompose -Service "inventory-consumer" -Needle $kafkaCp09.body.orderId
$kafkaBillingLogCp09 = Wait-ForLogContains -ComposeFile $kafkaCompose -Service "billing-consumer" -Needle $kafkaCp09.body.orderId
$kafkaNotificationStoppedLog = Save-CommandOutput -Path (Join-Path $kafkaDir "17-notification-while-stopped.log") -Script { docker compose -f $kafkaCompose logs --tail=120 notification-consumer }
Write-Utf8File -Path (Join-Path $kafkaDir "18-orders-api-after-cp09.log") -Content $kafkaOrdersLogCp09
Write-Utf8File -Path (Join-Path $kafkaDir "19-inventory-after-cp09.log") -Content $kafkaInventoryLogCp09
Write-Utf8File -Path (Join-Path $kafkaDir "20-billing-after-cp09.log") -Content $kafkaBillingLogCp09
Save-KafkaExecOutput -ComposeFile $kafkaCompose -Path (Join-Path $kafkaDir "21-consumer-groups-while-stopped.txt") -CommandLine "/opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notification-service-group" -AllowFailure
Save-CommandOutput -Path (Join-Path $kafkaDir "22-start-notification.txt") -Script { docker compose -f $kafkaCompose up -d notification-consumer }
$kafkaNotificationRecoveryLog = Wait-ForLogContains -ComposeFile $kafkaCompose -Service "notification-consumer" -Needle $kafkaCp09.body.orderId
Write-Utf8File -Path (Join-Path $kafkaDir "23-notification-after-recovery.log") -Content $kafkaNotificationRecoveryLog

Save-CommandOutput -Path (Join-Path $kafkaDir "24-scale-notification.txt") -Script { docker compose -f $kafkaCompose up -d --scale notification-consumer=2 }
Save-CommandOutput -Path (Join-Path $kafkaDir "25-ps-scaled.txt") -Script { docker compose -f $kafkaCompose ps notification-consumer }
$kafkaElasticOrders = New-Object System.Collections.Generic.List[string]
for ($i = 1; $i -le 8; $i++) {
  $payload = New-OrderPayload -CustomerName ("Cliente Kafka " + $i) -Product ("Producto Kafka " + $i) -Quantity $i -UnitPrice (50000 + ($i * 1000))
  $elasticResponse = Invoke-JsonPost -Url "http://localhost:3000/orders" -Payload $payload -SavePath (Join-Path $kafkaDir ("26-elastic-response-" + $i + ".txt"))
  $kafkaElasticOrders.Add($elasticResponse.body.orderId)
}
Start-Sleep -Seconds 8
$kafkaNotificationElasticLog = Save-CommandOutput -Path (Join-Path $kafkaDir "27-notification-after-scale.log") -Script { docker compose -f $kafkaCompose logs --tail=200 notification-consumer }
Save-KafkaExecOutput -ComposeFile $kafkaCompose -Path (Join-Path $kafkaDir "28-consumer-group-after-scale.txt") -CommandLine "/opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notification-service-group"

$summary.kafka = [ordered]@{
  cp01 = [ordered]@{
    orderId = $kafkaCp01.body.orderId
    httpCode = $kafkaCp01.httpCode
    timeTotal = $kafkaCp01.timeTotal
    correlationId = Get-CorrelationIdFromLog -LogText $kafkaOrdersLogCp01 -OrderId $kafkaCp01.body.orderId
  }
  cp09 = [ordered]@{
    orderId = $kafkaCp09.body.orderId
    httpCode = $kafkaCp09.httpCode
    timeTotal = $kafkaCp09.timeTotal
    notificationProcessedWhileStopped = ($kafkaNotificationStoppedLog -match [regex]::Escape($kafkaCp09.body.orderId))
    notificationProcessedAfterRestart = ($kafkaNotificationRecoveryLog -match [regex]::Escape($kafkaCp09.body.orderId))
    correlationId = Get-CorrelationIdFromLog -LogText $kafkaOrdersLogCp09 -OrderId $kafkaCp09.body.orderId
  }
  cp10 = [ordered]@{
    scaledConsumers = 2
    orderIds = $kafkaElasticOrders
    consumer1Seen = ($kafkaNotificationElasticLog -match "notification-consumer-1")
    consumer2Seen = ($kafkaNotificationElasticLog -match "notification-consumer-2")
  }
}

Save-CommandOutput -Path (Join-Path $kafkaDir "99-down.txt") -Script { docker compose -f $kafkaCompose down --remove-orphans }
Write-Utf8File -Path (Join-Path $fullOutputRoot "summary.json") -Content (($summary | ConvertTo-Json -Depth 10))
