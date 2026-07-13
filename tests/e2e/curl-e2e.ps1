#requires -Version 5.1
[CmdletBinding()]
param(
    [string]$IngestionBaseUrl = "",
    [string]$ProcessingBaseUrl = "",
    [int]$TimeoutSeconds = 90,
    [switch]$Compose
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-EnvOrDefault {
    param(
        [string]$Name,
        [string]$DefaultValue
    )

    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $DefaultValue
    }

    return $value
}

function Resolve-Curl {
    $curlExe = Get-Command curl.exe -ErrorAction SilentlyContinue
    if ($null -ne $curlExe) {
        return $curlExe.Source
    }

    $curlApp = Get-Command curl -CommandType Application -ErrorAction SilentlyContinue
    if ($null -ne $curlApp) {
        return $curlApp.Source
    }

    throw "curl executable was not found"
}

function Join-Url {
    param(
        [string]$BaseUrl,
        [string]$Path
    )

    return ($BaseUrl.TrimEnd("/") + "/" + $Path.TrimStart("/"))
}

function New-Json {
    param([object]$Value)

    return ($Value | ConvertTo-Json -Depth 10 -Compress)
}

function New-TelemetryJson {
    param(
        [string]$DeviceID,
        [string]$PatientID,
        [string]$BatchID,
        [int[]]$HeartRates,
        [DateTime]$BaseTime
    )

    $measurements = @()
    for ($i = 0; $i -lt $HeartRates.Count; $i++) {
        $measurements += [ordered]@{
            timestamp = $BaseTime.AddSeconds($i).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            heart_rate = [int]$HeartRates[$i]
        }
    }

    return New-Json ([ordered]@{
        device_id = $DeviceID
        patient_id = $PatientID
        batch_id = $BatchID
        measurements = $measurements
    })
}

function New-Measurement {
    param(
        [string]$Timestamp,
        [int]$HeartRate
    )

    return [ordered]@{
        timestamp = $Timestamp
        heart_rate = $HeartRate
    }
}

function Invoke-CurlJson {
    param(
        [ValidateSet("GET", "POST")]
        [string]$Method,

        [string]$Url,

        [AllowEmptyString()]
        [string]$Body = $null,

        [switch]$NoContentType,

        [int]$MaxTimeSeconds = 10
    )

    $bodyPath = [System.IO.Path]::GetTempFileName()
    $headersPath = [System.IO.Path]::GetTempFileName()
    $requestBodyPath = $null

    try {
        $curlArgs = @(
            "-sS",
            "-X", $Method,
            "-D", $headersPath,
            "-o", $bodyPath,
            "-w", "%{http_code}",
            "--connect-timeout", "3",
            "--max-time", [string]$MaxTimeSeconds,
            $Url
        )

        if ($PSBoundParameters.ContainsKey("Body")) {
            if (-not $NoContentType) {
                $curlArgs += @("-H", "Content-Type: application/json")
            }
            $requestBodyPath = [System.IO.Path]::GetTempFileName()
            $utf8NoBom = New-Object System.Text.UTF8Encoding $false
            [System.IO.File]::WriteAllText($requestBodyPath, $Body, $utf8NoBom)
            $curlArgs += @("--data-binary", "@$requestBodyPath")
        }

        $statusOutput = & $script:CurlCommand @curlArgs
        $exitCode = $LASTEXITCODE
        $statusRaw = (($statusOutput | Out-String).Trim())
        $bodyText = ""
        $headersText = ""

        if (Test-Path -LiteralPath $bodyPath) {
            $bodyText = Get-Content -LiteralPath $bodyPath -Raw
        }
        if (Test-Path -LiteralPath $headersPath) {
            $headersText = Get-Content -LiteralPath $headersPath -Raw
        }

        if ($exitCode -ne 0) {
            throw "curl failed with exit code $exitCode for $Method $Url"
        }
        if ($statusRaw -notmatch "^\d{3}$") {
            throw "curl returned an invalid HTTP status '$statusRaw' for $Method $Url"
        }

        $json = $null
        if (-not [string]::IsNullOrWhiteSpace($bodyText)) {
            try {
                $json = $bodyText | ConvertFrom-Json
            } catch {
                $json = $null
            }
        }

        return [pscustomobject]@{
            Method = $Method
            Url = $Url
            Status = [int]$statusRaw
            Headers = $headersText
            Body = $bodyText.Trim()
            Json = $json
        }
    } finally {
        Remove-Item -LiteralPath $bodyPath -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $headersPath -Force -ErrorAction SilentlyContinue
        if ($null -ne $requestBodyPath) {
            Remove-Item -LiteralPath $requestBodyPath -Force -ErrorAction SilentlyContinue
        }
    }
}

function Get-ResponseHeader {
    param(
        [object]$Response,
        [string]$Name
    )

    $value = $null
    foreach ($line in ($Response.Headers -split "`r?`n")) {
        if ($line -match "^\s*([^:]+):\s*(.*)$") {
            if ($matches[1].Equals($Name, [System.StringComparison]::OrdinalIgnoreCase)) {
                $value = $matches[2].Trim()
            }
        }
    }

    return $value
}

function Get-JsonProperty {
    param(
        [object]$Response,
        [string]$Name
    )

    if ($null -eq $Response.Json) {
        throw "Response body is not JSON for $($Response.Method) $($Response.Url): $($Response.Body)"
    }

    $property = $Response.Json.PSObject.Properties[$Name]
    if ($null -eq $property) {
        throw "Missing JSON property '$Name' in response: $($Response.Body)"
    }

    return $property.Value
}

function Get-ObjectProperty {
    param(
        [object]$Object,
        [string]$Name
    )

    if ($null -eq $Object) {
        return $null
    }

    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function As-Array {
    param([object]$Value)

    if ($null -eq $Value) {
        return @()
    }

    return @($Value)
}

function Assert-Status {
    param(
        [object]$Response,
        [int]$ExpectedStatus
    )

    if ($Response.Status -ne $ExpectedStatus) {
        throw "Expected HTTP $ExpectedStatus for $($Response.Method) $($Response.Url), got $($Response.Status). Body: $($Response.Body)"
    }
}

function Assert-StatusIn {
    param(
        [object]$Response,
        [int[]]$ExpectedStatuses
    )

    if ($ExpectedStatuses -notcontains $Response.Status) {
        throw "Expected one of HTTP [$($ExpectedStatuses -join ', ')] for $($Response.Method) $($Response.Url), got $($Response.Status). Body: $($Response.Body)"
    }
}

function Assert-NotSuccess {
    param([object]$Response)

    if ($Response.Status -ge 200 -and $Response.Status -lt 300) {
        throw "Expected non-success status for $($Response.Method) $($Response.Url), got $($Response.Status). Body: $($Response.Body)"
    }
}

function Assert-JsonEquals {
    param(
        [object]$Response,
        [string]$Name,
        [object]$ExpectedValue
    )

    $actualValue = Get-JsonProperty -Response $Response -Name $Name
    if ($actualValue -ne $ExpectedValue) {
        throw "Expected JSON '$Name' to be '$ExpectedValue', got '$actualValue'. Body: $($Response.Body)"
    }
}

function Assert-JsonError {
    param(
        [object]$Response,
        [int]$ExpectedStatus,
        [string]$ExpectedError,
        [string]$ExpectedMessage = ""
    )

    Assert-Status -Response $Response -ExpectedStatus $ExpectedStatus
    Assert-JsonEquals -Response $Response -Name "error" -ExpectedValue $ExpectedError

    if ($ExpectedMessage -ne "") {
        Assert-JsonEquals -Response $Response -Name "message" -ExpectedValue $ExpectedMessage
    }
}

function Assert-JsonContentType {
    param([object]$Response)

    $contentType = Get-ResponseHeader -Response $Response -Name "Content-Type"
    if ([string]::IsNullOrWhiteSpace($contentType) -or $contentType -notlike "*application/json*") {
        throw "Expected JSON content type, got '$contentType'"
    }
}

function Wait-Until {
    param(
        [string]$Name,
        [scriptblock]$Probe,
        [int]$Seconds = $script:TimeoutSeconds
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    $lastError = ""

    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $result = & $Probe
            if ($result) {
                return $result
            }
        } catch {
            $lastError = $_.Exception.Message
        }

        Start-Sleep -Milliseconds 500
    }

    if ($lastError -eq "") {
        $lastError = "probe did not pass"
    }
    throw "Timed out waiting for $Name after ${Seconds}s. Last error: $lastError"
}

function Run-Test {
    param(
        [string]$Name,
        [scriptblock]$Script
    )

    Write-Host "[TEST] $Name"
    try {
        & $Script
        $script:Passed++
        Write-Host "[PASS] $Name" -ForegroundColor Green
    } catch {
        $script:Failed++
        Write-Host "[FAIL] $Name" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        exit 1
    }
}

function Assert-AcceptedTelemetry {
    param(
        [object]$Response,
        [int]$ExpectedMeasurements
    )

    Assert-Status -Response $Response -ExpectedStatus 202
    Assert-JsonContentType -Response $Response
    Assert-JsonEquals -Response $Response -Name "status" -ExpectedValue "accepted"
    Assert-JsonEquals -Response $Response -Name "accepted_measurements" -ExpectedValue $ExpectedMeasurements
}

if ([string]::IsNullOrWhiteSpace($IngestionBaseUrl)) {
    $IngestionBaseUrl = Get-EnvOrDefault -Name "INGESTION_BASE_URL" -DefaultValue "http://localhost:8080"
}
if ([string]::IsNullOrWhiteSpace($ProcessingBaseUrl)) {
    $ProcessingBaseUrl = Get-EnvOrDefault -Name "PROCESSING_BASE_URL" -DefaultValue "http://localhost:8081"
}

$script:CurlCommand = Resolve-Curl
$script:TimeoutSeconds = $TimeoutSeconds
$script:Passed = 0
$script:Failed = 0
$script:RunID = "{0}-{1}" -f ([DateTime]::UtcNow.ToString("yyyyMMddHHmmss")), (Get-Random -Minimum 1000 -Maximum 9999)
$script:BaseTime = [DateTime]::UtcNow.Date.AddHours(12)
$telemetryUrl = Join-Url -BaseUrl $IngestionBaseUrl -Path "/api/v1/telemetry"
$ingestionHealthUrl = Join-Url -BaseUrl $IngestionBaseUrl -Path "/health"
$processingHealthUrl = Join-Url -BaseUrl $ProcessingBaseUrl -Path "/health"
$processingMetricsUrl = Join-Url -BaseUrl $ProcessingBaseUrl -Path "/metrics"
$alertsUrl = Join-Url -BaseUrl $ProcessingBaseUrl -Path "/alerts"

if ($Compose) {
    $docker = Get-Command docker -CommandType Application -ErrorAction SilentlyContinue
    if ($null -eq $docker) {
        throw "docker executable was not found"
    }

    Write-Host "[SETUP] docker compose up -d --build"
    & docker compose up -d --build
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose up failed"
    }
}

Write-Host "[INFO] run_id=$script:RunID"
Write-Host "[INFO] ingestion=$IngestionBaseUrl"
Write-Host "[INFO] processing=$ProcessingBaseUrl"
Write-Host "[INFO] curl=$script:CurlCommand"

Run-Test "ingestion health is ok" {
    Wait-Until -Name "ingestion /health" -Probe {
        $response = Invoke-CurlJson -Method GET -Url $ingestionHealthUrl
        Assert-Status -Response $response -ExpectedStatus 200
        Assert-JsonContentType -Response $response
        Assert-JsonEquals -Response $response -Name "status" -ExpectedValue "ok"
        return $true
    } | Out-Null
}

Run-Test "processing health is ok" {
    Wait-Until -Name "processing /health" -Probe {
        $response = Invoke-CurlJson -Method GET -Url $processingHealthUrl
        Assert-Status -Response $response -ExpectedStatus 200
        Assert-JsonContentType -Response $response
        Assert-JsonEquals -Response $response -Name "status" -ExpectedValue "ok"
        Assert-JsonEquals -Response $response -Name "service" -ExpectedValue "processing"
        return $true
    } | Out-Null
}

Run-Test "processing exposes prometheus metrics" {
    $response = Invoke-CurlJson -Method GET -Url $processingMetricsUrl

    Assert-Status -Response $response -ExpectedStatus 200
    $contentType = Get-ResponseHeader -Response $response -Name "Content-Type"
    if ([string]::IsNullOrWhiteSpace($contentType) -or $contentType -notlike "*text/plain*") {
        throw "Expected Prometheus text content type, got '$contentType'"
    }
    if ($response.Body -notlike "*processing_kafka_messages_total*") {
        throw "Expected processing_kafka_messages_total in /metrics body. Body: $($response.Body)"
    }
}

Run-Test "kafka publishing path is ready" {
    $script:WarmupAttempt = 0
    Wait-Until -Name "ingestion publisher" -Probe {
        $script:WarmupAttempt++
        $body = New-TelemetryJson `
            -DeviceID "e2e-warmup-device-$script:RunID-$script:WarmupAttempt" `
            -PatientID "e2e-warmup-patient-$script:RunID" `
            -BatchID "e2e-warmup-batch-$script:RunID-$script:WarmupAttempt" `
            -HeartRates @(75) `
            -BaseTime $script:BaseTime.AddMinutes(1)

        $response = Invoke-CurlJson -Method POST -Url $telemetryUrl -Body $body
        if ($response.Status -eq 202) {
            return $true
        }
        if ($response.Status -eq 503) {
            return $false
        }

        throw "Unexpected publisher warmup status $($response.Status). Body: $($response.Body)"
    } | Out-Null
}

Run-Test "processing returns empty array for a patient without alerts" {
    $emptyPatientUrl = Join-Url -BaseUrl $ProcessingBaseUrl -Path "/alerts/e2e-empty-$script:RunID"
    $response = Invoke-CurlJson -Method GET -Url $emptyPatientUrl

    Assert-Status -Response $response -ExpectedStatus 200
    Assert-JsonContentType -Response $response
    $alerts = @(As-Array -Value $response.Json)
    if ($alerts.Count -ne 0) {
        throw "Expected no alerts for empty patient, got $($response.Body)"
    }
}

Run-Test "processing exposes alerts collection endpoint" {
    $response = Invoke-CurlJson -Method GET -Url $alertsUrl

    Assert-Status -Response $response -ExpectedStatus 200
    Assert-JsonContentType -Response $response
}

Run-Test "processing does not accept telemetry HTTP API" {
    $response = Invoke-CurlJson `
        -Method POST `
        -Url (Join-Url -BaseUrl $ProcessingBaseUrl -Path "/api/v1/telemetry") `
        -Body "{}"

    Assert-NotSuccess -Response $response
}

Run-Test "ingestion does not expose alerts endpoint" {
    $response = Invoke-CurlJson -Method GET -Url (Join-Url -BaseUrl $IngestionBaseUrl -Path "/alerts")

    Assert-NotSuccess -Response $response
}

$validTimestamp = $script:BaseTime.AddMinutes(2).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$nextTimestamp = $script:BaseTime.AddMinutes(2).AddSeconds(1).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$validMeasurement = New-Measurement -Timestamp $validTimestamp -HeartRate 80

$validationCases = @(
    [pscustomobject]@{
        Name = "empty body"
        Body = ""
        Message = "invalid JSON body"
    },
    [pscustomobject]@{
        Name = "malformed JSON"
        Body = '{"device_id":'
        Message = "invalid JSON body"
    },
    [pscustomobject]@{
        Name = "unknown top-level field"
        Body = New-Json ([ordered]@{
            device_id = "e2e-invalid-device-$script:RunID"
            patient_id = "e2e-invalid-patient-$script:RunID"
            batch_id = "e2e-invalid-batch-$script:RunID-unknown"
            unexpected = "field"
            measurements = @($validMeasurement)
        })
        Message = "invalid JSON body"
    },
    [pscustomobject]@{
        Name = "trailing JSON value"
        Body = (New-TelemetryJson `
            -DeviceID "e2e-invalid-device-$script:RunID" `
            -PatientID "e2e-invalid-patient-$script:RunID" `
            -BatchID "e2e-invalid-batch-$script:RunID-trailing" `
            -HeartRates @(80) `
            -BaseTime $script:BaseTime.AddMinutes(2)) + "{}"
        Message = "invalid JSON body"
    },
    [pscustomobject]@{
        Name = "missing device_id"
        Body = New-Json ([ordered]@{
            patient_id = "e2e-invalid-patient-$script:RunID"
            batch_id = "e2e-invalid-batch-$script:RunID-no-device"
            measurements = @($validMeasurement)
        })
        Message = "device_id is required"
    },
    [pscustomobject]@{
        Name = "missing patient_id"
        Body = New-Json ([ordered]@{
            device_id = "e2e-invalid-device-$script:RunID"
            batch_id = "e2e-invalid-batch-$script:RunID-no-patient"
            measurements = @($validMeasurement)
        })
        Message = "patient_id is required"
    },
    [pscustomobject]@{
        Name = "missing batch_id"
        Body = New-Json ([ordered]@{
            device_id = "e2e-invalid-device-$script:RunID"
            patient_id = "e2e-invalid-patient-$script:RunID"
            measurements = @($validMeasurement)
        })
        Message = "batch_id is required"
    },
    [pscustomobject]@{
        Name = "empty measurements"
        Body = New-Json ([ordered]@{
            device_id = "e2e-invalid-device-$script:RunID"
            patient_id = "e2e-invalid-patient-$script:RunID"
            batch_id = "e2e-invalid-batch-$script:RunID-empty-measurements"
            measurements = @()
        })
        Message = "measurements length must be between 1 and 10"
    },
    [pscustomobject]@{
        Name = "more than 10 measurements"
        Body = New-TelemetryJson `
            -DeviceID "e2e-invalid-device-$script:RunID" `
            -PatientID "e2e-invalid-patient-$script:RunID" `
            -BatchID "e2e-invalid-batch-$script:RunID-too-many" `
            -HeartRates @(80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90) `
            -BaseTime $script:BaseTime.AddMinutes(3)
        Message = "measurements length must be between 1 and 10"
    },
    [pscustomobject]@{
        Name = "missing measurement timestamp"
        Body = New-Json ([ordered]@{
            device_id = "e2e-invalid-device-$script:RunID"
            patient_id = "e2e-invalid-patient-$script:RunID"
            batch_id = "e2e-invalid-batch-$script:RunID-no-ts"
            measurements = @([ordered]@{ heart_rate = 80 })
        })
        Message = "measurement timestamp is required"
    },
    [pscustomobject]@{
        Name = "heart_rate below lower bound"
        Body = New-Json ([ordered]@{
            device_id = "e2e-invalid-device-$script:RunID"
            patient_id = "e2e-invalid-patient-$script:RunID"
            batch_id = "e2e-invalid-batch-$script:RunID-low-hr"
            measurements = @(New-Measurement -Timestamp $validTimestamp -HeartRate 19)
        })
        Message = "heart_rate must be greater than or equal to 20"
    },
    [pscustomobject]@{
        Name = "heart_rate above upper bound"
        Body = New-Json ([ordered]@{
            device_id = "e2e-invalid-device-$script:RunID"
            patient_id = "e2e-invalid-patient-$script:RunID"
            batch_id = "e2e-invalid-batch-$script:RunID-high-hr"
            measurements = @(New-Measurement -Timestamp $validTimestamp -HeartRate 251)
        })
        Message = "heart_rate must be less than or equal to 250"
    },
    [pscustomobject]@{
        Name = "timestamps out of order"
        Body = New-Json ([ordered]@{
            device_id = "e2e-invalid-device-$script:RunID"
            patient_id = "e2e-invalid-patient-$script:RunID"
            batch_id = "e2e-invalid-batch-$script:RunID-time-order"
            measurements = @(
                New-Measurement -Timestamp $nextTimestamp -HeartRate 80
                New-Measurement -Timestamp $validTimestamp -HeartRate 81
            )
        })
        Message = "measurement timestamps must be strictly increasing"
    }
)

foreach ($case in $validationCases) {
    $current = $case
    Run-Test "ingestion rejects $($current.Name)" {
        $response = Invoke-CurlJson -Method POST -Url $telemetryUrl -Body $current.Body

        Assert-JsonError `
            -Response $response `
            -ExpectedStatus 400 `
            -ExpectedError "invalid_batch" `
            -ExpectedMessage $current.Message
        Assert-JsonContentType -Response $response
    }
}

$acceptedDevice = "e2e-device-$script:RunID-accepted"
$acceptedPatient = "e2e-patient-$script:RunID-accepted"
$acceptedBatch = "e2e-batch-$script:RunID-accepted-001"
$acceptedBody = New-TelemetryJson `
    -DeviceID $acceptedDevice `
    -PatientID $acceptedPatient `
    -BatchID $acceptedBatch `
    -HeartRates @(70, 71, 72, 73, 74, 75, 76, 77, 78, 79) `
    -BaseTime $script:BaseTime.AddMinutes(5)

Run-Test "ingestion accepts a maximum-size batch" {
    $response = Invoke-CurlJson -Method POST -Url $telemetryUrl -Body $acceptedBody -MaxTimeSeconds 70

    Assert-AcceptedTelemetry -Response $response -ExpectedMeasurements 10
}

Run-Test "ingestion ignores duplicate batch before rate limiting" {
    $response = Invoke-CurlJson -Method POST -Url $telemetryUrl -Body $acceptedBody

    Assert-Status -Response $response -ExpectedStatus 200
    Assert-JsonContentType -Response $response
    Assert-JsonEquals -Response $response -Name "status" -ExpectedValue "duplicate_ignored"
}

Run-Test "ingestion rate limits a new batch from the same device" {
    $rateDevice = "e2e-device-$script:RunID-rate"
    $ratePatient = "e2e-patient-$script:RunID-rate"
    $firstBody = New-TelemetryJson `
        -DeviceID $rateDevice `
        -PatientID $ratePatient `
        -BatchID "e2e-batch-$script:RunID-rate-001" `
        -HeartRates @(81) `
        -BaseTime $script:BaseTime.AddMinutes(6)
    $rateLimitedBody = New-TelemetryJson `
        -DeviceID $rateDevice `
        -PatientID $ratePatient `
        -BatchID "e2e-batch-$script:RunID-rate-limited-002" `
        -HeartRates @(82) `
        -BaseTime $script:BaseTime.AddMinutes(6).AddSeconds(1)

    $firstResponse = Invoke-CurlJson -Method POST -Url $telemetryUrl -Body $firstBody -MaxTimeSeconds 20
    Assert-AcceptedTelemetry -Response $firstResponse -ExpectedMeasurements 1

    $response = Invoke-CurlJson -Method POST -Url $telemetryUrl -Body $rateLimitedBody

    Assert-JsonError `
        -Response $response `
        -ExpectedStatus 429 `
        -ExpectedError "rate_limit_exceeded" `
        -ExpectedMessage "device rate limit exceeded"
    Assert-JsonContentType -Response $response

    $retryAfter = Get-ResponseHeader -Response $response -Name "Retry-After"
    if ([string]::IsNullOrWhiteSpace($retryAfter) -or ([int]$retryAfter) -lt 1) {
        throw "Expected Retry-After header with a positive integer value, got '$retryAfter'"
    }
}

$alertDevice = "e2e-device-$script:RunID-alert"
$alertPatient = "e2e-patient-$script:RunID-alert"
$alertStartTimestamp = $script:BaseTime.AddMinutes(10).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$alertTimestamp = $script:BaseTime.AddMinutes(11).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$alertBody = New-Json ([ordered]@{
    device_id = $alertDevice
    patient_id = $alertPatient
    batch_id = "e2e-batch-$script:RunID-alert-001"
    measurements = @(
        New-Measurement -Timestamp $alertStartTimestamp -HeartRate 130
        New-Measurement -Timestamp $alertTimestamp -HeartRate 130
    )
})
$patientAlertsUrl = Join-Url -BaseUrl $ProcessingBaseUrl -Path "/alerts/$alertPatient"

Run-Test "high heart rate flows from ingestion to processing alert" {
    $response = Invoke-CurlJson -Method POST -Url $telemetryUrl -Body $alertBody -MaxTimeSeconds 20

    Assert-AcceptedTelemetry -Response $response -ExpectedMeasurements 2

    $alert = Wait-Until -Name "alert for $alertPatient" -Probe {
        $alertsResponse = Invoke-CurlJson -Method GET -Url $patientAlertsUrl
        Assert-Status -Response $alertsResponse -ExpectedStatus 200
        Assert-JsonContentType -Response $alertsResponse

        $alerts = @(As-Array -Value $alertsResponse.Json)
        foreach ($item in $alerts) {
            if ((Get-ObjectProperty -Object $item -Name "patient_id") -eq $alertPatient) {
                if ((Get-ObjectProperty -Object $item -Name "type") -eq "HIGH_HEART_RATE") {
                    return $item
                }
            }
        }

        return $null
    }

    if ((Get-ObjectProperty -Object $alert -Name "message") -ne "Patient has high heart rate") {
        throw "Unexpected alert message: $($alert | ConvertTo-Json -Compress)"
    }
    if ((Get-ObjectProperty -Object $alert -Name "triggered_at") -ne $alertTimestamp) {
        throw "Unexpected alert timestamp: $($alert | ConvertTo-Json -Compress)"
    }
}

Run-Test "global alerts endpoint includes created alert" {
    $response = Invoke-CurlJson -Method GET -Url $alertsUrl

    Assert-Status -Response $response -ExpectedStatus 200
    Assert-JsonContentType -Response $response

    $alerts = @(As-Array -Value $response.Json)
    $found = $false
    foreach ($item in $alerts) {
        if ((Get-ObjectProperty -Object $item -Name "patient_id") -eq $alertPatient) {
            if ((Get-ObjectProperty -Object $item -Name "type") -eq "HIGH_HEART_RATE") {
                $found = $true
            }
        }
    }

    if (-not $found) {
        throw "Expected /alerts to include alert for $alertPatient. Body: $($response.Body)"
    }
}

Run-Test "GET telemetry endpoint is not a success route" {
    $response = Invoke-CurlJson -Method GET -Url $telemetryUrl

    Assert-StatusIn -Response $response -ExpectedStatuses @(404, 405)
}

Write-Host ""
Write-Host "[OK] E2E curl tests passed: $script:Passed" -ForegroundColor Green
