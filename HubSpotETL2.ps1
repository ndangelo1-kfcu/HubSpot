$fileLocation = "\KFCU_SSIS\Live\HubSpot"
$fileName = "HubSpotETL.py"
$startTime = Get-Date
$maxRetryTime = $startTime.AddHours(2.5)
$endTime = Get-Date -Hour 15 -Minute 0 -Second 0
$emailSent = $false

# Navigate to the folder containing the virtual environment and Python script
Set-Location -Path ("\\vsarcu02\c$\" + $fileLocation)

$sender1 = "svcui@kfcu.org"
$recipient = "ndangelo@kfcu.org"

function Send-AlertEmail1 { #devprod@kfcu.org
    Send-MailMessage -To $recipient -From $sender1 `
        -Subject "HubSpotETL.py Alert" `
        -Body "HubSpotETL.py has failed to find the daily extract files after 2.5 hours. It will continue to check until 3PM today." `
        -SmtpServer "mx.kfcu.org"
}
function Send-AlertEmail2 { #devprod@kfcu.org
    Send-MailMessage -To $recipient -From $sender1 `
        -Subject "HubSpotETL.py Alert" `
        -Body "HubSpotETL.py has failed to find the daily extract files for the day." `
        -SmtpServer "mx.kfcu.org"
}

while ($true) {
    # Activate the virtual environment
    & ("\\vsarcu02\c$\" + $fileLocation + "\venv\Scripts\Activate.ps1")

    # Run the Python script
    $pythonScriptPath = ("\\vsarcu02\c$\" + $fileLocation + "\" + $fileName)
    if (-Not (Test-Path $pythonScriptPath)) {
        Write-Output "Python script not found at $pythonScriptPath"
        exit 1
    }

    $process = Start-Process -FilePath ("\\vsarcu02\c$\" + $fileLocation + "\venv\Scripts\python.exe") `
        -ArgumentList $pythonScriptPath -NoNewWindow -PassThru -Wait

    # Check the exit code of the Python script
    if ($process.ExitCode -eq 0) {
        Write-Output "Python script completed successfully"
        break
    } elseif ($process.ExitCode -eq 2) {
        $now = Get-Date
        if ($now -lt $maxRetryTime) {
            Write-Output "Files not found. Retrying in 15 minutes..."
        } elseif (-not $emailSent) {
            Write-Output "Still failing after 2.5 hours. Sending alert email..."
            Send-AlertEmail1
            $emailSent = $true
        }

        if ($now -lt $endTime) {
            Start-Sleep -Seconds 900  # Wait 15 minutes
        } else {
            Write-Output "Reached 3 PM. Stopping retries."
            Send-AlertEmail2
            break
        }
    } else {
        Write-Output "Python script failed with unexpected exit code: $($process.ExitCode)"
        exit 1
    }
}
