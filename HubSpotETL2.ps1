$fileLocation = "\KFCU_SSIS\Live\HubSpot"
$fileName = "HubSpotETL.py"
$eightAM = Get-Date -Hour 8 -Minute 0 -Second 0
$endTime = Get-Date -Hour 15 -Minute 0 -Second 0
$emailSent = $false

# Get ProcessDate from config.json
$configPath = "\\vsarcu02\c$\KFCU_SSIS\Live\HubSpot\config.json"
if (Test-Path $configPath) {
    $config = Get-Content $configPath | ConvertFrom-Json
    $ProcessDate = $config.ProcessDate
    Write-Output "ProcessDate from config.json: $ProcessDate"
} else {
    Write-Output "config.json not found at $configPath"
    exit 1
}

# Navigate to the folder containing the virtual environment and Python script
Set-Location -Path ("\\vsarcu02\c$\" + $fileLocation)

function Send-Email {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Subject,
        [Parameter(Mandatory=$true)]
        [string]$Body
    )

    $smtpServer = "mx.kfcu.org"
    $smtpFrom = "svcui@kfcu.org"
    $smtpTo = "ndangelo@kfcu.org"

    Send-MailMessage -From $smtpFrom -To $smtpTo -Subject $Subject -Body $Body -SmtpServer $smtpServer
}

# Example usage:
# Send-Email -Subject "Test Subject" -Body "This is the body of the email."

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
    } elseif ($process.ExitCode -eq 2 -and $ProcessDate -eq "null") {
        $now = Get-Date
        if ($now -lt $eightAM ) {
            Write-Output "Files not found. Retrying in 15 minutes..."
        } elseif (-not $emailSent) {
            Write-Output "Still failing after 8 AM. Sending alert email..."
            Send-Email `
                -Subject "HubSpotETL.py Alert" `
                -Body "HubSpotETL.py has failed to find the daily extract files by 8AM. Process will retry every 15 minutes until 3PM today."
            $emailSent = $true
        }

        if ($now -lt $endTime) {
            Start-Sleep -Seconds 900  # Wait 15 minutes
        } else {
            Write-Output "Reached 3 PM. Stopping retries, reporting failure."
            exit 2
        }
    
    } else {
        Write-Output "Python script failed with unexpected exit code: $($process.ExitCode)"
        exit 1
    }
}
