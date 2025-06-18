$fileLocation = "\KFCU_SSIS\Live\HubSpot"
$fileName = "HubSpotETL.py"
# Navigate to the folder containing the virtual environment and Python script
Set-Location -Path ("\\vsarcu02\c$\" + $fileLocation)

# Activate the virtual environment
& ("\\vsarcu02\c$\" + $fileLocation + "\venv\Scripts\Activate.ps1")

# Run the Python script
$pythonScriptPath = ("\\vsarcu02\c$\" + $fileLocation + "\" + $fileName)
if (-Not (Test-Path $pythonScriptPath)) {
    Write-Output "Python script not found at $pythonScriptPath"
    exit 1  # Exit with a non-zero code to indicate failure
}
$process = Start-Process -FilePath ("\\vsarcu02\c$\"+ $fileLocation + "\venv\Scripts\python.exe") -ArgumentList $pythonScriptPath -NoNewWindow -PassThru -Wait

# Check the exit code of the Python script
if ($process.ExitCode -ne 0) {
    Write-Output "Python script failed with exit code $($process.ExitCode)"
    # Deactivate the virtual environment
    & "deactivate"
    exit 1  # Exit with a non-zero code to indicate failure
} else {
    Write-Output "Python script completed successfully"
    # Deactivate the virtual environment
    & "deactivate"
    exit 0  # Exit with zero code to indicate success
}