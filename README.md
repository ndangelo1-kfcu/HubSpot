# HubSpot ETL Automation

This repository contains scripts to automate the extraction, transformation, and delivery of daily data files for further processing or archival. The main orchestration is handled by **HubSpotETL2.ps1**, which coordinates the execution of the ETL process using a Python script and configuration files. HubSpot ETL will run daily on vsarcu02 and drop a zip file of yesterday's EXTRACT.* files with EXTRACT.CARD masked here: \\kfcu\share\PR\MIS\ASD Area\HubSpot
 
Because extract completion time is sporadic, it will retry every 30 mins and at 8AM it will send a warning email that the files are not found but it wont fail, it will just keep retrying till after noon. If it hasnt found them by then, it will fail.

## Overview

- **HubSpotETL2.ps1**: PowerShell script that manages the ETL workflow, including scheduling, error handling, and notification.
- **HubSpotETL.py**: Python script that performs the actual extraction, transformation (including masking sensitive data), and delivery of files.
- **config.json**: Configuration file specifying the `ProcessDate` (either `"null"` for normal operation or a specific date in `YYYYMMDD` format).

## How It Works

1. **Scheduling & Execution**:  
   The PowerShell script runs in a loop, activating a Python virtual environment and executing the ETL Python script.

2. **Process Date Handling**:  
   The process date is read from `config.json`. If set to `"null"`, the script processes the most recent data; otherwise, it processes the specified date.

3. **File Processing**:  
   - Locates source files in a dated subfolder.
   - Copies files to a staging area.
   - Masks sensitive data in `EXTRACT.CARD` files.
   - Delivers processed files to a network folder.
   - Archives delivered files and cleans up staging.

4. **Error Handling & Alerts**:  
   - If required files are missing, the script retries every 15 minutes until 3 PM.
   - If files are still missing after 8 AM, an alert email is sent.
   - All major actions and errors are logged.
   - The script exits with appropriate codes for success, retry, or failure.

## Error Handling

- **Missing Files**: Retries until a cutoff time, with email alerts if persistent.
- **Script Failures**: Logs errors and exits with a non-zero code.
- **Configuration Issues**: Exits if `config.json` is missing or invalid.

## Requirements

- Windows environment with PowerShell
- Python and virtual environment set up in the project directory
- Network access to source and destination folders

## Configuration

Edit `config.json` to set the process date:
```json
{
  "ProcessDate": "null"
}
```
Or specify a date:
```json
{
  "ProcessDate": "20250619"
}
```

## Notifications

Alert emails are sent if the ETL process fails to find required files after 8 AM.

---


 
