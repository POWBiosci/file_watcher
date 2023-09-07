# Initialize log function for tracking
Function Write-Log {
    Param (
        [string]$Message
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "$timestamp - $Message"
}

# Virtual environment setup function
Function Setup-VirtualEnv {
    [CmdletBinding()]
    Param (
        [string]$venvPath = ".venv"
    )
    
    Write-Log "Creating virtual environment..."
    
    if (Test-Path $venvPath) {
        Write-Log "$venvPath already exists. Skipping creation."
    } else {
        python3 -m venv $venvPath
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Failed to create virtual environment. Exiting."
            Exit 1
        }
    }
    
    Write-Log "Activating virtual environment..."
    . $venvPath/bin/Activate.ps1
}

# Function to install Python dependencies
Function Install-Dependencies {
    [CmdletBinding()]
    Param (
        [string]$requirementsFile = "requirements.txt"
    )
    
    Write-Log "Installing dependencies from $requirementsFile..."
    
    if (Test-Path $requirementsFile) {
        python3 -m pip install -r $requirementsFile
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Failed to install dependencies. Exiting."
            Exit 1
        }
    } else {
        Write-Log "$requirementsFile does not exist. Skipping."
    }
}

# Main function to run file watcher
Function Run-FileWatcher {
    Write-Log "Running file_watcher.py..."
    python3 file_watcher.py
    if ($LASTEXITCODE -ne 0) {
        Write-Log "Failed to run file_watcher.py. Exiting."
        Exit 1
    }
}

# Main Execution
try {
    Write-Log "------ Starting Setup ------"
    
    # Setup virtual environment
    Setup-VirtualEnv
    
    # Install dependencies
    Install-Dependencies
    
    # Run file watcher
    Run-FileWatcher
    
    Write-Log "------ Setup Complete ------"
}
catch {
    Write-Log "An unexpected error occurred: $_. Exiting."
    Exit 1
}
