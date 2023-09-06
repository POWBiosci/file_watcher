# Create virtual environment
python3 -m venv .venv

# Activate
.venv/bin/Activate.ps1

# Install dependencies
python3 -m pip install -r requirements.txt

# Run file
python3 file_watcher.py