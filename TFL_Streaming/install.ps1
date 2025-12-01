# install.ps1
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Create virtual environment
python -m venv venv

# Activate venv
& .\venv\Scripts\Activate.ps1

# Airflow variables
$AIRFLOW_VERSION = "3.1.0"
$PYTHON_VERSION = "3.13"
$CONSTRAINT_URL = "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"

# Install Airflow
pip install "apache-airflow==$AIRFLOW_VERSION" --constraint $CONSTRAINT_URL

# Install remaining dependencies
pip install -r requirements.txt
