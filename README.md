# Hot Animals ETL System

A robust ETL (Extract, Transform, Load) system for processing animal data from HTTP APIs. This system extracts animal information, transforms it into the required format, and loads it to a destination endpoint with comprehensive error handling and retry logic.

## Prerequisites

- Python 3.8 or higher
- Docker (for running the test API server)
- pip (Python package installer)

## Installation

### 1. Install Docker Desktop

    Download and install Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop)

### 2. Download and Setup the Test API Server
    # Download the Docker image
    curl -O https://storage.googleapis.com/lp-dev-hiring/images/lp-programming-challenge-1-1625758668.tar.gz

    # Load the Docker image
    docker load -i lp-programming-challenge-1-1625758668.tar.gz

    # Run the API server (this will start on localhost:3123)
    docker run --rm -p 3123:3123 -ti lp-programming-challenge-1

    git clone https://github.com/Shreyanshi-Vashistha/hot-animals.git
    cd hot-animals
    mkdir logs

### 3. Setup Python Environment
    # Create virtual environment
    python -3.11 -m venv venv

    # Activate virtual environment
    # On Windows:
    .\venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate

    # Upgrade pip
    python -m pip install --upgrade pip

    # Install the package in development mode
    pip install -e .

### 3. Run the ETL process

    # with default settings
    animal-etl --log-level INFO

    # Custom configuration
    animal-etl --base-url http://localhost:3123 --batch-size 50 --log-level DEBUG

    # Dry run (extract and transform only, no loading)
    animal-etl --dry-run

    # Custom timeout and retry settings
    animal-etl --timeout 60 --max-retries 5

### 4. Logging

    Logs are written to both console and file (animal_etl.log):