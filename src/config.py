import os
from typing import List

# API Configuration
BASE_URL = os.getenv("API_BASE_URL", "http://localhost:3123")
ANIMALS_LIST_ENDPOINT = "/animals/v1/animals"
ANIMAL_DETAIL_ENDPOINT = "/animals/v1/animals/{id}"
HOME_ENDPOINT = "/animals/v1/home"

# Processing Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
TIMEOUT = int(os.getenv("TIMEOUT", "30"))
INITIAL_RETRY_DELAY = float(os.getenv("INITIAL_RETRY_DELAY", "1.0"))
MAX_RETRY_DELAY = float(os.getenv("MAX_RETRY_DELAY", "60.0"))

# HTTP Status Codes to Retry
RETRY_STATUS_CODES: List[int] = [500, 502, 503, 504]

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Pagination Configuration
DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", "20"))
START_PAGE = int(os.getenv("START_PAGE", "1"))