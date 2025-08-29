import logging
import time
from typing import Dict, List, Optional, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import config
from models import AnimalSummary, AnimalDetail, PaginatedResponse, TransformedAnimal, APIError

logger = logging.getLogger(__name__)


class AnimalAPIClient:
    """
    HTTP client for the Animal API with robust retry logic and error handling.
    """
    
    def __init__(self, base_url: str = config.BASE_URL, timeout: int = config.TIMEOUT):
        """
        Initialize the API client.
        
        Args:
            base_url: Base URL for the API
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with connection pooling and basic retry strategy."""
        session = requests.Session()
        
        # Configure connection pooling and basic retries for connection errors
        retry_strategy = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.3,
            status_forcelist=[],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Animal-ETL/1.0'
        })
        
        return session
    
    @retry(
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(
            multiplier=config.INITIAL_RETRY_DELAY,
            max=config.MAX_RETRY_DELAY
        ),
        retry=retry_if_exception_type((requests.RequestException, APIError)),
        reraise=True
    )
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Make an HTTP request with automatic retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
            
        Raises:
            APIError: For HTTP errors or API-specific issues
        """
        full_url = f"{self.base_url}{url}"
        
        try:
            logger.debug(f"Making {method} request to {full_url}")
            response = self.session.request(
                method=method,
                url=full_url,
                timeout=self.timeout,
                **kwargs
            )

            if response.status_code in config.RETRY_STATUS_CODES:
                logger.warning(f"Received retryable error {response.status_code} from {full_url}")
                raise APIError(
                    f"HTTP {response.status_code} error",
                    status_code=response.status_code,
                    response_text=response.text
                )

            response.raise_for_status()
            
            logger.debug(f"Successfully received response from {full_url}")
            return response
            
        except requests.Timeout:
            logger.warning(f"Request timeout for {full_url}")
            raise APIError(f"Request timeout for {full_url}")
        except requests.ConnectionError as e:
            logger.warning(f"Connection error for {full_url}: {str(e)}")
            raise APIError(f"Connection error: {str(e)}")
        except requests.HTTPError as e:
            if e.response.status_code not in config.RETRY_STATUS_CODES:
                logger.error(f"Non-retryable HTTP error {e.response.status_code} for {full_url}")
                raise APIError(
                    f"HTTP {e.response.status_code} error",
                    status_code=e.response.status_code,
                    response_text=e.response.text
                )
            raise 
        except requests.RequestException as e:
            logger.warning(f"Request error for {full_url}: {str(e)}")
            raise APIError(f"Request error: {str(e)}")
    
    def get_animals_page(self, page: int = 1, page_size: int = config.DEFAULT_PAGE_SIZE) -> PaginatedResponse:
        """
        Get a page of animals from the list endpoint.
        
        Args:
            page: Page number (1-based)
            page_size: Number of items per page
            
        Returns:
            PaginatedResponse with animals data
            
        Raises:
            APIError: If the request fails
        """
        params = {'page': page, 'per_page': page_size}
        
        try:
            response = self._make_request('GET', config.ANIMALS_LIST_ENDPOINT, params=params)
            data = response.json()
            
            # Validate and parse the response
            return PaginatedResponse(**data)
            
        except (ValueError, KeyError) as e:
            logger.error(f"Failed to parse animals list response: {str(e)}")
            raise APIError(f"Invalid response format: {str(e)}")
    
    def get_all_animals(self) -> List[AnimalSummary]:
        """
        Get all animals by paginating through the list endpoint.
        
        Returns:
            List of all AnimalSummary objects
            
        Raises:
            APIError: If any request fails
        """
        all_animals = []
        current_page = config.START_PAGE
        
        logger.info("Starting to fetch all animals...")
        
        while True:
            logger.debug(f"Fetching page {current_page}")
            page_response = self.get_animals_page(current_page)
            
            all_animals.extend(page_response.items)
            
            logger.info(f"Fetched page {current_page}: {len(page_response.items)} animals "
                       f"(total so far: {len(all_animals)})")

            if current_page >= page_response.total_pages:
                break
            
            current_page += 1
        
        logger.info(f"Finished fetching all animals: {len(all_animals)} total")
        return all_animals
    
    def get_animal_detail(self, animal_id: int) -> AnimalDetail:
        """
        Get detailed information for a specific animal.
        
        Args:
            animal_id: The animal's ID
            
        Returns:
            AnimalDetail object
            
        Raises:
            APIError: If the request fails
        """
        url = config.ANIMAL_DETAIL_ENDPOINT.format(id=animal_id)
        
        try:
            response = self._make_request('GET', url)
            data = response.json()
            
            # Validate and parse the response
            return AnimalDetail(**data)
            
        except (ValueError, KeyError) as e:
            logger.error(f"Failed to parse animal detail response for ID {animal_id}: {str(e)}")
            raise APIError(f"Invalid response format for animal {animal_id}: {str(e)}")
    
    def get_all_animal_details(self, animal_summaries: List[AnimalSummary]) -> List[AnimalDetail]:
        """
        Get detailed information for all animals.
        
        Args:
            animal_summaries: List of animal summaries from the list endpoint
            
        Returns:
            List of AnimalDetail objects
            
        Raises:
            APIError: If any request fails
        """
        animal_details = []
        total_animals = len(animal_summaries)
        
        logger.info(f"Starting to fetch details for {total_animals} animals...")
        
        for i, animal_summary in enumerate(animal_summaries, 1):
            logger.debug(f"Fetching details for animal {animal_summary.id} ({i}/{total_animals})")
            
            try:
                detail = self.get_animal_detail(animal_summary.id)
                animal_details.append(detail)
                
                if i % 10 == 0:
                    logger.info(f"Fetched details for {i}/{total_animals} animals")
                    
            except APIError as e:
                logger.error(f"Failed to fetch details for animal {animal_summary.id}: {str(e)}")
                raise
        
        logger.info(f"Finished fetching details for all {len(animal_details)} animals")
        return animal_details
    
    def submit_animals_batch(self, animals: List[TransformedAnimal]) -> bool:
        """
        Submit a batch of transformed animals to the home endpoint.
        
        Args:
            animals: List of transformed animals (max 100)
            
        Returns:
            True if submission was successful
            
        Raises:
            APIError: If the request fails
            ValueError: If batch size exceeds 100
        """
        if len(animals) > 100:
            raise ValueError(f"Batch size {len(animals)} exceeds maximum of 100")

        animals_data = [animal.dict() for animal in animals]
        
        try:
            logger.info(f"Submitting batch of {len(animals)} animals")
            response = self._make_request('POST', config.HOME_ENDPOINT, json=animals_data)
            
            logger.info(f"Successfully submitted batch of {len(animals)} animals")
            return True
            
        except APIError as e:
            logger.error(f"Failed to submit batch of {len(animals)} animals: {str(e)}")
            raise
    
    def close(self):
        """Close the HTTP session."""
        if self.session:
            self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()