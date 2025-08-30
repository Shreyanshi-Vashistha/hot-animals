import logging
import time
from typing import List, Optional
from dataclasses import dataclass

import config
from api_client import AnimalAPIClient
from transformers import transform_animals_batch
from models import AnimalSummary, AnimalDetail, TransformedAnimal, APIError, TransformationError

logger = logging.getLogger(__name__)


@dataclass
class ETLStats:
    """Statistics for ETL processing."""
    total_animals_found: int = 0
    total_animals_detailed: int = 0
    total_animals_transformed: int = 0
    total_animals_submitted: int = 0
    total_batches_submitted: int = 0
    failed_details: int = 0
    failed_transformations: int = 0
    failed_submissions: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    @property
    def success_rate(self) -> float:
        """Calculate overall success rate."""
        if self.total_animals_found == 0:
            return 0.0
        return (self.total_animals_submitted / self.total_animals_found) * 100


class AnimalETLProcessor:
    """
    Main ETL processor for extracting, transforming, and loading animal data.
    """
    
    def __init__(self, 
                 base_url: str = config.BASE_URL,
                 batch_size: int = config.BATCH_SIZE,
                 max_retries: int = config.MAX_RETRIES,
                 timeout: int = config.TIMEOUT):
        """
        Initialize the ETL processor.
        
        Args:
            base_url: Base URL for the API
            batch_size: Number of animals to process in each batch
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.timeout = timeout
        self.stats = ETLStats()
    
    def extract_animals(self, client: AnimalAPIClient) -> List[AnimalSummary]:
        """
        Extract all animals from the API.
        
        Args:
            client: API client instance
            
        Returns:
            List of animal summaries
            
        Raises:
            APIError: If extraction fails
        """
        logger.info("Starting animal extraction...")
        try:
            animals = client.get_all_animals()
            self.stats.total_animals_found = len(animals)
            logger.info(f"Successfully extracted {len(animals)} animals")
            return animals
        except APIError as e:
            logger.error(f"Failed to extract animals: {str(e)}")
            raise
    
    def extract_animal_details(self, client: AnimalAPIClient, 
                             animal_summaries: List[AnimalSummary]) -> List[AnimalDetail]:
        """
        Extract detailed information for all animals.
        
        Args:
            client: API client instance
            animal_summaries: List of animal summaries
            
        Returns:
            List of animal details
        """
        logger.info("Starting animal detail extraction...")
        animal_details = []
        failed_count = 0
        
        for animal_summary in animal_summaries:
            try:
                detail = client.get_animal_detail(animal_summary.id)
                animal_details.append(detail)
            except APIError as e:
                logger.error(f"Failed to get details for animal {animal_summary.id}: {str(e)}")
                failed_count += 1
                continue
        
        self.stats.total_animals_detailed = len(animal_details)
        self.stats.failed_details = failed_count
        
        logger.info(f"Successfully extracted details for {len(animal_details)} animals "
                   f"({failed_count} failed)")
        return animal_details
    
    def transform_animals(self, animal_details: List[AnimalDetail]) -> List[TransformedAnimal]:
        """
        Transform animal details into the required format.
        
        Args:
            animal_details: List of animal details to transform
            
        Returns:
            List of transformed animals
        """
        logger.info("Starting animal transformation...")
        
        initial_count = len(animal_details)
        transformed_animals = transform_animals_batch(animal_details)
        
        self.stats.total_animals_transformed = len(transformed_animals)
        self.stats.failed_transformations = initial_count - len(transformed_animals)
        
        logger.info(f"Successfully transformed {len(transformed_animals)} animals "
                   f"({self.stats.failed_transformations} failed)")
        
        return transformed_animals
    
    def load_animals(self, client: AnimalAPIClient, 
                    transformed_animals: List[TransformedAnimal]) -> bool:
        """
        Load transformed animals to the destination endpoint in batches.
        
        Args:
            client: API client instance
            transformed_animals: List of transformed animals to submit
            
        Returns:
            True if all batches were submitted successfully
        """
        logger.info(f"Starting to load {len(transformed_animals)} animals in batches of {self.batch_size}")
        
        total_submitted = 0
        failed_batches = 0
        batch_number = 0

        for i in range(0, len(transformed_animals), self.batch_size):
            batch_number += 1
            batch = transformed_animals[i:i + self.batch_size]
            
            logger.info(f"Processing batch {batch_number}: {len(batch)} animals")
            
            try:
                success = client.submit_animals_batch(batch)
                if success:
                    total_submitted += len(batch)
                    logger.info(f"Successfully submitted batch {batch_number}")
                else:
                    logger.error(f"Failed to submit batch {batch_number}")
                    failed_batches += 1
                    
            except (APIError, ValueError) as e:
                logger.error(f"Error submitting batch {batch_number}: {str(e)}")
                failed_batches += 1
                continue
        
        self.stats.total_animals_submitted = total_submitted
        self.stats.total_batches_submitted = batch_number - failed_batches
        self.stats.failed_submissions = failed_batches
        
        logger.info(f"Finished loading animals: {total_submitted} submitted, {failed_batches} batches failed")
        
        return failed_batches == 0
    
    def run(self) -> bool:
        """
        Run the complete ETL process.
        
        Returns:
            True if the entire process completed successfully
        """
        self.stats.start_time = time.time()
        logger.info("Starting Animal ETL process...")
        
        try:
            with AnimalAPIClient(self.base_url, self.timeout) as client:
                # Extract animals
                animal_summaries = self.extract_animals(client)
                if not animal_summaries:
                    logger.warning("No animals found to process")
                    return True
                
                # Extract animal details
                animal_details = self.extract_animal_details(client, animal_summaries)
                if not animal_details:
                    logger.error("No animal details could be extracted")
                    return False
                
                # Transform animals
                transformed_animals = self.transform_animals(animal_details)
                if not transformed_animals:
                    logger.error("No animals could be transformed")
                    return False
                
                # Load animals
                load_success = self.load_animals(client, transformed_animals)
                
                self.stats.end_time = time.time()
                self._log_final_stats()
                
                return load_success
                
        except Exception as e:
            self.stats.end_time = time.time()
            logger.error(f"ETL process failed with error: {str(e)}")
            self._log_final_stats()
            return False
    
    def _log_final_stats(self):
        """Log final statistics for the ETL process."""
        logger.info("=" * 50)
        logger.info("ETL PROCESS SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Duration: {self.stats.duration_seconds:.2f} seconds")
        logger.info(f"Animals found: {self.stats.total_animals_found}")
        logger.info(f"Details extracted: {self.stats.total_animals_detailed} "
                   f"(failed: {self.stats.failed_details})")
        logger.info(f"Animals transformed: {self.stats.total_animals_transformed} "
                   f"(failed: {self.stats.failed_transformations})")
        logger.info(f"Animals submitted: {self.stats.total_animals_submitted}")
        logger.info(f"Batches submitted: {self.stats.total_batches_submitted} "
                   f"(failed: {self.stats.failed_submissions})")
        logger.info(f"Overall success rate: {self.stats.success_rate:.1f}%")
        logger.info("=" * 50)
    
    def get_stats(self) -> ETLStats:
        """
        Get the current ETL statistics.
        
        Returns:
            ETLStats object with current statistics
        """
        return self.stats