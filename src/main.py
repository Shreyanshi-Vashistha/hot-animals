#!/usr/bin/env python3
import logging
import sys
from typing import Optional
import click

import config
from etl_processor import AnimalETLProcessor

def setup_logging(log_level: str = config.LOG_LEVEL):
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('animal_etl.log', mode='a')
        ]
    )
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

@click.command()
@click.option('--base-url', default=config.BASE_URL, help='Base URL for the Animal API')
@click.option('--batch-size', default=config.BATCH_SIZE, type=int, help='Number of animals to process per batch')
@click.option('--max-retries', default=config.MAX_RETRIES, type=int, help='Maximum retry attempts')
@click.option('--timeout', default=config.TIMEOUT, type=int, help='Request timeout in seconds')
@click.option('--log-level', default=config.LOG_LEVEL,
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              help='Logging level')
@click.option('--dry-run', is_flag=True, default=False, help='Run ETL without submitting data')
def cli(base_url: str, batch_size: int, max_retries: int, timeout: int,
        log_level: str, dry_run: bool):
    setup_logging(log_level)
    logger = logging.getLogger(__name__)
    logger.info("Starting Leadpages Animal ETL System")
    logger.info(f"Configuration: base_url={base_url}, batch_size={batch_size}, "
                f"max_retries={max_retries}, timeout={timeout}")
    
    processor = AnimalETLProcessor(base_url, batch_size, max_retries, timeout)
    
    try:
        if dry_run:
            success = run_dry_run(processor)
        else:
            success = processor.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("ETL process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

def run_dry_run(processor: AnimalETLProcessor) -> bool:
    logger = logging.getLogger(__name__)
    try:
        from api_client import AnimalAPIClient
        with AnimalAPIClient(processor.base_url, processor.timeout) as client:
            animal_summaries = processor.extract_animals(client)
            if not animal_summaries:
                logger.warning("No animals found to process")
                return True
            animal_details = processor.extract_animal_details(client, animal_summaries)
            transformed = processor.transform_animals(animal_details)
            logger.info(f"DRY RUN: {len(transformed)} animals processed")
            processor._log_final_stats()
            return True
    except Exception as e:
        logger.error(f"Dry run failed: {e}")
        return False

if __name__ == '__main__':
    cli()
