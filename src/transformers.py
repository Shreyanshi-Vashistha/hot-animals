import logging
from datetime import datetime
from typing import List, Optional
from dateutil import parser, tz

from models import AnimalDetail, TransformedAnimal, TransformationError

logger = logging.getLogger(__name__)

def transform_friends(friends_str: str) -> List[str]:
    """
    Transform comma-delimited friends string to a list of strings.
    
    Args:
        friends_str: Comma-delimited string of friends
        
    Returns:
        List of friend names, empty list if input is empty/None
        
    Raises:
        TransformationError: If transformation fails
    """
    if not friends_str or friends_str.strip() == "":
        return []

    try:
        # Split by comma and clean up whitespace
        friends_list = [friend.strip() for friend in friends_str.split(",")]
        # Filter out empty strings
        friends_list = [friend for friend in friends_list if friend]
        return friends_list
    except Exception as e:
        raise TransformationError(
            f"Failed to transform friends field: {str(e)}",
            field="friends"
        )


def transform_born_at(born_at_val) -> Optional[str]:
    """
    Transform born_at field to ISO8601 UTC timestamp.

    Args:
        born_at_val: date string, datetime object, or None

    Returns:
        ISO8601 UTC timestamp string, or None if input is None/empty
    """
    if born_at_val is None:
        return None

    # If it's a datetime object
    if isinstance(born_at_val, datetime):
        if born_at_val.tzinfo is None:
            return born_at_val.replace(tzinfo=tz.UTC).isoformat()
        else:
            return born_at_val.astimezone(tz.UTC).isoformat()

    if isinstance(born_at_val, str) and born_at_val.strip() == "":
        return None

    try:
        # Parse string and convert to UTC
        parsed_date = parser.parse(born_at_val)
        if parsed_date.tzinfo is None:
            utc_date = parsed_date.replace(tzinfo=tz.UTC)
        else:
            utc_date = parsed_date.astimezone(tz.UTC)
        return utc_date.isoformat()

    except (ValueError, parser.ParserError) as e:
        raise TransformationError(
            f"Failed to parse born_at value '{born_at_val}': {str(e)}",
            field="born_at"
        )

def transform_animal(animal: AnimalDetail) -> TransformedAnimal:
    """
    Transform an AnimalDetail into a TransformedAnimal ready for submission.
    
    Args:
        animal: AnimalDetail instance to transform
        
    Returns:
        TransformedAnimal instance with transformed fields
        
    Raises:
        TransformationError: If any transformation fails
    """
    logger.debug(f"Transforming animal {animal.id}: {animal.name}")
    
    try:
        # Transform friends field
        if isinstance(animal.friends, list):
            transformed_friends = animal.friends
        else:
            transformed_friends = transform_friends(animal.friends)
        
        # Transform born_at field
        if isinstance(animal.born_at, datetime):
            # Convert datetime to ISO string
            transformed_born_at = animal.born_at.isoformat()
        else:
            # Transform from string
            transformed_born_at = transform_born_at(animal.born_at)
            
        # Create the transformed animal
        transformed = TransformedAnimal(
            id=animal.id,
            name=animal.name,
            friends=transformed_friends,
            born_at=transformed_born_at,
            **{k: v for k, v in animal.dict().items() 
               if k not in ['id', 'name', 'friends', 'born_at']}
        )
        
        logger.debug(f"Successfully transformed animal {animal.id}")
        return transformed
        
    except TransformationError:
        # Re-raise transformation errors with animal ID
        raise
    except Exception as e:
        raise TransformationError(
            f"Unexpected error transforming animal {animal.id}: {str(e)}",
            animal_id=animal.id
        )


def transform_animals_batch(animals: List[AnimalDetail]) -> List[TransformedAnimal]:
    """
    Transform a batch of animals, collecting any transformation errors.
    
    Args:
        animals: List of AnimalDetail instances to transform
        
    Returns:
        List of successfully transformed animals
        
    Note:
        Logs errors for failed transformations but continues processing
    """
    transformed_animals = []
    errors = []
    
    for animal in animals:
        try:
            transformed = transform_animal(animal)
            transformed_animals.append(transformed)
        except TransformationError as e:
            error_msg = f"Failed to transform animal {animal.id}: {e.message}"
            logger.error(error_msg)
            errors.append(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error transforming animal {animal.id}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
    
    if errors:
        logger.warning(f"Failed to transform {len(errors)} out of {len(animals)} animals")
    
    logger.info(f"Successfully transformed {len(transformed_animals)} out of {len(animals)} animals")
    return transformed_animals