from datetime import datetime
from typing import List, Optional, Union
from pydantic import BaseModel, Field, model_validator, validator


class AnimalSummary(BaseModel):
    """Model for animal summary data from the list endpoint."""
    
    id: int
    name: str
    
    class Config:
        extra = "allow"


class AnimalDetail(BaseModel):
    """Model for detailed animal data from the detail endpoint."""
    
    id: int
    name: str
    friends: Union[str, List[str]] 
    born_at: Optional[Union[str, datetime]] = None
    
    class Config:
        extra = "allow" 
    
    @validator('friends', pre=True, always=True)
    def validate_friends(cls, v):
        """Ensure friends is either a string or list of strings."""
        if isinstance(v, str):
            return v
        if isinstance(v, list):
            return [str(item) for item in v if item] 
        if v is None:
            return ""
        return str(v)
    
    @validator('born_at', pre=True, always=True)
    def validate_born_at(cls, v):
        """Ensure born_at is either None, string, or datetime."""
        if v is None or v == "":
            return None
        if isinstance(v, datetime):
            return v
        return str(v)


class TransformedAnimal(BaseModel):
    """Model for animal data after transformation, ready for submission."""
    
    id: int
    name: str
    friends: List[str]
    born_at: Optional[str] = None 
    
    class Config:
        extra = "allow" 


class PaginatedResponse(BaseModel):
    page: int
    total_pages: int
    items: List[AnimalSummary]

    @model_validator(mode="before")
    def check_pagination(cls, values):
        page = values.get('page')
        total_pages = values.get('total_pages')
        if page is not None and page < 1:
            raise ValueError('page must be >= 1')
        if total_pages is not None and total_pages < 1:
            raise ValueError('total_pages must be >= 1')
        return values


class APIError(Exception):
    """Custom exception for API-related errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        self.message = message
        self.status_code = status_code
        self.response_text = response_text
        super().__init__(self.message)


class TransformationError(Exception):
    """Custom exception for data transformation errors."""
    
    def __init__(self, message: str, animal_id: Optional[int] = None, field: Optional[str] = None):
        self.message = message
        self.animal_id = animal_id
        self.field = field
        super().__init__(self.message)