from typing import List, Optional
from pydantic import BaseModel


class Hotel(BaseModel):
    """
    Represents the data structure of a Hotel from luxe.ru.
    """
    name: str                    # Hotel name (e.g., "Heritance Aarah 5*")
    hotel_type: str             # Type of hotel (e.g., "Пляжный отель")
    discount: Optional[str]      # Discount information (e.g., "Скидки до 35%")
    stars: int                  # Star rating extracted from name
    image_urls: List[str]       # List of hotel images
    description: str            # Hotel description
    room_types: List[str]       # Available room categories
    meal_plan: Optional[str]    # Meal plan options
    facilities: List[str]       # Hotel facilities and amenities
    location: str              # Location on Maldives
    price: str                 # Price information
    check_in: Optional[str]    # Check-in date
    check_out: Optional[str]   # Check-out date
    adults: Optional[int]      # Number of adults
    children: Optional[int]    # Number of children
    url: str                   # Direct URL to hotel details page
