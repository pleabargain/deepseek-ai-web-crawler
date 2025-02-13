# config.py

BASE_URL = "https://www.luxe.ru/geo/maldives/hotels"
CSS_SELECTOR = ".hotel-card"  # Main container for each hotel listing
REQUIRED_KEYS = [
    "name",           # Hotel name (e.g., "Heritance Aarah 5*")
    "hotel_type",     # Type of hotel (e.g., "Пляжный отель")
    "discount",       # Discount information (e.g., "Скидки до 35%")
    "stars",         # Star rating extracted from name
    "image_urls",     # List of hotel images
    "description",    # Hotel description
    "room_types",     # Available room categories
    "meal_plan",      # Meal plan options
    "facilities",     # Hotel facilities and amenities
    "location",      # Location on Maldives
    "price",         # Price information
    "check_in",      # Check-in date
    "check_out",     # Check-out date
    "adults",        # Number of adults
    "children",      # Number of children
    "url"            # Direct URL to hotel details page
]
