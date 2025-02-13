import csv
from typing import Dict, List, Set
from urllib.parse import urlparse

from models.hotel import Hotel
from utils.logger import CrawlerLogger

# Initialize logger
logger = CrawlerLogger("data")


def is_duplicate_hotel(hotel_name: str, seen_names: Set[str]) -> bool:
    """
    Check if a hotel name has already been processed.

    Args:
        hotel_name (str): The name of the hotel to check
        seen_names (Set[str]): Set of previously processed hotel names

    Returns:
        bool: True if the hotel name is a duplicate, False otherwise
    """
    try:
        is_duplicate = hotel_name in seen_names
        if is_duplicate:
            logger.info("[DUPLICATE] Hotel already processed", hotel_name=hotel_name)
        return is_duplicate
    except Exception as e:
        logger.error("[DUPLICATE] Error checking for duplicate hotel", 
                    hotel_name=hotel_name, 
                    error=str(e))
        return False


def is_complete_hotel(hotel: Dict, required_keys: List[str]) -> bool:
    """
    Check if a hotel dictionary contains all required keys.

    Args:
        hotel (Dict): The hotel data to validate
        required_keys (List[str]): List of required keys

    Returns:
        bool: True if the hotel contains all required keys, False otherwise
    """
    try:
        missing_keys = [key for key in required_keys if key not in hotel]
        
        if missing_keys:
            logger.warning(
                "[VALIDATE] Incomplete hotel data",
                hotel_name=hotel.get("name", "Unknown"),
                missing_keys=missing_keys
            )
            return False
            
        # Additional type validation for critical fields
        try:
            if not isinstance(hotel.get("image_urls", []), list):
                hotel["image_urls"] = [hotel.get("image_urls")] if hotel.get("image_urls") else []
                logger.debug("[FIX] Converted image_urls to list", hotel_name=hotel["name"])
            
            if not isinstance(hotel.get("room_types", []), list):
                hotel["room_types"] = [hotel.get("room_types")] if hotel.get("room_types") else []
                logger.debug("[FIX] Converted room_types to list", hotel_name=hotel["name"])
            
            if not isinstance(hotel.get("facilities", []), list):
                hotel["facilities"] = [hotel.get("facilities")] if hotel.get("facilities") else []
                logger.debug("[FIX] Converted facilities to list", hotel_name=hotel["name"])
            
            # Extract numeric star rating
            if "stars" in hotel and isinstance(hotel["stars"], str):
                try:
                    hotel["stars"] = int(''.join(filter(str.isdigit, hotel["stars"])))
                    logger.debug("[FIX] Converted stars to integer", hotel_name=hotel["name"])
                except ValueError:
                    hotel["stars"] = 0
                    logger.warning("[VALIDATE] Could not parse star rating", hotel_name=hotel["name"])
        
        except Exception as e:
            logger.error(
                "[VALIDATE] Error fixing data types",
                hotel_name=hotel.get("name", "Unknown"),
                error=str(e)
            )
            return False
            
        logger.info(
            "[VALIDATE] Hotel data complete",
            hotel_name=hotel.get("name", "Unknown")
        )
        return True
        
    except Exception as e:
        logger.error(
            "[VALIDATE] Error validating hotel data",
            hotel_data=str(hotel),
            error=str(e)
        )
        return False


def save_hotels_to_csv(hotels: List[Dict], filename: str) -> None:
    """
    Save a list of hotel dictionaries to a CSV file.

    Args:
        hotels (List[Dict]): List of hotel dictionaries to save
        filename (str): Name of the CSV file to create
    """
    if not hotels:
        logger.warning("[SAVE] No hotels to save")
        return

    try:
        # Extract domain from the first hotel's URL or use default
        if hotels and 'url' in hotels[0]:
            domain = urlparse(hotels[0]['url']).netloc.replace('www.', '')
        else:
            domain = 'luxe.ru'  # Default domain if no URL available
        
        # Prepend domain to filename
        filename = f"{domain}_{filename}"
        
        # Use field names from the Hotel model
        fieldnames = Hotel.model_fields.keys()
        
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            # Track successful and failed writes
            success_count = 0
            error_count = 0
            
            for hotel in hotels:
                try:
                    # Convert list fields to string for CSV
                    hotel_data = hotel.copy()
                    for field in ['image_urls', 'room_types', 'facilities']:
                        if field in hotel_data and isinstance(hotel_data[field], list):
                            hotel_data[field] = '|'.join(hotel_data[field])
                    
                    writer.writerow(hotel_data)
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(
                        "[SAVE] Failed to write hotel to CSV",
                        hotel_name=hotel.get("name", "Unknown"),
                        error=str(e)
                    )
            
            logger.info(
                "[SAVE] Hotels saved to CSV",
                filename=filename,
                total_hotels=len(hotels),
                successful_saves=success_count,
                failed_saves=error_count
            )
            
    except Exception as e:
        logger.error(
            "[SAVE] Failed to create CSV file",
            filename=filename,
            error=str(e)
        )
        raise
