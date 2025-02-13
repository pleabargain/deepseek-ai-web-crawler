import json
import os
import time
from typing import List, Set, Tuple
from urllib.parse import urljoin

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
)

from models.hotel import Hotel
from utils.data_utils import is_complete_hotel, is_duplicate_hotel
from utils.logger import CrawlerLogger

# Initialize logger
logger = CrawlerLogger("scraper")


def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    try:
        # https://docs.crawl4ai.com/core/browser-crawler-config/
        config = BrowserConfig(
            browser_type="chromium"  # Type of browser to simulate
        )
        logger.info("[CONFIG] Browser configuration created successfully")
        return config
    except Exception as e:
        logger.error("[CONFIG] Failed to create browser configuration", error=str(e))
        raise


def get_llm_strategy() -> LLMExtractionStrategy:
    """
    Returns the configuration for the language model extraction strategy.

    Returns:
        LLMExtractionStrategy: The settings for how to extract data using LLM.
    """
    try:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY environment variable not found")

        # https://docs.crawl4ai.com/api/strategies/#llmextractionstrategy
        strategy = LLMExtractionStrategy(
            provider="groq/deepseek-r1-distill-llama-70b",  # Name of the LLM provider
            api_token=api_key,  # API token for authentication
            schema=Hotel.model_json_schema(),  # JSON schema of the data model
            extraction_type="schema",  # Type of extraction to perform
            instruction=(
                "Extract hotel information from the Russian website content. For each hotel, extract:\n"
                "1. name (including star rating, e.g., 'Heritance Aarah 5*')\n"
                "2. hotel_type (e.g., 'Пляжный отель')\n"
                "3. discount offers if any (e.g., 'Скидки до 35%')\n"
                "4. stars as a number (e.g., 5)\n"
                "5. image_urls as a list\n"
                "6. description in Russian\n"
                "7. room_types as a list of available categories\n"
                "8. meal_plan options\n"
                "9. facilities as a list of amenities\n"
                "10. location (atoll/area in Maldives)\n"
                "11. price information\n"
                "12. check_in and check_out dates if available\n"
                "13. adults and children counts if available\n"
                "14. full URL to the hotel's detail page\n\n"
                "Handle Russian text appropriately and ensure all URLs are absolute."
            ),
            input_format="html",  # Format of the input content
            verbose=True,  # Enable verbose logging
        )
        logger.info("[CONFIG] LLM strategy created successfully")
        return strategy
    except Exception as e:
        logger.error("[CONFIG] Failed to create LLM strategy", error=str(e))
        raise


async def check_no_results(
    crawler: AsyncWebCrawler,
    url: str,
    session_id: str,
) -> bool:
    """
    Checks if the "No Results Found" message is present on the page.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL to check.
        session_id (str): The session identifier.

    Returns:
        bool: True if "No Results Found" message is found, False otherwise.
    """
    start_time = time.time()
    try:
        # Fetch the page without any CSS selector or extraction strategy
        result = await crawler.arun(
            url=url,
            config=CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                session_id=session_id,
            ),
        )

        fetch_time = time.time() - start_time
        
        if result.success:
            logger.log_fetch(url, "success", fetch_time)
            
            # Check for various "no results" indicators in Russian
            no_results_phrases = [
                "No Results Found",
                "Ничего не найдено",
                "нет результатов",
                "не найдено отелей"
            ]
            
            for phrase in no_results_phrases:
                if phrase in result.cleaned_html:
                    logger.info("[CHECK] No results found message detected", url=url, phrase=phrase)
                    return True
                    
            return False
        else:
            logger.error(
                "[CHECK] Error checking for 'No Results Found'",
                url=url,
                error=result.error_message
            )
            return False

    except Exception as e:
        logger.error("[CHECK] Failed to check for 'No Results Found'", url=url, error=str(e))
        return False


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
) -> Tuple[List[dict], bool]:
    """
    Fetches and processes a single page of hotel data.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        page_number (int): The page number to fetch.
        base_url (str): The base URL of the website.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.
        required_keys (List[str]): List of required keys in the hotel data.
        seen_names (Set[str]): Set of hotel names that have already been seen.

    Returns:
        Tuple[List[dict], bool]:
            - List[dict]: A list of processed hotels from the page.
            - bool: A flag indicating if the "No Results Found" message was encountered.
    """
    url = f"{base_url}?page={page_number}"
    logger.info(f"[PAGE] Processing page {page_number}", url=url)

    try:
        # Check if "No Results Found" message is present
        no_results = await check_no_results(crawler, url, session_id)
        if no_results:
            return [], True  # No more results, signal to stop crawling

        # Fetch page content with the extraction strategy
        fetch_start = time.time()
        result = await crawler.arun(
            url=url,
            config=CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,  # Do not use cached data
                extraction_strategy=llm_strategy,  # Strategy for data extraction
                css_selector=css_selector,  # Target specific content on the page
                session_id=session_id,  # Unique session ID for the crawl
            ),
        )
        fetch_time = time.time() - fetch_start

        if not result.success:
            logger.error(
                f"[FETCH] Failed to fetch page {page_number}",
                url=url,
                error=result.error_message
            )
            return [], False

        # Log raw content for debugging
        logger.debug(
            f"[RAW] Page {page_number} content",
            content=result.cleaned_html[:500],  # First 500 chars for debug
            css_matches=result.css_matches_count if hasattr(result, 'css_matches_count') else 'N/A'
        )

        # Parse extracted content
        try:
            if not result.extracted_content:
                logger.error(
                    f"[EXTRACT] No content extracted from page {page_number}",
                    url=url,
                    html_length=len(result.cleaned_html) if result.cleaned_html else 0,
                    css_selector=css_selector
                )
                return [], False

            extracted_data = json.loads(result.extracted_content)
            logger.debug(
                f"[PARSE] Parsed JSON from page {page_number}",
                data_length=len(str(extracted_data)),
                items_count=len(extracted_data) if isinstance(extracted_data, list) else 0
            )

        except json.JSONDecodeError as e:
            logger.error(
                f"[PARSE] Failed to parse JSON from page {page_number}",
                url=url,
                error=str(e),
                content_sample=result.extracted_content[:200] if result.extracted_content else None,
                exc_info=True
            )
            return [], False
        except Exception as e:
            logger.error(
                f"[PARSE] Unexpected error parsing page {page_number}",
                url=url,
                error=str(e),
                exc_info=True
            )
            return [], False

        if not extracted_data:
            logger.warning(
                f"[EXTRACT] No hotels found on page {page_number}",
                url=url,
                content_type=type(extracted_data).__name__
            )
            return [], False

        # Process hotels
        complete_hotels = []
        for idx, hotel in enumerate(extracted_data):
            try:
                # Log raw hotel data for debugging
                logger.debug(
                    f"[HOTEL] Processing hotel {idx + 1}",
                    hotel_data=str(hotel)[:200]  # First 200 chars for debug
                )

                # Ignore the 'error' key if it's False
                if hotel.get("error") is False:
                    hotel.pop("error", None)

                # Ensure URL is absolute
                if "url" in hotel and not hotel["url"].startswith(("http://", "https://")):
                    hotel["url"] = urljoin(base_url, hotel["url"])

                # Check for required keys
                if not is_complete_hotel(hotel, required_keys):
                    missing = [k for k in required_keys if k not in hotel]
                    present = [k for k in required_keys if k in hotel]
                    logger.warning(
                        "[VALIDATE] Incomplete hotel data",
                        hotel_name=hotel.get("name", "Unknown"),
                        missing_keys=missing,
                        present_keys=present,
                        hotel_keys=list(hotel.keys())
                    )
                    continue

                # Check for duplicate
                if is_duplicate_hotel(hotel["name"], seen_names):
                    logger.info(
                        "[DUPLICATE] Skipping duplicate hotel",
                        hotel_name=hotel["name"],
                        total_seen=len(seen_names)
                    )
                    continue

                # Add hotel to the list
                seen_names.add(hotel["name"])
                complete_hotels.append(hotel)
                logger.debug(
                    "[SUCCESS] Hotel processed successfully",
                    hotel_name=hotel["name"],
                    fields_count=len(hotel)
                )

            except Exception as e:
                logger.error(
                    "[PROCESS] Failed to process hotel",
                    hotel_idx=idx,
                    hotel_data=str(hotel)[:200],
                    error=str(e),
                    exc_info=True
                )
                continue

        if not complete_hotels:
            logger.warning(
                f"[EXTRACT] No complete hotels found on page {page_number}",
                url=url
            )
            return [], False

        logger.info(
            f"[SUCCESS] Processed page {page_number}",
            url=url,
            hotels_count=len(complete_hotels),
            time=f"{fetch_time:.2f}s"
        )
        return complete_hotels, False

    except Exception as e:
        logger.error(
            f"[FATAL] Failed to process page {page_number}",
            url=url,
            error=str(e),
            exc_info=True
        )
        return [], False
