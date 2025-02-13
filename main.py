import asyncio
import sys
import time
import traceback
from datetime import datetime, timezone

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS
from utils.data_utils import save_hotels_to_csv
from utils.logger import CrawlerLogger
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)

# Initialize logger
logger = CrawlerLogger()

async def crawl_hotels():
    """
    Main function to crawl hotel data from the website.
    """
    start_time = time.time()
    try:
        logger.info("[START] Beginning hotel crawl", base_url=BASE_URL)

        # Initialize configurations
        try:
            browser_config = get_browser_config()
            llm_strategy = get_llm_strategy()
            session_id = f"hotel_crawl_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            logger.info(
                "[CONFIG] Initialized crawler configuration",
                session_id=session_id,
                css_selector=CSS_SELECTOR,
                required_keys=REQUIRED_KEYS
            )
        except Exception as e:
            logger.critical(
                "[CONFIG] Failed to initialize crawler configuration",
                error=str(e),
                exc_info=True
            )
            raise

        # Initialize state variables
        page_number = 1
        all_hotels = []
        seen_names = set()
        total_pages_processed = 0
        total_hotels_found = 0
        total_hotels_skipped = 0
        retry_count = 0
        max_retries = 3

        # Start the web crawler context
        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                while True:
                    page_start_time = time.time()
                    
                    # Fetch and process data from the current page
                    try:
                        hotels, no_results_found = await fetch_and_process_page(
                            crawler,
                            page_number,
                            BASE_URL,
                            CSS_SELECTOR,
                            llm_strategy,
                            session_id,
                            REQUIRED_KEYS,
                            seen_names,
                        )
                        page_time = time.time() - page_start_time

                        if no_results_found:
                            logger.info(
                                "[END] No more hotels found. Ending crawl.",
                                total_pages=total_pages_processed,
                                total_hotels=len(all_hotels)
                            )
                            break

                        total_pages_processed += 1
                        total_hotels_found += len(hotels) if hotels else 0
                        total_hotels_skipped = total_hotels_found - len(all_hotels)

                        if not hotels:
                            logger.warning(
                                f"[EXTRACT] No hotels extracted from page {page_number}",
                                time=f"{page_time:.2f}s",
                                total_processed=total_pages_processed,
                                total_hotels=len(all_hotels)
                            )
                            
                            # Implement retry logic
                            if retry_count < max_retries:
                                retry_count += 1
                                logger.info(
                                    f"[RETRY] Attempting retry {retry_count} of {max_retries}",
                                    page=page_number
                                )
                                await asyncio.sleep(5)  # Wait longer between retries
                                continue
                            else:
                                logger.error(
                                    f"[RETRY] Max retries ({max_retries}) reached",
                                    page=page_number
                                )
                                break

                        # Reset retry count on success
                        retry_count = 0

                        # Log successful extraction
                        logger.info(
                            f"[EXTRACT] Page {page_number} processed",
                            hotels_count=len(hotels),
                            time=f"{page_time:.2f}s",
                            total_hotels=len(all_hotels),
                            skipped_hotels=total_hotels_skipped
                        )

                        # Add the hotels from this page to the total list
                        all_hotels.extend(hotels)
                        page_number += 1

                        # Pause between requests
                        await asyncio.sleep(2)

                    except Exception as e:
                        logger.error(
                            f"[ERROR] Failed processing page {page_number}",
                            error=str(e),
                            traceback=traceback.format_exc(),
                            exc_info=True
                        )
                        if retry_count < max_retries:
                            retry_count += 1
                            logger.info(
                                f"[RETRY] Attempting retry {retry_count} of {max_retries}",
                                page=page_number
                            )
                            await asyncio.sleep(5)
                            continue
                        break

        except Exception as e:
            logger.critical(
                "[CRAWLER] Failed to initialize web crawler",
                error=str(e),
                exc_info=True
            )
            raise

        # Save the collected hotels to a CSV file
        if all_hotels:
            try:
                save_hotels_to_csv(all_hotels, "complete_hotels.csv")
                logger.info(
                    "[SAVE] Hotels saved to CSV",
                    count=len(all_hotels),
                    file="complete_hotels.csv"
                )
            except Exception as e:
                logger.error(
                    "[SAVE] Failed to save hotels to CSV",
                    error=str(e),
                    exc_info=True
                )
        else:
            logger.warning(
                "[SAVE] No hotels were found during the crawl",
                pages_processed=total_pages_processed
            )

        # Log completion statistics
        total_time = time.time() - start_time
        logger.info(
            "[COMPLETE] Crawl finished",
            total_time=f"{total_time:.2f}s",
            total_pages=total_pages_processed,
            total_hotels_found=total_hotels_found,
            total_hotels_saved=len(all_hotels),
            total_hotels_skipped=total_hotels_skipped
        )

        # Display usage statistics for the LLM strategy
        llm_strategy.show_usage()

    except Exception as e:
        logger.critical(
            "[FATAL] Crawl failed with error",
            error=str(e),
            traceback=traceback.format_exc(),
            exc_info=True
        )
        raise


async def main():
    """
    Entry point of the script.
    """
    try:
        await crawl_hotels()
    except Exception as e:
        logger.critical(
            "[FATAL] Application terminated with error",
            error=str(e),
            traceback=traceback.format_exc(),
            exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    load_dotenv()  # Load environment variables first
    asyncio.run(main())
