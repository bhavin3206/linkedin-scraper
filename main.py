from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import undetected_chromedriver as uc
from threading import Thread, Lock
from bs4 import BeautifulSoup
import concurrent.futures
from queue import Queue
import pymongo
import random
import time
import re
import signal
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("linkedin_scraper.log", encoding='utf-8'),  # Added encoding='utf-8'
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "host": "mongodb://localhost:27017/",
    "db_name": "local",
    "collection_name": "jobs"
}

# Scraper configuration
SCRAPER_CONFIG = {
    "num_drivers": 10,
    "user_agents": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
    ],
    "chrome_user_data_dir": r"C:\\Users\\bhavi\\AppData\\Local\\Google\\Chrome\\User Data",
    "chrome_profile": "Default",
    "chrome_version": 133,
    "page_load_wait": 2,
    "job_detail_wait": (1, 2),  # Random wait between these values
    "max_pages": 8,  # Limit to 8 pages (25 jobs per page = 200 jobs)
    "max_scroll_attempts": 10,
    "retry_attempts": 3,
    "wait_for_jobs_timeout": 120  # Wait up to 2 minutes for jobs
}

# Initialize global variables
client = None
db = None
collection = None
drivers = []
job_queue = Queue()
lock = Lock()
stop_event = False

class DatabaseManager:
    """Manages database connections and operations"""
    
    @staticmethod
    def connect():
        """Connect to MongoDB and return client, db, and collection objects"""
        try:
            client = pymongo.MongoClient(DB_CONFIG["host"])
            db = client[DB_CONFIG["db_name"]]
            collection = db[DB_CONFIG["collection_name"]]
            logger.info("Connected to MongoDB successfully")
            return client, db, collection
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            sys.exit(1)
    
    @staticmethod
    def insert_job(job_data):
        """Insert job data into the database with thread safety"""
        with lock:
            if not collection.find_one({"Job Id": job_data["Job Id"]}):
                collection.insert_one(job_data)
                return True
            return False

    @staticmethod
    def close():
        """Close MongoDB connection"""
        if client:
            client.close()
            logger.info("MongoDB connection closed")

class DriverManager:
    """Manages Selenium WebDriver instances"""
    
    @staticmethod
    def create_driver(headless=True, use_profile=False):
        """Create and return a new undetected Chrome driver"""
        try:
            user_agent = random.choice(SCRAPER_CONFIG["user_agents"])
            options = uc.ChromeOptions()
            options.add_argument(f"--user-agent={user_agent}")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            
            if use_profile:
                options.add_argument(f"--user-data-dir={SCRAPER_CONFIG['chrome_user_data_dir']}")
                options.add_argument(f"--profile-directory={SCRAPER_CONFIG['chrome_profile']}")
            
            driver = uc.Chrome(options=options, version_main=SCRAPER_CONFIG["chrome_version"], headless=headless)
            
            # Block unnecessary resources for better performance
            if not use_profile:
                driver.execute_cdp_cmd("Network.setBlockedURLs", {
                    "urls": [
                        "*.jpg", "*.png", "*.svg", "*.gif", "*.webp", 
                        "*.css", "*.woff2", "*.ttf", "*.mp4", "*.avi", "*.mkv",
                        "*.json", "*.xml", "*.websocket", 
                        "media.licdn.com", 
                        "linkedin.com/li/track",
                        "static.licdn.com/*", 
                        "fonts.gstatic.com/*",
                        "csp.withgoogle.com/*"
                    ]
                })
                driver.execute_cdp_cmd("Network.enable", {})
            
            return driver
        except Exception as e:
            logger.error(f"Failed to create driver: {e}")
            return None
    
    @staticmethod
    def create_drivers(count, headless=True):
        """Create multiple drivers and return them as a list"""
        driver_list = []
        for _ in range(count):
            driver = DriverManager.create_driver(headless)
            if driver:
                driver_list.append(driver)
        return driver_list
    
    @staticmethod
    def quit_drivers(driver_list):
        """Quit all drivers in the list"""
        for driver in driver_list:
            try:
                driver.quit()
            except Exception:
                pass

class DateTimeHelper:
    """Helper class for date and time operations"""
    
    @staticmethod
    def convert_to_datetime(post_time):
        """Convert LinkedIn relative time to datetime object"""
        if post_time == "Not Mentioned":
            return None
            
        current_time = datetime.now()
        
        # Parse the post time
        try:
            number = int(post_time.split()[0])
            
            if 'minute' in post_time:
                return current_time - timedelta(minutes=number)
            elif 'hour' in post_time:
                return current_time - timedelta(hours=number)
            elif 'day' in post_time:
                return current_time - timedelta(days=number)
            elif 'week' in post_time:
                return current_time - timedelta(weeks=number)
            elif 'month' in post_time:
                return current_time - relativedelta(months=number)
            elif 'year' in post_time:
                return current_time - relativedelta(years=number)
            else:
                return current_time
        except (ValueError, IndexError):
            return current_time

class JobScraper:
    """Main class for scraping LinkedIn jobs"""
    
    @staticmethod
    def get_job_links(url, driver):
        """Scrape job links from LinkedIn search results pages"""
        try:
            driver.get(url)
            time.sleep(SCRAPER_CONFIG["page_load_wait"])
            
            total_jobs = 0
            
            # Function to update page number in URL
            def update_page(current_url, start_number):
                if '&start=' in current_url:
                    return re.sub(r'&start=\d+', f'&start={start_number}', current_url)
                else:
                    return current_url + f'&start={start_number}'
            
            # Iterate through pages
            for page_num, i in enumerate(range(0, SCRAPER_CONFIG["max_pages"] * 25, 25), 1):
                if stop_event:
                    break
                    
                # If not first page, navigate to next page
                if i > 0:
                    updated_url = update_page(driver.current_url, i)
                    driver.get(updated_url)
                    time.sleep(SCRAPER_CONFIG["page_load_wait"])
                
                # Find job elements
                job_elements = driver.find_elements(By.CLASS_NAME, "job-card-list__title--link")
                
                # Ensure jobs are visible with scrolling
                scroll_attempts = 0
                while len(job_elements) < 25 and scroll_attempts < SCRAPER_CONFIG["max_scroll_attempts"]:
                    if not job_elements:
                        break
                    
                    try:
                        driver.execute_script("arguments[0].scrollIntoView();", job_elements[-1])
                        time.sleep(0.5)  # Reduced wait time for scrolling
                        job_elements = driver.find_elements(By.CLASS_NAME, "job-card-list__title--link")
                    except (IndexError, Exception) as e:
                        logger.warning(f"Scroll error: {e}")
                        break
                    
                    scroll_attempts += 1
                
                # Process job elements
                page_jobs = 0
                for job_element in job_elements:
                    try:
                        job_url = job_element.get_attribute('href')
                        
                        # Extract job type if available
                        try:
                            metadata_wrapper = job_element.find_element(By.XPATH, 
                                "./ancestor::div[contains(@class, 'artdeco-entity-lockup__content')]//ul[contains(@class, 'job-card-container__metadata-wrapper')]")
                            match = re.search(r'\((.*?)\)', metadata_wrapper.text.strip())
                            job_type = match.group(1) if match else "Unknown"
                        except Exception:
                            job_type = "Unknown"
                        
                        # Add job to queue
                        job_queue.put((job_url, job_type))
                        page_jobs += 1
                    except Exception as e:
                        logger.warning(f"Error processing job element: {e}")
                
                total_jobs += page_jobs
                logger.info(f"Added {page_jobs} jobs from Page {page_num} | Total: {total_jobs}")
                
                # Check if we've reached the end of available jobs
                if page_jobs == 0:
                    logger.info("No more jobs found, stopping pagination")
                    break
            
            return total_jobs
        except Exception as e:
            logger.error(f"Error in get_job_links: {e}")
            return 0
    
    @staticmethod
    def get_job_details(driver):
        """Extract detailed information from individual job pages"""
        logger.info('Waiting for job links to be available...')
        
        # Wait for jobs to be available
        start_time = time.time()
        while job_queue.empty() and not stop_event:
            time.sleep(1)
            if time.time() - start_time > SCRAPER_CONFIG["wait_for_jobs_timeout"]:
                logger.warning(f"No jobs found after waiting {SCRAPER_CONFIG['wait_for_jobs_timeout']} seconds, exiting worker")
                return
        
        if stop_event:
            return
            
        logger.info('Processing jobs...')
        
        # Process jobs from queue
        while not stop_event:
            job_url = None
            job_type = None
            try:
                # Get job from queue with timeout
                job_url, job_type = job_queue.get(timeout=5)
                
                # Check if the job is None (end signal)
                if job_url is None:
                    job_queue.task_done()
                    break
                
                logger.info(f"Processing job: {job_url}")
                
                # Extract job ID from URL
                try:
                    job_id = job_url.split('jobs/view/')[-1].split('/')[0]
                    job_id = job_id.split('?')[0]  # Handle URLs with query parameters
                except Exception as e:
                    logger.error(f"Error extracting job ID from URL {job_url}: {e}")
                    job_queue.task_done()
                    continue
                
                logger.debug(f"Job ID: {job_id}")
                
                # Skip if job already exists
                if collection.find_one({'Job Id': job_id}):
                    logger.info(f"Job {job_id} already exists in database, skipping")
                    job_queue.task_done()
                    continue
                
                # Initialize job data
                job_data = {
                    'Job Id': job_id,
                    'Job Url': job_url,
                    'Job Type': job_type,
                    'Scrape Time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Load job page
                driver.get(job_url)
                wait_time = random.uniform(*SCRAPER_CONFIG["job_detail_wait"])
                time.sleep(wait_time)
                
                # Extract job details with retries
                soup = None
                h4 = None
                job_title = None
                
                for attempt in range(SCRAPER_CONFIG["retry_attempts"]):
                    try:
                        logger.debug(f"Attempt {attempt+1} to parse job page")
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        
                        # Check if the page has loaded properly
                        h4 = soup.select_one('h1 + h4') or soup.select_one('section.top-card-layout h4')
                        job_title = soup.find('h1')
                        
                        if h4 and job_title:
                            logger.debug("Job page loaded successfully")
                            break
                        
                        if attempt < SCRAPER_CONFIG["retry_attempts"] - 1:
                            logger.warning(f"Retrying page load for job {job_id} (attempt {attempt + 1})")
                            driver.refresh()
                            time.sleep(2)
                    except Exception as parsing_error:
                        logger.warning(f"Error parsing job page on attempt {attempt+1}: {parsing_error}")
                        if attempt < SCRAPER_CONFIG["retry_attempts"] - 1:
                            driver.refresh()
                            time.sleep(2)
                
                # Skip if page didn't load properly
                if not h4 or not job_title:
                    logger.warning(f"Failed to load job page for {job_id} after {SCRAPER_CONFIG['retry_attempts']} attempts")
                    # Save the page source for debugging
                    with open(f"error_page_{job_id}.html", "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    job_queue.task_done()
                    continue
                
                # Extract job information
                try:
                    job_data['Job Title'] = job_title.get_text(strip=True) if job_title else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error extracting job title: {e}")
                    job_data['Job Title'] = "Not Mentioned"
                
                # Job description
                try:
                    job_description = soup.find(class_='description__text')
                    job_data['Job Description'] = job_description.get_text(strip=True) if job_description else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error extracting job description: {e}")
                    job_data['Job Description'] = "Not Mentioned"
                
                # Job criteria
                try:
                    job_criteria = soup.find('ul', class_='description__job-criteria-list')
                    if job_criteria:
                        for li in job_criteria.find_all('li', class_='description__job-criteria-item'):
                            try:
                                heading = li.find('h3', class_='description__job-criteria-subheader')
                                value = li.find('span', class_='description__job-criteria-text')
                                
                                if heading and value:
                                    job_data[heading.get_text(strip=True)] = value.get_text(strip=True)
                            except Exception as e:
                                logger.error(f"Error extracting job criteria item: {e}")
                except Exception as e:
                    logger.error(f"Error extracting job criteria: {e}")
                
                # Company information
                try:
                    company_name = h4.find('a', class_='topcard__org-name-link')
                    job_data['Company Name'] = company_name.get_text(strip=True) if company_name else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error extracting company name: {e}")
                    job_data['Company Name'] = "Not Mentioned"
                
                try:
                    company_link = h4.find('a', class_='topcard__org-name-link')
                    job_data['Company Link'] = company_link['href'] if company_link and 'href' in company_link.attrs else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error extracting company link: {e}")
                    job_data['Company Link'] = "Not Mentioned"
                
                try:
                    location = h4.find('span', class_='topcard__flavor--bullet')
                    job_data['Company Location'] = location.get_text(strip=True) if location else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error extracting company location: {e}")
                    job_data['Company Location'] = "Not Mentioned"
                
                # Posting time
                try:
                    post_time = h4.find('span', class_='posted-time-ago__text')
                    post_time_text = post_time.text.strip() if post_time else "Not Mentioned"
                    job_data['Post Time'] = post_time_text
                except Exception as e:
                    logger.error(f"Error extracting post time: {e}")
                    job_data['Post Time'] = "Not Mentioned"
                
                # Convert posting time
                try:
                    converted_time = DateTimeHelper.convert_to_datetime(job_data['Post Time'])
                    job_data['Post Converted Time'] = converted_time.strftime("%Y-%m-%d %H:%M:%S") if converted_time else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error converting post time: {e}")
                    job_data['Post Converted Time'] = "Not Mentioned"
                
                # Applicant information
                try:
                    applicants = h4.find('figcaption') or h4.find('span', class_='num-applicants__caption')
                    job_data['Applicants Apply'] = applicants.get_text(strip=True) if applicants else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error extracting applicants info: {e}")
                    job_data['Applicants Apply'] = "Not Mentioned"
                
                # Salary information
                try:
                    salary_description = soup.find('p', class_='compensation__description')
                    job_data['Salary Description'] = salary_description.get_text(strip=True) if salary_description else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error extracting salary description: {e}")
                    job_data['Salary Description'] = "Not Mentioned"
                
                try:
                    salary_range = soup.find('div', class_='salary')
                    job_data['Salary Range'] = salary_range.get_text(strip=True) if salary_range else "Not Mentioned"
                except Exception as e:
                    logger.error(f"Error extracting salary range: {e}")
                    job_data['Salary Range'] = "Not Mentioned"
                
                # Insert job data into database
                try:
                    inserted = DatabaseManager.insert_job(job_data)
                    if inserted:
                        logger.info(f"Inserted job: {job_data['Job Title']} at {job_data['Company Name']}")
                    else:
                        logger.info(f"Job {job_id} already exists or could not be inserted")
                except Exception as e:
                    logger.error(f"Error inserting job data into database: {e}")

                job_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error processing job {job_url}: {e}", exc_info=True)  # Added exc_info for full traceback
                
                if job_url and "429" in str(e).lower():
                    logger.warning(f"Rate limit detected, restarting driver: {e}")
                    # Restart driver
                    with lock:
                        if driver in drivers:
                            drivers.remove(driver)
                        
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        
                        new_driver = DriverManager.create_driver()
                        if new_driver:
                            drivers.append(new_driver)
                            # Put the job back in the queue for retry
                            job_queue.put((job_url, job_type))
                    
                    return
                else:
                    try:
                        if job_url:  # Only mark as done if we had a valid job
                            job_queue.task_done()
                    except Exception:
                        pass

def signal_handler(sig, frame):
    """Handle interrupt signals"""
    global stop_event
    logger.info("Interrupt received, shutting down gracefully...")
    stop_event = True
    cleanup()

def cleanup():
    """Clean up resources"""
    logger.info("Cleaning up resources...")
    DriverManager.quit_drivers(drivers)
    DatabaseManager.close()

def main():
    """Main function to run the scraper"""
    global client, db, collection, drivers, job_queue, stop_event
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Connect to database
        client, db, collection = DatabaseManager.connect()
        
        # LinkedIn search URL
        linkedin_url = "https://www.linkedin.com/jobs/search/?currentJobId=4141450053&geoId=103644278&keywords=python%20developer&origin=JOB_SEARCH_PAGE_LOCATION_AUTOCOMPLETE&refresh=true"
        
        # Create main driver for pagination
        main_driver = DriverManager.create_driver(headless=False, use_profile=True)
        if not main_driver:
            logger.error("Failed to create main driver")
            return
        
        # Create worker drivers
        drivers = DriverManager.create_drivers(SCRAPER_CONFIG["num_drivers"])
        if not drivers:
            logger.error("Failed to create worker drivers")
            main_driver.quit()
            return
        
        # Run URL scraper in a thread
        url_thread = Thread(target=JobScraper.get_job_links, args=(linkedin_url, main_driver))
        url_thread.start()
        
        # Start worker threads
        worker_threads = []
        for driver in drivers:
            worker_thread = Thread(target=JobScraper.get_job_details, args=(driver,))
            worker_thread.start()
            worker_threads.append(worker_thread)
        
        # Wait for URL scraper to finish
        url_thread.join()
        
        # Add end signals to queue
        for _ in range(len(drivers)):
            job_queue.put((None, None))
        
        # Wait for workers to finish
        for thread in worker_threads:
            thread.join()
        
        logger.info("✅ LinkedIn Scraping Done Successfully!")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        cleanup()

if __name__ == "__main__":
    main()