from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import logging
from datetime import datetime
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ZomatoScraper:
    def __init__(self):
        self.service = Service(ChromeDriverManager().install())
        self.options = webdriver.ChromeOptions()
        self._configure_options()
        self.driver = webdriver.Chrome(service=self.service, options=self.options)
        self.restaurant_urls = []
        self.detailed_data = []
        self.target_records = 1000
        self.start_time = datetime.now()
        self.unique_identifiers = set()  # Track URLs to avoid duplicates

    def _configure_options(self):
        self.options.add_argument('--start-maximized')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
                                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # Uncomment to run headless
        # self.options.add_argument('--headless')

    def _scroll_to_bottom(self):
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def _extract_restaurant_urls(self):
        try:
            cards = WebDriverWait(self.driver, 20).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "div.sc-1mo3ldo-0.sc-jGkVzM.BXbKf")
                )
            )
            logging.info(f"Found {len(cards)} restaurant cards on the page.")
            
            for card in cards:
                try:
                    # Extract restaurant URL
                    link_element = card.find_element(By.CSS_SELECTOR, "a.sc-hPeUyl.cKQNlu")
                    url = link_element.get_attribute('href')
                    if url in self.unique_identifiers:
                        continue
                    self.unique_identifiers.add(url)
                    self.restaurant_urls.append(url)
                    if len(self.restaurant_urls) >= self.target_records:
                        logging.info("Reached target number of URLs.")
                        return
                except (StaleElementReferenceException, NoSuchElementException) as e:
                    logging.debug(f"Skipping card: {str(e)}")
                    continue
        except Exception as e:
            logging.error(f"Extraction error: {str(e)}")

    def _load_full_page(self):
        scroll_attempts = 0
        max_scroll_attempts = 2
        
        while scroll_attempts < max_scroll_attempts:
            initial_count = len(self.restaurant_urls)
            self._scroll_to_bottom()
            self._extract_restaurant_urls()
                
            if len(self.restaurant_urls) >= self.target_records:
                logging.info("Collected desired amount of URLs.")
                break
                
            if len(self.restaurant_urls) == initial_count:
                scroll_attempts += 1
                logging.info(f"No new URLs found. Scroll attempt {scroll_attempts}/{max_scroll_attempts}")
                time.sleep(1)
            else:
                scroll_attempts = 0

    def _extract_restaurant_details(self, url):
        try:
            self.driver.get(url)
            # Wait for critical elements to load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.sc-7kepeu-0"))
            )
            # Extract details
            name = self.driver.find_element(By.CSS_SELECTOR, "h1.sc-7kepeu-0.sc-iSDuPN.fwzNdh").text.strip()
            location = self.driver.find_element(By.CSS_SELECTOR, "a.sc-clNaTc.vNCcy").text.strip()
            dining_rating = self.driver.find_element(By.CSS_SELECTOR, "div.sc-1q7bklc-1.cILgox").text.strip()
            dining_reviews = self.driver.find_element(By.CSS_SELECTOR, "div.sc-1q7bklc-8.kEgyiI").text.strip()
            cuisine = self.driver.find_element(By.CSS_SELECTOR, "div.sc-gVyKpa.fXdtVd").text.strip()
            price = self.driver.find_element(By.CSS_SELECTOR, "div.sc-bEjcJn.ePRRqr").text.strip()
            
            # Additional details (customize selectors as needed)
            address = self.driver.find_element(By.CSS_SELECTOR, "p.sc-bFADNz.gNdKCg").text.strip()
            phone = self.driver.find_element(By.CSS_SELECTOR, "a.sc-bFADNz.leEVAg").text.strip()

            return {
                'name': name,
                'location': location,
                'dining rating': dining_rating,
                'dining reviews': dining_reviews,
                'cuisine': cuisine,
                'price': price,
                'address': address,
                'phone': phone,
                'url': url
            }
        except Exception as e:
            logging.error(f"Error scraping {url}: {str(e)}")
            return None

    def _scrape_restaurant_details(self):
        for url in self.restaurant_urls[:self.target_records]:  # Limit to target
            data = self._extract_restaurant_details(url)
            if data:
                self.detailed_data.append(data)
                logging.info(f"Collected data for {data['name']}")
            # Throttle requests to avoid being blocked
            time.sleep(2)

    def run(self):
        try:
            logging.info("Collecting restaurant URLs...")
            self.driver.get("https://www.zomato.com/ncr/restaurants")
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.sc-1mo3ldo-0.sc-jGkVzM.BXbKf"))
            )
            self._load_full_page()
            
            logging.info(f"Collected {len(self.restaurant_urls)} URLs. Now scraping details...")
            self._scrape_restaurant_details()

            # Save data
            df = pd.DataFrame(self.detailed_data)
            if not df.empty:
                df.drop_duplicates(subset=['url'], inplace=True)
                # Merge with existing data
                try:
                    existing_df = pd.read_csv('zomato_data.csv')
                except FileNotFoundError:
                    existing_df = pd.DataFrame()
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_csv('zomato_data.csv', index=False)
                logging.info(f"Saved {len(combined_df)} records.")
            else:
                logging.info("No data collected.")
        except Exception as e:
            logging.error(f"Main error: {str(e)}")
        finally:
            self.driver.quit()

if __name__ == '__main__':
    scraper = ZomatoScraper()
    scraper.run()