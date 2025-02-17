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
from selenium.common.exceptions import StaleElementReferenceException

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
        self.collected_data = set()
        self.target_records = 2000
        self.start_time = datetime.now()
        self.unique_identifiers = set()

    def _configure_options(self):
        self.options.add_argument('--start-maximized')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])

    def _scroll_to_bottom(self):
        """Smooth scroll to trigger lazy loading"""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def _extract_restaurant_data(self):
        current_count = 0
        try:
            cards = WebDriverWait(self.driver, 20).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "div.sc-evWYkj.cRThYq")
                )
            )
            
            for card in cards:
                try:
                    # Get unique identifier
                    name = card.find_element(By.CSS_SELECTOR, "h4.sc-1hp8d8a-0.sc-Ehqfj.bxOQva").text.strip()
                    location = card.find_element(By.CSS_SELECTOR, "p.sc-1hez2tp-0 sc-cyQzhP uIMEk").text.strip()
                    uid = f"{name}-{location}"
                    
                    if uid in self.unique_identifiers:
                        continue
                        
                    rating = card.find_element(By.CSS_SELECTOR, "div.sc-1q7bklc-1.cILgox").text.strip()
                    cuisine = card.find_element(By.CSS_SELECTOR, "p.sc-1hez2tp-0.sc-gggouf.fSxdnq").text.strip()
                    price = card.find_element(By.CSS_SELECTOR, "p.sc-1hez2tp-0.sc-gggouf.KXcjT").text.strip()

                    # Validate all fields
                    if not all([name, location, rating, cuisine, price]):
                        continue

                    self.unique_identifiers.add(uid)
                    yield {
                        'name': name,
                        'location': location,
                        'rating': rating,
                        'cuisine': cuisine,
                        'price': price,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    current_count += 1
                    
                    if len(self.unique_identifiers) >= self.target_records:
                        return

                except (StaleElementReferenceException, Exception) as e:
                    logging.debug(f"Skipping card: {str(e)}")
                    continue

        except Exception as e:
            logging.error(f"Extraction error: {str(e)}")
        
        return current_count

    def _load_full_page(self):
        """Handle infinite scroll with timeout"""
        scroll_attempts = 0
        max_scroll_attempts = 50
        
        while scroll_attempts < max_scroll_attempts:
            initial_count = len(self.unique_identifiers)
            self._scroll_to_bottom()
            
            # Process newly loaded cards
            for data in self._extract_restaurant_data():
                self.collected_data.add(frozenset(data.items()))  # Prevent duplicates
                
            if len(self.collected_data) >= self.target_records:
                break
                
            if len(self.unique_identifiers) == initial_count:
                scroll_attempts += 1
                time.sleep(1)
            else:
                scroll_attempts = 0

    def _calculate_progress(self):
        elapsed = datetime.now() - self.start_time
        progress = len(self.collected_data) / self.target_records
        return {
            'elapsed': elapsed,
            'progress': progress,
            'remaining': (elapsed / progress) - elapsed if progress > 0 else 0
        }

    def run(self):
        try:
            self.driver.get("https://www.zomato.com/ncr/restaurants")
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.sc-1mo3ldo-0.sc-jGkVzM.BXbKf"))
            )
            
            self._load_full_page()
            
            # Convert to list of dicts
            final_data = [dict(item) for item in self.collected_data]
            
            # Save data
            df = pd.DataFrame(final_data)
            df.drop_duplicates(subset=['name', 'location'], inplace=True)
            df.to_csv('zomato_data.csv', index=False)
            logging.info(f"Successfully saved {len(df)} records")

        except Exception as e:
            logging.error(f"Main execution error: {str(e)}")
        finally:
            self.driver.quit()

if __name__ == '__main__':
    scraper = ZomatoScraper()
    scraper.run()