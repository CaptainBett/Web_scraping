import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os

# Random user agents list
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
]

def get_random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US, en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

def scrape_ebay_data(url):
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    data = []
    next_url = None

    try:
        headers = get_random_headers()
        print(f"Fetching page: {url}")
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        if not soup.title or "eBay" not in soup.title.text:
            return [], None

        product_containers = soup.select(".s-item__wrapper")
        print(f"Found {len(product_containers)} products")

        for container in product_containers:
            product = {
                'PRODUCT NAME': None,
                'PRICE': None,
                'CONDITION': None,
                'SELLER': None,
                'SELLER RATING': None,
                'RATING COUNT': None,
                'SELLER LOCATION': None,
                'URL': None,
            }

            # Clean the product name (remove "New Listing" prefix)
            try:
                name = container.select_one(".s-item__title").text.strip()
                product['PRODUCT NAME'] = re.sub(r'^New Listing\s*', '', name)
            except AttributeError:
                continue

            try:
                price = container.select_one(".s-item__price").text.strip()
                product['PRICE'] = price
            except AttributeError:
                continue

            try:
                condition = container.select_one(".SECONDARY_INFO").text.strip()
                product['CONDITION'] = condition
            except AttributeError:
                pass

            # Extract seller info (name, rating count, rating percentage) using regex
            try:
                seller_info = container.select_one(".s-item__seller-info-text")
                if seller_info:
                    seller_text = seller_info.text.strip()
                    # Expected format: "sellerName (ratingCount) rating%"
                    match = re.search(r'(.+?)\s*\((\d+)\)\s*(\S+)', seller_text)
                    if match:
                        product['SELLER'] = match.group(1).strip()
                        product['RATING COUNT'] = match.group(2).strip()
                        product['SELLER RATING'] = match.group(3).strip()
                    else:
                        product['SELLER'] = seller_text
                        product['RATING COUNT'] = "N/A"
                        product['SELLER RATING'] = "N/A"
                else:
                    product['SELLER'] = "N/A"
                    product['RATING COUNT'] = "N/A"
                    product['SELLER RATING'] = "N/A"
            except AttributeError:
                product['SELLER'] = "N/A"
                product['RATING COUNT'] = "N/A"
                product['SELLER RATING'] = "N/A"

            try:
                seller_location = container.select_one(".s-item__location.s-item__itemLocation").text.strip()
                product['SELLER LOCATION'] = seller_location
            except AttributeError:
                pass

            try:
                link_element = container.select_one(".s-item__link")
                if link_element and link_element.has_attr("href"):
                    product['URL'] = link_element["href"].split("?")[0]
            except AttributeError:
                pass

            if all(value not in [None, "", "N/A"] for value in product.values()):
                            data.append(product)

        next_page_link = soup.find('a', {'class': 'pagination__next'})
        if next_page_link and next_page_link.has_attr('href'):
            next_url = urljoin("https://www.ebay.com", next_page_link['href'])

    except Exception as e:
        print(f"Error: {str(e)}")
        return [], None

    return data, next_url

if __name__ == "__main__":
    output_folder = 'ebay_scrape'
    os.makedirs(output_folder, exist_ok=True)
    csv_filename = os.path.join(output_folder, 'ebay_data.csv')
    request_delay = (5, 10)
    max_pages_per_query = 100
    target_records = 5000

    if os.path.exists(csv_filename):
        existing_df = pd.read_csv(csv_filename)
        existing_count = len(existing_df)
        print(f"Found existing file with {existing_count} records")
    else:
        existing_df = pd.DataFrame()
        existing_count = 0

    target_remaining = max(target_records - existing_count, 0)
    
    if target_remaining == 0:
        print(f"Already have {existing_count} records. Target achieved.")
        exit()

    all_data = []
    collected_new = 0
    current_url = "https://www.ebay.com/sch/i.html?_nkw=electronics&_sacat=0&_pgn=1"
    page_count = 0

    while current_url and page_count < max_pages_per_query and collected_new < target_remaining:
        page_count += 1
        scraped_data, next_url = scrape_ebay_data(current_url)
        
        if scraped_data:
            clean_batch = pd.DataFrame(scraped_data)
            clean_batch = clean_batch.drop_duplicates(
                subset=['PRODUCT NAME', 'PRICE'],
                keep='first'
            )
            
            if not clean_batch.empty:
                if not existing_df.empty:
                    mask = ~clean_batch['PRODUCT NAME'].isin(existing_df['PRODUCT NAME']) & \
                           ~clean_batch['PRICE'].isin(existing_df['PRICE'])
                    clean_batch = clean_batch[mask]

                remaining_needed = target_remaining - collected_new
                clean_batch = clean_batch.head(remaining_needed)
                
                if not clean_batch.empty:
                    all_data.append(clean_batch)
                    clean_batch.to_csv(csv_filename, mode='a', index=False, header=not os.path.exists(csv_filename))
                    collected_new += len(clean_batch)
                    print(f"Page: {page_count}")
                    print(f"New clean records: {len(clean_batch)} | Total collected: {collected_new}")

        current_url = next_url if next_url and collected_new < target_remaining else None
        if current_url:
            sleep_time = random.uniform(*request_delay)
            time.sleep(sleep_time)

    if all_data:
        new_df = pd.concat(all_data)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        final_df = combined_df.drop_duplicates(
            subset=['PRODUCT NAME', 'PRICE'],
            keep='first'
        ).head(target_records)
        
        final_df.to_csv(csv_filename, index=False)
        print(f"\nTotal records after deduplication: {len(final_df)}")
        print(f"Data successfully saved to {csv_filename}")
    else:
        print("No new data collected")

    print("\nScraping session completed")
