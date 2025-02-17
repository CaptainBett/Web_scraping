from bs4 import BeautifulSoup
import requests

html_text = requests.get('https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&searchTextSrc=&searchTextText=&txtKeywords=python&txtLocation=').text

soup = BeautifulSoup(html_text, 'lxml')

jobs = soup.find_all('li', class_='clearfix job-bx wht-shd-bx')

for job in jobs:
    published_at = job.find('span', class_='sim-posted').get_text(strip=True)
    if 'few' in published_at:
        company_name = job.find('h3', class_='joblist-comp-name').get_text(strip=True)
        skills = job.find('span', class_='srp-skills').get_text(strip=True) if job.find('span', class_='srp-skills') else "Not Specified"        

        print()
        print(f'Company name: {company_name}')
        print(f'Skills required: {skills}')
        print(f'Posted at: {published_at}')
        
    print('TOO OLD!')