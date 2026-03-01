from bs4 import BeautifulSoup
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

def getNumberofPage():
    page_count = 2500
    status_code = None
    try:
        response = requests.get('https://batdongsan.com.vn/nha-dat-cho-thue', impersonate = 'chrome')
        soup = BeautifulSoup(response.text, 'html.parser')
        all_page = soup.find_all('a', 're__pagination-number')
        page_count = int(re.sub(r'[^0-9]', '', all_page[-1].text))

        if not isinstance(page_count, int):
            property_count = soup.find('span', id='count-number')
            property_count = int(re.sub(r'[^0-9]', '', property_count.text))

            property_per_page = soup.find_all('div', class_='js__card')
            property_per_page = len(property_per_page)
            print(property_per_page)
            page_count = int(property_count/property_per_page) + 1
    except Exception as e:
        print('Error! ', e)
    
    return page_count, status_code

def getURL(page):
    url_list = []
    response = None
    soup = None
    url_request = f"https://batdongsan.com.vn/nha-dat-cho-thue/p{page}"
    error_msg = ""
    status_code = ""
    try:
        response = requests.get(url_request, impersonate='chrome')
        status_code = response.status_code
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', class_="js__product-link-for-product-id")

        if len(links) == 0:
            return url_list, response.status_code, str(soup), url_request, 'batdongsan.com.vn', ''

        if response.status_code == 403:
            return url_list, response.status_code, str(soup), url_request, 'batdongsan.com.vn', ''

        for link in links:
            href = str(link.get('href'))
            if href.startswith('/'):
                href = 'https://batdongsan.com.vn' + href
            url_list.append(href)
    except Exception as e:
        print("Error happened when retrieve page: ", page)
        print("Error Message: ", e)
        error_msg = e
        print(error_msg)
    
    return url_list, status_code, str(soup), url_request, 'batdongsan.com.vn', str(error_msg)


def getData(url):
    soup = ""
    status_code = ""
    try:
        response = requests.get(url, impersonate = 'chrome')
        if response.status_code:
            status_code = response.status_code
        if status_code != 200:
            return str(soup), status_code, ""
        else:
            soup = BeautifulSoup(response.text, 'html.parser')
            return str(soup), status_code, ""
    except Exception as e:
        return soup, status_code, str(e) 


def getDataList(url_list, max_workers=20):
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        future_map = {executor.submit(getData, url): url for url in url_list}

        for f in as_completed(future_map):
            url = future_map[f]                # <--- Lấy đúng URL
            html, status = f.result()
            results.append({
                'html': html,
                'status': status,
                'url': url
            })

    return results

