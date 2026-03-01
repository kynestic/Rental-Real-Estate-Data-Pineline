from bs4 import BeautifulSoup
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

def getNumberofPage():
    response = requests.get('https://muaban.net/bat-dong-san/cho-thue-nha-dat', impersonate = 'chrome')
    soup = BeautifulSoup(response.text, 'html.parser')
    
    page_count = 3500

    item_count = int(re.sub(r'[^0-9]', '', soup.find('div', class_='sc-1b0gpch-8 gxVBYb').text))
    
    url_per_page = len(soup.find_all('a', class_="over"))

    if item_count and url_per_page:
        page_count = int(item_count/url_per_page) + 1

    return page_count, response.status_code

def getURL(page):
    url_list = []
    response = None
    status_code = None
    soup = None
    url_request = f"https://muaban.net/bat-dong-san/cho-thue-nha-dat?page={page}"
    error_msg = ""
    try:
        response = requests.get(url_request, impersonate='chrome')
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', class_="over")
        status_code = response.status_code

        if len(links) == 0:
            return url_list, status_code, str(soup), url_request, 'muaban.net', str(error_msg)

        if response.status_code == 403:
            return url_list, status_code, str(soup), url_request, 'muaban.net', str(error_msg)

        for link in links:
            href = str(link.get('href'))
            if href.startswith('/'):
                href = 'https://muaban.net' + href
            url_list.append(href)
    except Exception as e:
        error_msg = e
        print("Có lỗi xảy ra khi lấy URL của trang ", page)
        print("Nội dung lỗi: ", e)
    
    return url_list, status_code, str(soup), url_request, 'muaban.net', str(error_msg)


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

