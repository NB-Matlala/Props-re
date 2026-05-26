import os
from requests_html import HTMLSession
from bs4 import BeautifulSoup

session = HTMLSession()

def get_web_total():
    import re
    retry = 0
    web_total = None

    while retry < 5:
        try:
            res = session.get('https://www.property24.com/for-sale/advanced-search/results?sp=pid%3d1%2c5%2c6%2c9%2c7%2c8%2c2%2c3%2c14%26so%3dNewest')
            soup = BeautifulSoup(res.content, 'html.parser')

            tot_div = soup.find('div', class_ = 'panel-body').text
            total_list = tot_div.split('of ')[1]
            total_list = re.sub(r"[\s\xa0]", "", total_list)

            web_total = int(total_list)
            print(f"{web_total} found")
            break

        except Exception as e:
            retry += 1
            print(f"[Total Listings Error]: {e}\nAttempt ({retry})")
            time.sleep(2)
