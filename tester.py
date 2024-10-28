import os
from requests_html import HTMLSession
from bs4 import BeautifulSoup

session = HTMLSession()

# url = "https://www.airbnb.co.za/?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&search_mode=flex_destinations_search&flexible_trip_lengths%5B%5D=one_week&location_search=MIN_MAP_BOUNDS&monthly_start_date=2024-08-01&monthly_length=3&monthly_end_date=2024-11-01&price_filter_input_type=0&channel=EXPLORE&category_tag=Tag%3A8538&search_type=category_change"

# response = session.get(url)
# response.html.render()  # This executes the JavaScript

# soup = BeautifulSoup(response.html.html, 'html.parser')
secret_str = os.getenv("SECRET_STR")
url = secret_str
response = session.get(url)
html_content = BeautifulSoup(response.content, 'html.parser')

print("Code running..\n",html_content)
