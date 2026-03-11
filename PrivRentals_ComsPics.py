#################################################################################################################################################################################
#  --- Rentals Comments & Pictures ---
#################################################################################################################################################################################
print(f"Starting with Comments & Pics Extraction:\n")
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import re
import math
import json
import time
import threading
from queue import Queue
from datetime import datetime
import csv
import gzip
from azure.storage.blob import BlobClient
import os

session = HTMLSession()

list_ids = []

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")
con_str_coms = os.getenv("CON_STR_COMS")

def getIds(soup):
    try:
        url = soup['href']

        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            return prop_ID_match.group(1)
    except Exception as e:
        print(f"Error extracting ID from {soup}: {e}")
    return None

def getPages(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='sort-and-listing-count')
        # ('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
        num_pgV = num_pg.text.split('of ')[-1]
        num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
        return 0
    

prop_types = ['houses-to-rent', 'apartments-to-rent', 'townhouses-to-rent','land-to-rent', 'garden-cottages-to-rent']
# prop_types = ['land-to-rent', 'garden-cottages-to-rent']


for pt in prop_types:
    x = f"{base_url}/{pt}/south-africa/1?"
    try:
        land = session.get(x)
        land_html = BeautifulSoup(land.content, 'html.parser')
        pgs = getPages(land_html, x)

        for p in range(1, pgs + 1):
            url = f"{x}page={p}"
            pg_request = session.get(url)
            pg_html = BeautifulSoup(pg_request.content, 'html.parser')
            ###
            prop_contain = pg_html.find_all('a', class_='featured-listing')
            prop_contain.extend(pg_html.find_all('a', class_='listing-result'))
            for x_page in prop_contain:
                prop_id = getIds(x_page)
                if prop_id:
                    list_ids.append(prop_id)
    
    except Exception as e:
        print(f"Failed to process URL {x}: {e}")


# Thread worker function
def worker(queue, results, pic_results):
    while True:
        item = queue.get()
        if item is None:
            break
        url = item.get("url")
        extract_function = item.get("extract_function")
        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            print(f"[Response Status: {response.status_code}]\n")

            result = extract_function(soup, url)
            if result:
                if extract_function == extractor_com:
                    results.append(result)
                elif extract_function == extractor_pics:
                    pic_results.extend(result)
        except Exception as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()

def extractor_com(soup, url):
    try:
        prop_ID = None
        prop_div = soup.find('div', class_='property-details')
        lists = prop_div.find('ul', class_='property-details__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('href')
            if '#listing-alt' in icon:
                prop_ID = feature.find('span', class_='property-details__value').text.strip()
    except KeyError:
        prop_ID = None
    
    prop_desc = None
    latitude = None
    longitude = None
    
    try:
        comment_div = soup.find('div', class_='listing-description__text')
        prop_desc = comment_div.text.strip()
    except:
        print('Error. Cannot find comments')
    
    current_datetime = datetime.now().strftime('%Y-%m-%d')
    
    return {"Listing ID": prop_ID, "Description": prop_desc, "Latitude": latitude, "Longitude": longitude,"Time_stamp": current_datetime}



def extractor_pics(soup, prop_ID): # extracts from created urls
    try:
        prop_ID = None
        prop_div = soup.find('div', class_='property-details')
        lists = prop_div.find('ul', class_='property-details__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('href')
            if '#listing-alt' in icon:
                prop_ID = feature.find('span', class_='property-details__value').text.strip()
    except KeyError:
        prop_ID = None
    list_id = prop_ID
    
    try:
        photo_div = soup.find('div', class_='details-page-photogrid__photos')
        photo_data = []
        img_links = photo_div.find_all('img')
        count = 0
        for url in img_links:
            count += 1
            photo_data.append({'Listing_ID': list_id, 'Photo_Link': url.get('src')})
            if count == 8:
                break
        return photo_data        
    except KeyError:
        print('Pictures not found')
        return []

fieldnames = ['Listing ID', 'Description', 'Latitude', 'Longitude', 'Time_stamp']
filename = "PrivRentComments.csv"

fieldnames_pics = ['Listing_ID', 'Photo_Link']
filename_pics = "PrivRentPictures.csv"

# Initialize thread queue and results list
queue = Queue()
results = []
pic_results = []

  
for list in list_ids:
    try:
        list_url = f"{base_url}//to-rent/something/something/something/{list}"
        queue.put({"url": list_url, "extract_function": extractor_com})
        queue.put({"url": list_url, "extract_function": extractor_pics})
    except Exception as e:
        print(f"Failed to process URL {list_url}: {e}")

# Start threads
num_threads = 10  
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results, pic_results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()

# Write results to CSV files
with open(filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

with open(filename_pics, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames_pics)
    writer.writeheader()
    writer.writerows(pic_results)

## Upload to Azure Blob Storage
blob_connection_string = f"{con_str_coms}"
blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="comments-pics",
    blob_name=filename
)
with open(filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

blob_pics = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="comments-pics",
    blob_name=filename_pics
)
with open(filename_pics, "rb") as data:
    blob_pics.upload_blob(data, overwrite=True)
