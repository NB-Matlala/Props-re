import aiohttp
import asyncio
import re
from bs4 import BeautifulSoup
import json
import random
import csv
import math
from datetime import datetime
from azure.storage.blob import BlobClient
import os

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")
con_str_coms = os.getenv("CON_STR_COMS")

ids = []
async def fetch(session, url, semaphore):
    async with semaphore:
        async with session.get(url) as response:
            return await response.text()

######################################Functions##########################################################
def getPages(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
        num_pgV = num_pg.text.split('of ')[-1]
        num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
        return 0


def getIds(soup):
    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)
    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        prop_ID = None

    return prop_ID

def extractor(soup, url): # extracts from created urls
    try:
        prop_ID = None
        erfSize = None
        floor_size = None
        rates = None
        levy = None

        prop_div = soup.find('div', class_='property-features')
        lists = prop_div.find('ul', class_='property-features__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#listing-alt' in icon:
                prop_ID = feature.find('span',class_='property-features__value').text.strip()

            elif '#property-type' in icon:
                prop_type = feature.find('span',class_='property-features__value').text.strip()

            elif '#erf-size' in icon:
                erfSize = feature.find('span',class_='property-features__value').text.strip()
                erfSize = erfSize.replace('\xa0', ' ')

            elif '#property-size' in icon:
                floor_size = feature.find('span',class_='property-features__value').text.strip()
                floor_size = floor_size.replace('\xa0', ' ')

            elif '#rates' in icon:
                rates = feature.find('span',class_='property-features__value').text.strip()
                rates = rates.replace('\xa0', ' ')

            elif '#levies' in icon:
                levy = feature.find('span',class_='property-features__value').text.strip()
                levy = levy.replace('\xa0', ' ')

    except KeyError:
        prop_ID = None
        erfSize = None
        prop_type = None
        floor_size = None
        rates = None
        levy = None

    beds = None
    baths = None
    lounge = None
    dining = None
    garage = None
    parking = None
    storeys = None
    open_park = None

    try:
        prop_feat_div = soup.find('div', id='property-features-list')
        lists_feat = prop_feat_div.find('ul', class_='property-features__list')
        feats = lists_feat.find_all('li')
        for feat in feats:
            feat_icon = feat.find('svg').find('use').get('xlink:href')
            if '#bedrooms' in feat_icon:
                beds = feat.find('span',class_='property-features__value').text.strip()
            elif '#bathroom' in feat_icon:
                baths = feat.find('span',class_='property-features__value').text.strip()
            elif '#lounges' in feat_icon:
                lounge = feat.find('span',class_='property-features__value').text.strip()
            elif '#dining' in feat_icon:
                dining = feat.find('span',class_='property-features__value').text.strip()
            elif '#garages' in feat_icon:
                garage = feat.find('span',class_='property-features__value').text.strip()
            elif '#covered-parkiung' in feat_icon:
                parking = feat.find('span',class_='property-features__value').text.strip()
            elif '#parking-spaces' in feat_icon:
                open_park = feat.find('span',class_='property-features__value').text.strip()            
            elif '#storeys' in feat_icon:
                storeys = feat.find('span',class_='property-features__value').text.strip()

    except (AttributeError, KeyError) as f:
        print(f"Property Features Not Found: for {url}")
        beds = None
        baths = None
        lounge = None
        dining = None
        garage = None
        parking = None
        storeys = None
        open_park = None

    agent_name = None
    agent_url = None

    try:
        script_tag = soup.find('script', string=re.compile(r'const serverVariables'))
        if script_tag:
            script_content = script_tag.string
            script_data2 = re.search(r'const serverVariables\s*=\s*({.*?});', script_content, re.DOTALL).group(1)
            json_data = json.loads(script_data2)
            try:
                agent_name = json_data['bundleParams']['agencyInfo']['agencyName']
                agent_url = json_data['bundleParams']['agencyInfo']['agencyPageUrl']
                agent_url = f"{base_url}{agent_url}"
            except :
                agent_name = "Private Seller"
                agent_url = None
    except (AttributeError, KeyError) as e:
        agent_name = None
        agent_url = None

    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Erf Size": erfSize, "Property Type": prop_type, "Floor Size": floor_size,
        "Rates and taxes": rates, "Levies": levy, "Bedrooms": beds, "Bathrooms": baths, "Lounges": lounge,
        "Dining": dining, "Garages": garage, "Covered Parking": parking, "Storeys": storeys, "Open Parkings": open_park, "Agent Name": agent_name,
        "Agent Url": agent_url, "Time_stamp": current_datetime}

######################################Functions##########################################################
async def main():
    fieldnames = ['Listing ID', 'Erf Size', 'Property Type', 'Floor Size', 'Rates and taxes', 'Levies',
                  'Bedrooms', 'Bathrooms', 'Lounges', 'Dining', 'Garages', 'Covered Parking', 'Storeys', 'Open Parkings',
                  'Agent Name', 'Agent Url', 'Time_stamp']
    filename = "PrivateRentals(Inside)3.csv"
    
    semaphore = asyncio.Semaphore(500)

    async with aiohttp.ClientSession() as session:
        with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            async def process_province(prov):
                response_text = await fetch(session, f"{base_url}/to-rent/mpumalanga/{prov}", semaphore)
                home_page = BeautifulSoup(response_text, 'html.parser')

                links = []
                ul = home_page.find('ul', class_='region-content-holder__unordered-list')
                li_items = ul.find_all('li')
                for area in li_items:
                    link = area.find('a')
                    link = f"{base_url}{link.get('href')}"
                    links.append(link)

                new_links = []
                for l in links:
                    try:
                        res_in_text = await fetch(session, f"{l}", semaphore)
                        inner = BeautifulSoup(res_in_text, 'html.parser')
                        ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
                        if ul2:
                            li_items2 = ul2.find_all('li', class_='region-content-holder__list')
                            for area2 in li_items2:
                                link2 = area2.find('a')
                                link2 = f"{base_url}{link2.get('href')}"
                                new_links.append(link2)
                        else:
                            new_links.append(l)
                    except aiohttp.ClientError as e:
                        print(f"Request failed for {l}: {e}")

                async def process_link(x):
                    try:
                        x_response_text = await fetch(session, x, semaphore)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}?page={s}", semaphore)
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = getIds(prop)
                                ids.append(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                tasks = [process_link(x) for x in new_links]
                await asyncio.gather(*tasks)

            async def process_ids():
                count = 0

                async def process_id(list_id):
                    nonlocal count
                    count += 1
                    if count % 1000 == 0:
                        print(f"Processed {count} IDs, sleeping for 20 seconds...")
                        await asyncio.sleep(55)
                    list_url = f"{base_url}/to-rent/something/something/something/{list_id}"
                    try:
                        listing = await fetch(session, list_url, semaphore)
                        list_page = BeautifulSoup(listing, 'html.parser')
                        data = extractor(list_page, list_url)
                        writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing ID {list_id}: {e}")

                tasks = [process_id(list_id) for list_id in ids]
                await asyncio.gather(*tasks)

            await asyncio.gather(*(process_province(prov) for prov in range(4, 5)))
            await process_ids()
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")

    connection_string = f"{con_str}"
    container_name = "privatepropre"
    blob_name = "PrivateRentals(Inside)3.csv"

    blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name)
    
    with open(filename, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f"File uploaded to Azure Blob Storage: {blob_name}")

# Running the main coroutine
asyncio.run(main())

#################################################################################################################################################

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
from azure.storage.blob import BlobClient

session = HTMLSession()

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
        prop_div = soup.find('div', class_='property-features')
        lists = prop_div.find('ul', class_='property-features__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#listing-alt' in icon:
                prop_ID = feature.find('span', class_='property-features__value').text.strip()
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



def extractor_pics(soup, prop_id): # extracts from created urls
    try:
        photo_div = soup.find('div', class_='details-page-photogrid__photos')
        photo_data = []
        img_links = photo_div.find_all('img')
        count = 0
        for url in img_links:
            count += 1
            photo_data.append({'Listing_ID': prop_id, 'Photo_Link': url.get('src')})
            if count == 8:
                break
        return photo_data        
    except KeyError:
        print('Pictures not found')
        return []

# def getIds(soup):
#     try:
#         script_data = soup.find('script', type='application/ld+json').string
#         json_data = json.loads(script_data)
#         url = json_data['url']
#         prop_ID_match = re.search(r'/([^/]+)$', url)
#         if prop_ID_match:
#             return prop_ID_match.group(1)
#     except Exception as e:
#         print(f"Error extracting ID from {soup}: {e}")
#     return None

fieldnames = ['Listing ID', 'Description', 'Latitude', 'Longitude', 'Time_stamp']
filename = "PrivRentComments3.csv"

fieldnames_pics = ['Listing_ID', 'Photo_Link']
filename_pics = "PrivRentPictures3.csv"

# Initialize thread queue and results list
queue = Queue()
results = []
pic_results = []

  
for list in ids:
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

# Upload to Azure Blob Storage
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
