
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

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")
con_str_coms = os.getenv("CON_STR_COMS")

session = HTMLSession()

# # Thread worker function
def worker(queue, results):
    while True:
        item = queue.get()
        if item is None:
            break
        url = item.get("url")
        extract_function = item.get("extract_function")
        pt = item.get("prop_type")
        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            result = extract_function(soup, url, pt)
            
            print(f"[Response Status: {response.status_code}]\n")
            
            if result:
                results.extend(result)
        except Exception as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()

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

def extractor(soup, url, pt):
    prop_contain = soup.find_all('a', class_='listing-result')
    prop_contain.extend(soup.find_all('a', class_='featured-listing'))
    property_type = None
    
    if pt == 'houses-to-rent':
        property_type = 'House'
    elif pt == 'apartments-to-rent':
        property_type = 'Apartment'
    elif pt == 'townhouses-to-rent':
        property_type = 'Townhouse'
    elif pt == 'land-to-rent':
        property_type = 'Land'
    elif pt == 'garden-cottages-to-rent':
        property_type = 'Garden Cottage'

    data = []   
    for x_page in prop_contain:

        try:
            link = x_page['href']

            prop_ID_match = re.search(r'/([^/]+)$', link)
            if prop_ID_match:
                prop_id = prop_ID_match.group(1)

                # url = f"{url}{prop_id}"
        except Exception as e:
            print(f"Error extracting ID from {e}")    
        
        
        try:
            agent_name = None
            agent_url = None
            agent_div = x_page.find('div', class_='listing-result__advertiser txt-small-regular')
            title = x_page.get('title')

            if agent_div:
                try:
                    agent_detail = agent_div.find('img', class_='listing-result__logo')
                    agent_name = agent_detail.get('alt')
                    agent_url = agent_detail.get('src')
                    agent_id_match = re.search(r'offices/(\d+)', agent_url)
                    if agent_id_match:
                        agent_id = agent_id_match.group(1)
                        agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
                except:
                    agent_name = "Private Seller"
                    agent_url = None
        except Exception as e:
            agent_name = None
            agent_url = None
            print(f"Something went wrong with getting details: {e}")
        current_datetime = datetime.now().strftime('%Y-%m-%d')
        
        
        data.append({
            "Listing ID": prop_id,
            "Title": title,
            "Property Type": property_type,
            "URL": f"{base_url}{link}",
            "Agent Name": agent_name,
            "Agent Url": agent_url,
            "Time_stamp": current_datetime
        })

    return data


# Initialize thread queue and results list
queue = Queue()
results = []

prop_types = ['houses-to-rent', 'apartments-to-rent', 'townhouses-to-rent','land-to-rent', 'garden-cottages-to-rent']

for pt in prop_types:
    x = f"{base_url}/{pt}/south-africa/1?"
    land = session.get(x)
    land_html = BeautifulSoup(land.content, 'html.parser')
    pgs = getPages(land_html, x)

    for p in range(1, pgs + 1):
        url = f"{x}page={p}"

        # pg_request = session.get(url)
        # pg_html = BeautifulSoup(pg_request.content, 'html.parser')
        # print(extractor(pg_html, url, pt))

        queue.put({"url": url, "extract_function": extractor, "prop_type": pt})



# # Start threads
num_threads = 10  
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()

# # Write results to CSV
# csv_filename = 'PrivateRental.csv'
# with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
#     fieldnames = results[0].keys() if results else []
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#     writer.writeheader()
#     for result in results:
#         writer.writerow(result)

# # Write to gzip format
gz_filename = "PrivateRental.csv.gz"
with gzip.open(gz_filename, "wt", newline="", encoding="utf-8") as gzfile:
    fieldnames = results[0].keys() if results else []
    writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

# ###
# ### Upload to Azure Blob Storage
blob_connection_string = f"{con_str}"
blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="privatepropre",
    blob_name = gz_filename     #csv_filename
)
with open(gz_filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

print("CSV file uploaded to Azure Blob Storage.")

################################################################################################################################################################################################################################################
# -- Rental Agents
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

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")
con_str_coms = os.getenv("CON_STR_COMS")

session = HTMLSession()

# Thread worker function
def worker(queue, results):
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
                results.append(result)
        except Exception as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()

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

def extractor(soup, url):
    # Initialize variables with default values
    agent_name =  None
    agent_url = url

    agent_name = soup.find('h1').text
 
    return {"Agent Name": agent_name, "Agent Url": agent_url}

def getDealers(soup):
    try:
        agent_name = None
        agent_url = None
        agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

        if agent_div:
            try:
                agent_detail = agent_div.find('img', class_='listing-result__logo')
                agent_name = agent_detail.get('alt')
                agent_url = agent_detail.get('src')
                agent_id_match = re.search(r'offices/(\d+)', agent_url)
                if agent_id_match:
                    agent_id = agent_id_match.group(1)
                    agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
            except:
                agent_name = "Private Seller"
                agent_url = None
    except KeyError:
        agent_name = None
        agent_url = None

    return agent_url

# Initialize thread queue and results list
queue = Queue()
results = []

prop_types = ['houses-to-rent', 'apartments-to-rent', 'townhouses-to-rent','land-to-rent', 'garden-cottages-to-rent']

for pt in prop_types:
    x = f"{base_url}/{pt}/south-africa/1?"
    land = session.get(x)
    land_html = BeautifulSoup(land.content, 'html.parser')
    pgs = getPages(land_html, x)
    for p in range(1, pgs + 1):
        home_page = session.get(f"{x}page={p}")
        soup = BeautifulSoup(home_page.content, 'html.parser')
        prop_contain = soup.find_all('a', class_='listing-result')
        for x_page in prop_contain:
            prop = getDealers(x_page)
            if prop is not None:
                Deal_url = prop
                queue.put({"url": Deal_url, "extract_function": extractor})

# Start threads
num_threads = 10 
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()

# Write results to CSV
gz_filename = 'PrivatePropRentDealer_part1.csv.gz'
with gzip.open(gz_filename, 'wt', newline='', encoding='utf-8') as csvfile:
    fieldnames = results[0].keys() if results else []
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

### Upload to Azure Blob Storage
blob_connection_string = f"{con_str}"
blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="privatepropre",
    blob_name=gz_filename
)
with open(gz_filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

print("CSV file uploaded to Azure Blob Storage.")


################################################################################################################################################################################################################################################
print("Now Running Commercial Rentals.\n\n")

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

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")
con_str_coms = os.getenv("CON_STR_COMS")

session = HTMLSession()

# # Thread worker function
def worker(queue, results):
    while True:
        item = queue.get()
        if item is None:
            break
        url = item.get("url")
        extract_function = item.get("extract_function")
        pt = item.get("prop_type")
        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            result = extract_function(soup, url, pt)
            
            print(f"[Response Status: {response.status_code}]\n")
            
            if result:
                results.extend(result)
        except Exception as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()

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

def extractor(soup, url, pt):
    prop_contain = soup.find_all('a', class_='listing-result')
    prop_contain.extend(soup.find_all('a', class_='featured-listing'))
    property_type = None
    
    if pt == 'Commercial':
        property_type = 'Commercial'
    elif pt == 'Hospitality':
        property_type = 'Hospitality'
    elif pt == 'Industrial':
        property_type = 'Industrial'
    elif pt == 'Office':
        property_type = 'Office'
    elif pt == 'Retail':
        property_type = 'Retail'
    elif pt == 'VacantLandPlot':
        property_type = 'Land'


    data = []   
    for x_page in prop_contain:

        try:
            link = x_page['href']

            prop_ID_match = re.search(r'/([^/]+)$', link)
            if prop_ID_match:
                prop_id = prop_ID_match.group(1)

                # url = f"{url}{prop_id}"
        except Exception as e:
            print(f"Error extracting ID from {e}")    
        
        
        try:
            agent_name = None
            agent_url = None
            agent_div = x_page.find('div', class_='listing-result__advertiser txt-small-regular')
            title = x_page.get('title')

            if agent_div:
                try:
                    agent_detail = agent_div.find('img', class_='listing-result__logo')
                    agent_name = agent_detail.get('alt')
                    agent_url = agent_detail.get('src')
                    agent_id_match = re.search(r'offices/(\d+)', agent_url)
                    if agent_id_match:
                        agent_id = agent_id_match.group(1)
                        agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
                except:
                    agent_name = "Private Seller"
                    agent_url = None
        except Exception as e:
            agent_name = None
            agent_url = None
            print(f"Something went wrong with getting details: {e}")
        current_datetime = datetime.now().strftime('%Y-%m-%d')
        
        
        data.append({
            "Listing ID": prop_id,
            "Title": title,
            "Property Type": property_type,
            "URL": f"{base_url}{link}",
            "Agent Name": agent_name,
            "Agent Url": agent_url,
            "Time_stamp": current_datetime
        })

    return data


# Initialize thread queue and results list
queue = Queue()
results = []

prop_types = ['Commercial', 'Hospitality', 'Industrial','Office', 'Retail', 'VacantLandPlot']

for pt in prop_types:
    x = f"{base_url}/commercial-rentals/south-africa/1?pt={pt}"
    land = session.get(x)
    land_html = BeautifulSoup(land.content, 'html.parser')
    pgs = getPages(land_html, x)

    for p in range(1, pgs + 1):
        url = f"{x}&page={p}"

        # pg_request = session.get(url)
        # pg_html = BeautifulSoup(pg_request.content, 'html.parser')
        # print(extractor(pg_html, url, pt))

        queue.put({"url": url, "extract_function": extractor, "prop_type": pt})



# # Start threads
num_threads = 10  
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()


# # Write to gzip format
gz_filename = "PrivateComRental.csv.gz"
with gzip.open(gz_filename, "wt", newline="", encoding="utf-8") as gzfile:
    fieldnames = results[0].keys() if results else []
    writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

# ###
# ### Upload to Azure Blob Storage
blob_connection_string = f"{con_str}"
blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="privatepropre",
    blob_name = gz_filename     #csv_filename
)
with open(gz_filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

print("CSV file uploaded to Azure Blob Storage.")

################################################################################################################################################################################################################################################
# -- Comm Rental Agents
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

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")
con_str_coms = os.getenv("CON_STR_COMS")

session = HTMLSession()

# Thread worker function
def worker(queue, results):
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
                results.append(result)
        except Exception as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()

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

def extractor(soup, url):
    # Initialize variables with default values
    agent_name =  None
    agent_url = url

    agent_name = soup.find('h1').text
 
    return {"Agent Name": agent_name, "Agent Url": agent_url}

def getDealers(soup):
    try:
        agent_name = None
        agent_url = None
        agent_div = soup.find('div', class_='listing-result__advertiser txt-small-regular')

        if agent_div:
            try:
                agent_detail = agent_div.find('img', class_='listing-result__logo')
                agent_name = agent_detail.get('alt')
                agent_url = agent_detail.get('src')
                agent_id_match = re.search(r'offices/(\d+)', agent_url)
                if agent_id_match:
                    agent_id = agent_id_match.group(1)
                    agent_url = f"{base_url}/estate-agency/estate-agent/{agent_id}"
            except:
                agent_name = "Private Seller"
                agent_url = None
    except KeyError:
        agent_name = None
        agent_url = None

    return agent_url

# Initialize thread queue and results list
queue = Queue()
results = []

prop_types = ['Commercial', 'Hospitality', 'Industrial','Office', 'Retail', 'VacantLandPlot']

for pt in prop_types:
    x = f"{base_url}/commercial-rentals/south-africa/1?pt={pt}"
    land = session.get(x)
    land_html = BeautifulSoup(land.content, 'html.parser')
    pgs = getPages(land_html, x)

    for p in range(1, pgs + 1):
        home_page = session.get(f"{x}&page={p}")
        soup = BeautifulSoup(home_page.content, 'html.parser')
        prop_contain = soup.find_all('a', class_='listing-result')
        for x_page in prop_contain:
            prop = getDealers(x_page)
            if prop is not None:
                Deal_url = prop
                queue.put({"url": Deal_url, "extract_function": extractor})

# Start threads
num_threads = 10
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()

# Write results to CSV
gz_filename = 'PrivatePropRentDealer_part2.csv.gz'
with gzip.open(gz_filename, 'wt', newline='', encoding='utf-8') as csvfile:
    fieldnames = results[0].keys() if results else []
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

### Upload to Azure Blob Storage
blob_connection_string = f"{con_str}"
blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="privatepropre",
    blob_name=gz_filename
)
with open(gz_filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

print("CSV file uploaded to Azure Blob Storage.")





################################################################################################################################################################################################################################################
