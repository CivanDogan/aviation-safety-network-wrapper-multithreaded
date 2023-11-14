import requests
from bs4 import BeautifulSoup
import pandas as pd
import threading
import queue
import time
import random

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 '
                  'Safari/537.36',
} #Stole this from another repo
def get_soup(url):
    response = requests.get(url, headers=headers)
    page = response.text
    soup = BeautifulSoup(page, "html.parser")
    return soup

def get_data(soup):
    table = soup.find('table', class_='hp')
    if table is None:
        return []
    table_rows = table.find_all("tr")
    data = []
    for tr in table_rows:
        td = tr.find_all("td")
        row = [tr.text.strip() for tr in td if tr.text.strip()]
        if row:
            data.append(row)
    return data

def get_next_pages(soup):
    div = soup.find('div', class_='pagenumbers')
    if div is None:
        return None
    a = div.find_all('a')
    if len(a) == 0:
        return None
    #get page numbers
    pages = [int(x.text) for x in a]
    return pages
def worker(url_queue, data_lock, df_list):
    while True:
        # Get the next URL from the queue
        year, page = url_queue.get()
        if year is None:
            break

        try:
            # Process the URL
            link = f"https://aviation-safety.net/wikibase/dblist.php?Year={year}&page={page}"
            soup = get_soup(link)
            data = get_data(soup)
            if page == 1: # If first page, get the rest of the pages and add to queue
                next_page = get_next_pages(soup)
                if next_page:
                    for page in next_page:
                        url_queue.put((year, page))

            if data:
                with data_lock:
                    df_list.append(pd.DataFrame(data))

        finally:
            # Signal to the queue that task is done
            url_queue.task_done()
            #time.sleep(0.1)
def print_queue_size(): # Print the queue size every 2 seconds kind of useless LOL
    last=1
    while True:
        print("Queue size: {}".format(url_queue.qsize()))
        time.sleep(2)
        if last == url_queue.qsize():
            break
        last = url_queue.qsize()

url_queue = queue.Queue() # Queue of URLs to process
data_lock = threading.Lock()    # Lock to serialize console output
def main():
    years = range(1902, 2024)
    num_threads = 16 # Number of threads

    df_list = []

    # Start worker threads
    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=worker, args=(url_queue, data_lock, df_list))
        thread.start()
        threads.append(thread)

    # Enqueue all URLs
    for year in years:
            url_queue.put((year,1))

    # Start a thread to print the queue size
    counter = threading.Thread(target=print_queue_size).start()

    # Block until all tasks are done
    url_queue.join()

    # Stop workers
    for _ in range(num_threads):
        url_queue.put((None, None))

    for thread in threads:
        thread.join()

    print("Done!")
    # Combine all dataframes
    df = pd.concat(df_list)
    df.columns = ["date", "type", "reg", "operator", "fat", "location", "dmg"]

    #sort by date
    df = df.sort_values(by=['date'], key=lambda x: pd.to_datetime(x, format="%d-%b-%Y", errors='coerce'))
    #save to
    df.to_csv("data.csv", index=False)
    print("Saved to data.csv")
    #total length
    print("Total Length: {}".format(len(df)))

if __name__ == "__main__":
    #time the program
    start_time = time.time()

    main()

    print("--- %s seconds ---" % (time.time() - start_time))