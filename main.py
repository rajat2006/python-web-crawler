import requests
import pymongo
import time
import threading
from cfg import root_url, num_parallel_threads, mongo_conn_url, mongo_conn_port
from functions import initialize_database, scrape_url, check_doc, get_connection

# function to run threads
def run_threads(threads):
    # print("Running threads in parallel")
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


mycol = get_connection()
# initialize database with the first entry (root_url)
initialize_database(mycol)


print("----- Starting the crawling process -----")
docs = mycol.find({})
finished = False
max_col = len(list(docs))
col_done = 0

"""
starting an infinte loop to scrape all urls
present in the database that have not been scraped in the last 24 hours

"""
docs.rewind()
while True:
    threads = []
    for i in range(num_parallel_threads):
        try:
            doc = docs.next()
        except:
            break
        else:
            toScrape = check_doc(doc)
            if toScrape:
                thread = threading.Thread(target=scrape_url, args=(doc['link'], get_connection(),))
                threads.append(thread)
            col_done += 1

    if len(threads) > 0:
        run_threads(threads)

    if col_done == max_col:
        docs.rewind()
        time.sleep(5)
        col_done = 0
        max_col = len(list(mycol.find({})))
        


        
            
        

            


    

