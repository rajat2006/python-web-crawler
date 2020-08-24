import requests
import pymongo
import time
import threading
from cfg import root_url, num_parallel_threads
from functions import initialize_database, scrape_url, check_doc

# initialize database with the first entry (root_url)
initialize_database()

"""
starting an infinte loop to scrape all urls
present in the database that have not been scraped in the last 24 hours

"""

myclient = pymongo.MongoClient('localhost', 27017)
mydb = myclient['crawlerdb']
mycol = mydb['Links']
docs = mycol.find({})
print(len(list(docs)))

def run_threads(threads):
    # print("Running threads in parallel")
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

print("----- Starting the crawling process -----")

finished = False
max_col = len(list(docs))
col_done = 0
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
                thread = threading.Thread(target=scrape_url(doc['link']))
                threads.append(thread)
            col_done += 1

    if len(threads) > 0:
        run_threads(threads)

    if col_done == max_col:
        docs.rewind()
        time.sleep(5)
        col_done = 0
        max_col = len(list(mycol.find({})))
        


        
            
        

            


    

