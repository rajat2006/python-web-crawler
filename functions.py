import random
import string
import requests
import pymongo
from datetime import datetime
from bs4 import BeautifulSoup
from cfg import root_url, file_name_length, html_files_path, max_link_limits

# function for creating database -> done - working
def initialize_database():
    print("Initializing database with the first entry")

    myclient = pymongo.MongoClient('localhost', 27017)
    mydb = myclient["crawlerdb"]
    mycol = mydb["Links"]
    mycol.delete_many({})
    ini_doc = {
        'link' : root_url,
        'sourceLink' : None,
        'isCrawled' : False,
        'lastCrawledDate' : None,
        'responseStatus' : None,
        'contentType' : None,
        'contentLength' : None,
        'filePath' : None,
        'createdAt' : datetime.now(),
    }
    mycol.insert_one(ini_doc)

    print("Database initialized with the first entry")

# function to insert information of a link into the database - done - working
def insert_into_database(doc):
    myclient = pymongo.MongoClient('localhost', 27017)
    mydb = myclient["crawlerdb"]
    mycol = mydb["Links"]

    if len(list(mycol.find())) == max_link_limits:
        print("Limit reached, cannot insert anything into the database")
        return

    print("Inserting link : {} into the database".format(doc['link']))
    mycol.insert_one(doc)
        
# function to update document of a particuler link after it has been scraped - done - working
def update_database(info):
    print("Updating database for the link : {}".format(info['link']))
    myclient = pymongo.MongoClient('localhost', 27017)
    mydb = myclient['crawlerdb']
    mycol = mydb['Links']

    query = {'link' : info['link']}
    new_info = {
        '$set': {
            'filePath' : info['filePath'],
            'isCrawled' : info['isCrawled'],
            'lastCrawledDate' : info['lastCrawledDate'],
            'responseStatus' : info['responseStatus'],
            'contentType' : info['contentType'],
            'contentLength' : info['contentLength'],
        }
    }
    mycol.find_one_and_update(query, new_info)

# generate random string for file name of the specified length (file_name_length) - done - working
def generate_random_string():
    letters = string.ascii_lowercase
    strr = ''.join(random.choice(letters) for i in range(file_name_length))
    return strr


# function to get link from the anchor tag - done - working
def get_link(s:str, source_link:str):
    # print(s)
    try:
        temp = s.split('>')
        start = temp[0].index('href="') + 6
        end = temp[0][start:].index('"') + start
        link = temp[0][start:end]
        if len(link) > 0 and link[0] == "/":
            link = source_link + link
        
    except:
        return ""
    else:
        return link

# check if the document needs to be crawled or not - done - working
def check_doc(doc):
    cur_date = datetime.now()
    if doc['lastCrawledDate'] is None:
        return True
    else:
        last_date = doc['lastCrawledDate']
        diff = cur_date - last_date
        diff = diff.seconds/3600
        if diff >= 24 and 'html' in doc['contentType']:
            return True
        else:
            return False

# function to store html file of a page and update its document is database - done - working
def store_html(text:str, link:str):
    print("Storing html file for the link : {}".format(link))
    myclient = pymongo.MongoClient('localhost', 27017)
    mydb = myclient['crawlerdb']
    mycol = mydb['Links']
    file_path = ""

    # link is already in db
    # we have to check if it has the html file or not

    # if html file has been already generated
    if list(mycol.find({'link' : link}))[0]['filePath'] is not None:
        html_file = open(mycol.find({}, {'link' : link})[0]['filePath'], "w")
        html_file.write(text)

    # if html file has not been generated, then generate a new file name and store the html file
    else:
        file_name = generate_random_string()
        file_path = html_files_path + file_name + '.html'

        # if new file name found, store the html and then return the html file path
        while file_path in mycol.find({}, {'filePath' : file_path}):
            file_name = generate_random_string()
            file_path = html_files_path + file_name + '.html'
        html_file = open(file_path, "w")
        html_file.write(text)
        html_file.close()

    # return html file path if file generated just now else return an empty string
    return file_path

# scrape the given url -> store all the html -> get all the links -> store the links in database 
def scrape_url(link:str):
    myclient = pymongo.MongoClient('localhost', 27017)
    mydb = myclient['crawlerdb']
    mycol = mydb['Links']
    try :
        request = requests.get(link)
        text = request.text
    except:
        return
    
    print("Scraping the link : {}".format(link))

    # file path to the html file
    file_path = store_html(text, link)
    
    # finding anchor tag using BeautifulSoup -> extracting links from all a tags and inserting them into database
    with open(file_path) as fp:
        soup = BeautifulSoup(fp, 'lxml')
        li = soup.find_all('a')
        for item in li:
            if len(list(mycol.find({}))) == max_link_limits:
                break
            # print(item)
            sub_link = get_link(str(item), link)
            # print(list(mycol.find({'link' : sub_link})))
            if sub_link != "" and len(list(mycol.find({'link' : sub_link}))) == 0:
                # print("inserting into database")
                insert_into_database({
                    'link' : sub_link,
                    'sourceLink' : link,
                    'isCrawled' : False,
                    'lastCrawledDate' : None,
                    'responseStatus' : None,
                    'contentType' : None,
                    'contentLength' : None,
                    'filePath' : None,
                    'createdAt' : datetime.now(),
                }) 
    
    # update info about file
    if file_path != "":
        # print('here')
        update_database({
            'link' : link,
            'filePath' : file_path,
            'isCrawled' : True,
            'lastCrawledDate' : datetime.now(),
            'responseStatus' : request.status_code,
            'contentType' : request.headers['Content-Type'],
            'contentLength' : request.content.__sizeof__(),
        })
    else:
        update_database({
            'link' : link,
            'isCrawled' : True,
            'lastCrawledDate' : datetime.now(),
            'responseStatus' : request.status_code,
            'contentType' : request.headers['Content-Type'],
            'contentLength' : request.content.__sizeof__(),
        })


        


    
    
    
