import random
import string
import requests
import pymongo
from datetime import datetime
from bs4 import BeautifulSoup
from cfg import root_url, file_name_length, html_files_path, max_link_limits, mongo_conn_url, mongo_conn_port, mongo_db_name, mongo_col_name, other_files_path

# return connections to the mongodb collection
def get_connection():
    myclient = pymongo.MongoClient(mongo_conn_url, mongo_conn_port)
    mydb = myclient[mongo_db_name]
    mycol = mydb[mongo_col_name]
    return mycol

# function for creating database
def initialize_database(mycol):
    print("Initializing database with the first entry")
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

# function to insert information of a link into the database
def insert_into_database(doc, mycol):
    if len(list(mycol.find())) == max_link_limits:
        print("Limit reached, cannot insert anything into the database")
        return

    print("Inserting link : {} into the database".format(doc['link']))
    mycol.insert_one(doc)
        
# function to update document of a particuler link after it has been scraped
def update_database(info, mycol):
    print("Updating database for the link : {}".format(info['link']))

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

# generate random string for file name of the specified length (file_name_length)
def generate_random_string():
    letters = string.ascii_lowercase
    strr = ''.join(random.choice(letters) for i in range(file_name_length))
    return strr

# function to get link from the anchor tag 
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

# check if the document needs to be crawled or not
def check_doc(doc):
    cur_date = datetime.now()
    if doc['lastCrawledDate'] is None:
        return True
    else:
        last_date = doc['lastCrawledDate']
        diff = cur_date - last_date
        diff = diff.seconds/3600
        if diff >= 24:
            return True
        else:
            return False

# function to store html file of a page and update its document in database 
def store_html(text:str, link:str, mycol):
    print("Storing html file for the link : {}".format(link))
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

# function to store files other than html file and update its document in database
def store_file(text:str, link:str, mycol, file_type:str):
    print("Storing file for the link : {}".format(link))
    file_path = ""

    # link is already in db
    # we have to check if it has the file or not

    # if file has been already generated
    if list(mycol.find({'link' : link}))[0]['filePath'] is not None:
        fp = open(mycol.find({}, {'link' : link})[0]['filePath'], "wb")
        fp.write(bytearray(text, 'utf8'))

    # if file has not been generated, then generate a new file name and store the html file
    else:
        file_name = generate_random_string()
        file_path = other_files_path + file_name + file_type

        # if new file name found, store the html and then return the html file path
        while file_path in mycol.find({}, {'filePath' : file_path}):
            file_name = generate_random_string()
            file_path = html_files_path + file_name + file_type
        html_file = open(file_path, "wb")
        html_file.write(bytearray(text, "utf8"))
        html_file.close()

    # return file path if file generated just now else return an empty string
    return file_path

# scrape the given url -> store all the html -> get all the links -> store the links in database 
def scrape_url(link:str, mycol):
    try :
        request = requests.get(link)
        text = request.text
    except:
        return
    
    print("Scraping the link : {}".format(link))

    # check if file is html or not
    if 'html' not in request.headers['Content-Type']:
        con_type = request.headers['Content-Type']
        l = con_type.index('/')
        file_type = '.'+ con_type[l+1:]
        file_path = store_file(text, link, mycol, file_type)

    else:
        # file path to the html file
        file_path = store_html(text, link, mycol)
    
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
                    }, mycol) 
    
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
        }, mycol)
    else:
        update_database({
            'link' : link,
            'isCrawled' : True,
            'lastCrawledDate' : datetime.now(),
            'responseStatus' : request.status_code,
            'contentType' : request.headers['Content-Type'],
            'contentLength' : request.content.__sizeof__(),
        }, mycol)


        


    
    
    
