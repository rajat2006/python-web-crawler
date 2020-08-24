from cfg import root_url
from functions import scrape_url, initialize_database
import pymongo


myclient = pymongo.MongoClient('localhost', 27017)
mydb = myclient['crawlerdb']
mycol = mydb['Links']
# x = mycol.find_one({'sourceLink' : root_url})
# print(x)
mycol.delete_many({})

#initialize_database()
x = list(mycol.find())
# print(x)
for item in x:
    print(x)
# print(list(mycol.find({})))

