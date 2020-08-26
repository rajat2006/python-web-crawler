# Url from which the crawling process starts
root_url = "https://flinkhub.com"

# waiting time between each cycle of scraping
wait_time = 5

# maximum number of allowed links in the database
max_link_limits = 5000

#length of the randomly generated file name
file_name_length = 20

# path where all html files are stored
html_files_path = "/Users/rajat/Documents/Projects/python-web-crawler/html_files/"

# path where files other than html files are stores
other_files_path = "/Users/rajat/Documents/Projects/python-web-crawlerr/other_files/"

# number of parallel threads
num_parallel_threads = 5

# mongo connection URL, port, database name and collection name
mongo_conn_url = "localhost"
mongo_conn_port = 27017
mongo_db_name = "crawlerdb"
mongo_col_name = "Links"