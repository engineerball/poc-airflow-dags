from pymongo import MongoClient

# client = MongoClient('mongodb://app_user:app_password@mongodb:27017/')
client  = MongoClient(
    host = 'mongodb', # <-- IP and port go here
    port=27017,
    serverSelectionTimeoutMS = 3000, # 3 second timeout
    username="app_user",
    password="app_password",
)