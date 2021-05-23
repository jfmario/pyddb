
import os

from pymongo import MongoClient

client = MongoClient(os.getenv('MONGO_URL'))

admin_db = client['ddb_admin']
admin_db['tables'].create_index('full_name', unique=True)

def get_db(db_name):
    if db_name == 'ddb_admin':
        raise Exception("Cannot use DB name 'ddb_admin'.")
    else:
        return client[db_name]