import pymongo
import config #db creds

if __name__ == "__main__":
    client = pymongo.MongoClient(config.mdb_uri)
    collection = client[config.db][config.collection]

    change_stream = collection.watch()

    for change in change_stream:
        if change['operationType'] == 'insert':
            doc = change['fullDocument']
            updates = {}

            crud = collection.update_one(
                {'_id': doc['_id']},
                {'$set': {'updates': updates}}
            )

'''
https://www.mongodb.com/docs/manual/changeStreams/
https://www.mongodb.com/developer/languages/python/python-change-streams/
'''