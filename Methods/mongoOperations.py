from pymongo import MongoClient


def getMongoConnCreds(mongoProps):
    try:
        username = mongoProps["username"]
        password = mongoProps["password"]
        dsname = mongoProps["dsName"]
        authDatabase = mongoProps["authDatabase"]
        database = mongoProps["database"]
        client = MongoClient(f"mongodb://{username}:{password}@{dsname}/{authDatabase}")
        client.server_info()
        dbclient = client[database]
        return dbclient
    except Exception as e:
        error = f"Error at :{getMongoConnCreds.__name__} - {e}"
        print(error)
        return e


def getMongoConnURI(mongouri, database, collection):
    try:
        client = MongoClient(mongouri)
        client.server_info()
        dbclient = client[database]
        collectionconn = dbclient[collection]
        return collectionconn
    except Exception as e:
        error = f"Error at :{getMongoConnURI.__name__} - {e}"
        print(error)
        return e
