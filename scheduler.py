import schedule
from dotenv import load_dotenv
from Methods.mongoOperations import *
from Methods.xml_extractor import *

load_dotenv()
# env variables
mongo_uri = os.getenv("MASTER_MONGO_URI")
dbname = os.getenv("MASTER_DBNAME")
conf_collection_name = os.getenv("MASTER_CONF_COLLECTION")
employers_list = os.getenv("EMPLOYERS").split(",") or []

conf_collection_conn = getMongoConnURI(mongo_uri, dbname, conf_collection_name)

def myjob():
    try:
        updated_doc = conf_collection_conn.update_many({"employerId": {"$in": employers_list},"audStatus":3, "expStatus":3, "extStatus":3}, {"$set": {"extStatus": 1}})
        if updated_doc.modified_count == 0:
            print("yet to complete aud or expansion or extraction before next extraction")
            time.sleep(300)
            myjob()
    except Exception as e:
        print("database connection failed - retrying")
        time.sleep(300)
        myjob()

# schedule.every().day.at("00:00").do(myjob)
schedule.every().day.at("02:00").do(myjob)
schedule.every().day.at("10:00").do(myjob)
while 1:
    schedule.run_pending()