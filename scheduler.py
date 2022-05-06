import pymongo
import schedule
from dotenv import load_dotenv
from Methods.mongoOperations import *
from Methods.xml_extractor import *

load_dotenv()
# env variables
mongo_uri = os.getenv("MASTER_MONGO_URI")
dbname = os.getenv("MASTER_DBNAME")
conf_collection_name = os.getenv("MASTER_CONF_COLLECTION")
client_source_master_collection_name = os.getenv("CLIENT_SOURCE_MASTER_COLLECTION")
client_extracted_feed_collection_name = os.getenv("CLIENT_EXTRACTED_FEED_COLLECTION")
employers_list = os.getenv("EMPLOYERS").split(",") or []
running_uri_list = os.getenv("RUNNING_URIS").split(",") or []
running_db_list = os.getenv("RUNNING_DBS").split(",") or []

conf_collection_conn = getMongoConnURI(mongo_uri, dbname, conf_collection_name)

def myjob():
    try:
        counterCheck = conf_collection_conn.count_documents({ "$or" : [ { "audStatus" : { "$lt" : 3 } }, { "expStatus" : { "$lt" : 3 } }, { "extStatus" : { "$lt" : 3 } } ], "employerId" : { "$in" : employers_list} })
        print(counterCheck)
        if counterCheck == 0:
            def collectionAlteration(mongouri,database):
                print(f"connecting to {mongouri} and {database} for dropping collections")
                client = MongoClient(mongouri)
                dbclient = client[database]
                client_extracted_feed_conn = dbclient[client_extracted_feed_collection_name]
                client_source_master_collection_conn = dbclient[client_source_master_collection_name]
                print("Dropping CSM and CFE collections")
                client_extracted_feed_conn.drop()
                client_source_master_collection_conn.drop()
                print("Completed Dropping CSM and CFE collections")
                print("Creating CSM and CFE collections")
                dbclient.create_collection(client_extracted_feed_collection_name)
                dbclient.create_collection(client_source_master_collection_name)
                print("Completed Creating CSM and CFE collections")
                def createIndexes():
                    indexesList = ["company_1","employerId_1","uniqueKey_1","clientJobId_1","syncStatus_1","url_1","posted_at_1","cpa_1","cpc_1","title_1","logo_1","audStatus_1","etlStatus_1","company_1_employerId_1_audStatus_1","city_1","postalCode_1","company_1_employerId_1_audStatus_1_uniqueKey_1","employerId_1_locExpand_1","employerId_1_uniqueKey_1_cpa_1","unqKeyRegen_status_1","title_1_city_1_state_1_employerId_1_company_1_locExpand_1","tlExpand_1","title_1_employerId_1_company_1_tlExpand_1","original_title_1","employerId_1_source_1"]
                    for eachIndex in indexesList:
                        eachIndexNew = eachIndex.replace("_1_","_1")
                        print(eachIndexNew)
                        newIndexes = list(filter(None,eachIndexNew.split("_1")))
                        print(newIndexes)
                        if len(newIndexes) == 1:
                            client_extracted_feed_conn.create_index(newIndexes[0])
                            client_source_master_collection_conn.create_index(newIndexes[0])
                        elif len(newIndexes) > 1:
                            buildIndexList = []
                            for eachNewIndex in newIndexes:
                                print(eachNewIndex)
                                buildIndexList.append((eachNewIndex, pymongo.ASCENDING))
                            print(buildIndexList)
                            client_extracted_feed_conn.create_index(buildIndexList)
                            client_source_master_collection_conn.create_index(buildIndexList)
                print("Creating Indexes over CSM and CFE collections")
                createIndexes()
                print("Completed Creating Indexes over CSM and CFE collections")
            for eachuri,eachdb in zip(running_uri_list,running_db_list):
                collectionAlteration(eachuri,eachdb)
            updated_doc = conf_collection_conn.update_many({"employerId": {"$in": employers_list}, "audStatus" : { "$gte" : 3 } , "expStatus" : { "$gte" : 3 }, "extStatus" : { "$gte" : 3 } }, {"$set": {"extStatus": 1}})
            if updated_doc.modified_count == 0:
                print("yet to complete aud or expansion or extraction before next extraction")
                time.sleep(300)
                myjob()
        else:
            print("yet to complete aud or expansion or extraction before dropping collections")
            time.sleep(300)
            myjob()
    except Exception as e:
        print("database connection failed - retrying")
        time.sleep(300)
        myjob()

schedule.every().day.at("00:30").do(myjob)
schedule.every().day.at("08:30").do(myjob)
#schedule.every().day.at("16:30").do(myjob)
# schedule.every(10).seconds.do(myjob)

while 1:
    schedule.run_pending()