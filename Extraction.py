import datetime

import pymongo
import urllib.request
import os
from urllib.parse import urlparse
from urllib.parse import parse_qs
import requests
import schedule
from dotenv import load_dotenv
import re

from Methods.mongoOperations import *
from Methods.xml_extractor import *

load_dotenv()
# env variables
mongo_uri = os.getenv("MASTER_MONGO_URI")
dbname = os.getenv("MASTER_DBNAME")
conf_collection_name = os.getenv("MASTER_CONF_COLLECTION")
aud_history_collection_name = os.getenv("AUD_HISTORY_COLLECTION")
client_companies_collection_name = os.getenv("CLIENT_COMPANIES_COLLECTION")
conf_collection_conn = getMongoConnURI(mongo_uri, dbname, conf_collection_name)


def myjob():
    try:
        client_conf_data = conf_collection_conn.find_one_and_update({"extStatus": 1}, {
            "$set": {"extStatus": 2}})
        if client_conf_data is not None:
            print(client_conf_data["employerId"])
            clientsJobCounter = 0
            if client_conf_data["sourceType"] == "FEED":
                extractionDbConnection = getMongoConnCreds(client_conf_data["extractionSource"]["mongoProps"])
                if client_conf_data["extractionSource"]["mongoProps"] == client_conf_data["etlSource"]["collection"]:
                    etlDbConnection = extractionDbConnection
                else:
                    etlDbConnection = getMongoConnCreds(client_conf_data["etlSource"]["mongoProps"])
                if isinstance(extractionDbConnection, Exception) is not True and isinstance(etlDbConnection, Exception) is not True:
                    feed_extraction_collection_conn = extractionDbConnection[client_conf_data["extractionSource"]["collection"]]
                    client_source_master_collection_conn = etlDbConnection[client_conf_data["etlSource"]["collection"]]
                    aud_history_collection_conn = extractionDbConnection[aud_history_collection_name]
                    client_companies_collection_conn = extractionDbConnection[client_companies_collection_name]
                    print("In If condition")
                    URLS_data = client_conf_data["sourceUrl"]
                    URLS = list(map(lambda x: x["url"], URLS_data))
                    usernames = list(map(lambda x:x["username"] if "username" in x else None, URLS_data))
                    passwords = list(map(lambda x:x["password"] if "password" in x else None, URLS_data))
                    # fieldMaps = client_conf_data["fieldmap"]
                    # replaceFields = client_conf_data["replaceFields"]
                    # extraFields = client_conf_data["extraFields"]
                    # fieldMapsKeys = fieldMaps.keys()
                    # extraFieldsKeys = extraFields.keys()
                    # replaceFieldsKeys = replaceFields.keys()
                    employerId = client_conf_data["employerId"]
                    totalEmployersData = {}
                    getProcessID = aud_history_collection_conn.insert_one(
                        {"employerId": employerId, "extraction_startTime": datetime.datetime.now()}).inserted_id

                    for URL in URLS:
                        additional_fields_data = {
                            "employerId": employerId,
                            "clientName": employerId.split("-")[-1] if "-" in employerId else employerId,
                            "source": URL,
                            "expStatus": 0,
                            "uniqueFields": client_conf_data["uniqueFields"][0],
                            "processId": getProcessID,
                            "original_title": "",
                            "original_company": ""
                        }
                        def removeJobs(input_collection_name):
                            list_of_ids = list(map(lambda x: x["_id"], list(input_collection_name.find(
                                {"employerId": employerId, "source": additional_fields_data["source"]}, {"_id": 1}))))

                            def divide_chunks(l, n):
                                # looping till length l
                                for i in range(0, len(l), n):
                                    yield l[i:i + n]

                            # How many elements each
                            # list should have
                            n = 100000
                            x = list(divide_chunks(list_of_ids, n))
                            for eachchunk in x:
                                start = time.time()
                                input_collection_name.delete_many({'_id': {'$in': eachchunk}})
                                print(time.time() - start)

                        removeJobs(feed_extraction_collection_conn)
                        removeJobs(client_source_master_collection_conn)

                        os.mkdir(employerId)
                        os.chdir(employerId)
                        parent_working_directory = employerId
                        if usernames[URLS.index(URL)] is not None and passwords[URLS.index(URL)] is not None:
                            print(usernames[URLS.index(URL)])
                            print(passwords[URLS.index(URL)])
                            response = requests.get(URL, auth=(usernames[URLS.index(URL)], passwords[URLS.index(URL)]))
                            filename = employerId + ".xml"
                            with open(filename, 'wb') as file:
                                file.write(response.content)
                            finalOp = xmlExtractor(parent_working_directory, None, filename,
                                                   feed_extraction_collection_conn, client_source_master_collection_conn,
                                                   employerId,
                                                   client_conf_data["feed"], additional_fields_data,
                                                   client_companies_collection_conn, getProcessID)
                            print(finalOp)
                        elif "jobadx" in employerId:
                            folder_name = 'feed.xml.gz'
                            response = requests.get(URL, stream=True)
                            if response.status_code == 200:
                                with open(folder_name, 'wb') as f:
                                    f.write(response.raw.read())
                            print("Downloading data now")
                            os.system("gunzip " + folder_name)
                            print("Completed Gunzipping data now")
                            filename = folder_name.split(".gz")[0]
                            print(filename)
                            finalOp = xmlExtractor(parent_working_directory, folder_name, filename,
                                                   feed_extraction_collection_conn,
                                                   client_source_master_collection_conn,
                                                   employerId,
                                                   client_conf_data["feed"], additional_fields_data,
                                                   client_companies_collection_conn, getProcessID)
                            print(finalOp)
                        elif URL.endswith(".zip"):
                            os.system(f"wget -c --read-timeout=5 --tries=0 {URL}")

                            from zipfile import ZipFile
                            folder_name = "feed.xml.gz"
                            # opening the zip file in READ mode
                            with ZipFile(folder_name, 'r') as zip:
                                # printing all the contents of the zip file
                                zip.printdir()
                                # extracting all the files
                                print('Extracting all the files now...')
                                zip.extractall()
                                print('Done!')

                            filename = "jobiak_feed.xml"
                            finalOp = xmlExtractor(parent_working_directory, folder_name, filename, feed_extraction_collection_conn, client_source_master_collection_conn, employerId,
                                                   client_conf_data["feed"], additional_fields_data,
                                                   client_companies_collection_conn, getProcessID)
                            print(finalOp)

                        elif URL.endswith(".xml"):
                            # response = requests.get(URL)
                            filename = employerId + '.xml'
                            r = requests.get(URL, stream=True)
                            with open(filename, 'wb') as f:
                                for chunk in r.iter_content(chunk_size=1200000):
                                    if chunk:  # filter out keep-alive new chunks
                                        f.write(chunk)
                            finalOp = xmlExtractor(parent_working_directory, None, filename, feed_extraction_collection_conn, client_source_master_collection_conn, employerId,
                                                       client_conf_data["feed"], additional_fields_data,
                                                       client_companies_collection_conn, getProcessID)
                            print(finalOp)

                        elif URL.endswith(".gz"):
                            print("Downloading data now")
                            os.system(f"wget -c --read-timeout=5 --tries=0 {URL}")
                            print("Completed Downloading data now")
                            folder_name = URL.split("/")[-1]
                            print("Gunzipping data now")
                            os.system("gunzip " + folder_name)
                            print("Completed Gunzipping data now")

                            filename = folder_name.split(".gz")[0]
                            finalOp = xmlExtractor(parent_working_directory, folder_name, filename, feed_extraction_collection_conn, client_source_master_collection_conn, employerId,
                                                   client_conf_data["feed"], additional_fields_data,
                                                   client_companies_collection_conn, getProcessID)
                            print(finalOp)

                        elif "?" in URL and "talent.com" in URL:
                            parsed_url = urlparse(URL)
                            partnervalue = parse_qs(parsed_url.query)['partner'][0]
                            countryvalue = parse_qs(parsed_url.query)['country'][0]
                            print(partnervalue, countryvalue)
                            filename = f"{partnervalue}-{countryvalue}.xml"
                            urllib.request.urlretrieve(URL, filename)
                            finalOp = xmlExtractor(parent_working_directory, None, filename, feed_extraction_collection_conn, client_source_master_collection_conn, employerId,
                                                   client_conf_data["feed"], additional_fields_data,
                                                   client_companies_collection_conn, getProcessID)
                            print(finalOp)
                        else:
                            # response = requests.get(URL)
                            filename = 'feedFile.xml'
                            # with open(filename, 'wb') as file:
                            #     file.write(response.content)
                            r = requests.get(URL, stream=True)
                            with open(filename, 'wb') as f:
                                for chunk in r.iter_content(chunk_size=1200000):
                                    if chunk:  # filter out keep-alive new chunks
                                        f.write(chunk)
                            finalOp = xmlExtractor(parent_working_directory, None, filename, feed_extraction_collection_conn, client_source_master_collection_conn, employerId,
                                                   client_conf_data["feed"], additional_fields_data,
                                                   client_companies_collection_conn, getProcessID)
                            print(finalOp)
                        totalEmployersData[URL] = finalOp
                        clientsJobCounter = clientsJobCounter + finalOp
                        # conf_collection_conn.update_one({"sourceUrl.Url": {"$in": [URL]},"employerId": employerId},
                        #                                 {"$set": {"lastUpdatedDate": datetime.datetime.now()}})
                    conf_collection_conn.update_one({"_id": client_conf_data["_id"]}, {
                        "$set": {"extStatus": 3, "processID": getProcessID,
                                 "expStatus": 1,"lastUpdatedDate": datetime.datetime.now()}})
                    aud_history_collection_conn.update_one({"_id": getProcessID},{"$set":{"extraction_EndTime": datetime.datetime.now()}})
                    # datehere = daily_feed_refresh_id_collection_conn.find_one({"audId": "hybrid"})['audCode']
                    # aud_history_collection_conn.insert_one(
                    #     {"company": client_conf_data["company"], "employerId": client_conf_data["employerId"],
                    #      "audCode": datehere,
                    #      "jobscount_PE": clientsJobCounter,"dateInserted":datetime.datetime.now()})
                    print(totalEmployersData)
                else:
                    print("In else condition")
                    conf_collection_conn.find_one_and_update({"_id": client_conf_data["_id"]}, {
                        "$set": {"extStatus": 1}})
            myjob()
    except Exception as e:
        print(e)
        time.sleep(600)
        myjob()


# schedule.every().day.at("00:00").do(myjob)
schedule.every(5).seconds.do(myjob)
while 1:
    schedule.run_pending()
