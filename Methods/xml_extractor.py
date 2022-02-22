import datetime
import re
import xml.etree.ElementTree as ET
import os
from Methods.helper import *
import dateutil.parser as parser
import pymongo
import time

usStatesAbbr = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District Of Columbia': 'DC',
    'Federated States Of Micronesia': 'FM',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Marshall Islands': 'MH',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands': 'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Palau': 'PW',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}
def getAttributeValues(allfields):
    return list(map(lambda x: x["tagName"], allfields)), list(map(lambda x: x["name"], allfields)), list(
        map(lambda x: x["dataType"], allfields))


def xmlExtractor(parent_directory, foldername, filename, extraction_collection_conn, csm_collection_conn, employerId, fieldmaps, additional_fields_data,
                 client_companies_collection_conn, processId):
    try:
        jobscounter = 0
        totaldata = []
        def removeJobs(input_collection_name):
            list_of_ids = list(map(lambda x: x["_id"], list(input_collection_name.find({"employerId": employerId,"source": additional_fields_data["source"]}, {"_id": 1}))))
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
        # removeJobs(extraction_collection_conn)
        removeJobs(csm_collection_conn)

        # extraction_collection_conn.bulk_write([pymongo.DeleteMany({"employerId": employerId,"source": additional_fields_data["source"]})])
        # csm_collection_conn.bulk_write([pymongo.DeleteMany({"employerId": employerId,"source": additional_fields_data["source"]})])
        requiredFieldsDicts = fieldmaps["fields"]
        xmltags, databaseFields, typechecks = getAttributeValues(requiredFieldsDicts)
        companiesDict = {}
        for event, elem in ET.iterparse(filename, events=("start", "end")):
            if elem.tag == fieldmaps["root"] and event == "end":
                eachjobdata = {}
                rangeofmembers = range(0, len(elem))
                tagnamesXML = [elem[x].tag for x in rangeofmembers]

                def getTypeBasedValue(value, typecheck):
                    try:
                        if is_valid_data(value):
                            if typecheck == "STRING":
                                return str(value)
                            elif typecheck == "INTEGER":
                                return int(value)
                            elif typecheck == "DECIMAL":
                                return float(value)
                            elif typecheck == "DATE":
                                datehere = parser.parse(str(value))
                                return datehere
                            else:
                                return str(value)
                        return ""
                    except Exception as e:
                        error = f"Error at :{getTypeBasedValue.__name__} - {e}"
                        print(error)
                        return error

                def getallelemdata(eachmember):
                    try:
                        # eachjobdata[fieldMaps[elem[eachmember].tag]] = elem[eachmember].text
                        if eachmember in xmltags:
                            eachjobdata[databaseFields[xmltags.index(eachmember)]] = getTypeBasedValue(
                                elem[tagnamesXML.index(eachmember)].text, typechecks[xmltags.index(eachmember)])
                        else:
                            eachjobdata[eachmember] = elem[tagnamesXML.index(eachmember)].text
                    except:
                        eachjobdata[eachmember] = None
                        pass

                def getAdditionalFields(eachadditionalfield):
                    try:
                        if eachadditionalfield == "uniqueFields":
                            uniqueFieldName = additional_fields_data[eachadditionalfield]
                            uniqueFieldTags = fieldmaps["uniqueFields"]
                            if len(uniqueFieldTags) > 0:
                                totallistUniqueKey = []
                                for eachuniqueField in uniqueFieldTags:
                                    if is_valid_data(eachjobdata[eachuniqueField]):
                                        if eachuniqueField == "state":
                                            eachjobdata["state"] = usStatesAbbr[eachjobdata[eachuniqueField]] if eachjobdata[eachuniqueField] in usStatesAbbr else eachjobdata[eachuniqueField]
                                        if eachuniqueField == "country":
                                            countryChecker = eachjobdata[eachuniqueField].lower()
                                            if (countryChecker == "unitedstates" or countryChecker == "united states" or countryChecker == "usa" or countryChecker == "us" or countryChecker == "united states of america"):
                                                eachjobdata[eachuniqueField] = "US"
                                        uniqueKeyString = ""
                                        cleansedField = re.sub('[^A-Za-z0-9]+', ' ',
                                                               eachjobdata[eachuniqueField].lower().strip())
                                        cleansedField = re.sub(' {2,}', ' ', cleansedField).strip()
                                        splitlist = cleansedField.split(" ")
                                        splitlist.sort()
                                        uniqueKeyString = "`".join(splitlist)
                                        totallistUniqueKey.append(uniqueKeyString)
                                eachjobdata[uniqueFieldName] = "~~".join(totallistUniqueKey)
                        elif eachadditionalfield == "original_title":
                            eachjobdata[eachadditionalfield] = eachjobdata["title"].lower() if "title" in eachjobdata and is_valid_data(eachjobdata["title"]) else None
                        elif eachadditionalfield == "original_company":
                            eachjobdata[eachadditionalfield] = eachjobdata["company"] if "company" in eachjobdata and is_valid_data(eachjobdata["company"]) else None
                        else:
                            eachjobdata[eachadditionalfield] = additional_fields_data[eachadditionalfield]
                    except Exception as e:
                        error = f"Error at :{getAdditionalFields.__name__} - {e}"
                        print(error)
                        return error

                list(map(getallelemdata, tagnamesXML))
                additional_fields_data_keys = additional_fields_data.keys()
                list(map(getAdditionalFields, additional_fields_data_keys))
                # def getMissingFields():
                #     keyshere = eachjobdata.keys()
                #     keysadd = list(set(databaseFields) - set(keyshere))
                #     if len(keysadd) > 0:
                #         for eachnewkey in keysadd:
                #             eachjobdata[eachnewkey] = ""
                # getMissingFields()
                company = eachjobdata["company"]
                companiesDict[company] = companiesDict.get(company, 0) + 1
                totaldata.append(eachjobdata)
                elem.clear()
                if len(totaldata) >= 25000:
                    # extraction_collection_conn.insert_many(totaldata)
                    csm_collection_conn.insert_many(totaldata)
                    jobscounter += len(totaldata)
                    totaldata = []
        if len(totaldata) > 0:
            # extraction_collection_conn.insert_many(totaldata)
            csm_collection_conn.insert_many(totaldata)
            jobscounter += len(totaldata)
        CompaniesToInsert = list(map(lambda x: pymongo.UpdateOne({"company": x, "employerId": employerId}, {
            "$set": {"total": companiesDict[x], "audId": processId, "lastUpdatedDate": datetime.datetime.now()},
            "$setOnInsert": {"createdOn": datetime.datetime.now()}}, upsert=True), companiesDict))
        client_companies_collection_conn.bulk_write(CompaniesToInsert) if len(CompaniesToInsert) > 0 else ""
        if foldername is not None:
            if os.path.exists(foldername):
                os.remove(foldername)
        if filename is not None:
            if os.path.exists(filename):
                os.remove(filename)
        os.chdir("../")
        if parent_directory is not None:
            if os.path.exists(parent_directory):
                os.rmdir(parent_directory)
        return jobscounter
    except Exception as e:
        if foldername is not None:
            if os.path.exists(foldername):
                os.remove(foldername)
        if filename is not None:
            if os.path.exists(filename):
                os.remove(filename)
        os.chdir("../")
        if parent_directory is not None:
            if os.path.exists(parent_directory):
                os.rmdir(parent_directory)
        error = f"Error at :{xmlExtractor.__name__} - {e}"
        print(error)
        return error
