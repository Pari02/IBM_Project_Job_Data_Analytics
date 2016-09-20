# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 10:38:22 2016

@author: Parikshita
@date: 09/19/2016

"""

# import pacakages
import json, itertools, pandas as pd
from pandas import DataFrame
from pandas.io.json import json_normalize
import extractID
from extractID import *
import extractAPIData
from extractAPIData import *

# function try and catch is the key exists in the json
def check(keyname, Data):
    val = 0
    if[req for req in Data if "result" in Data] != []:
        if keyname == "data":
            try:
                if Data["result"][keyname] != []:
                    val = 1
                else:
                    val = 0
            except KeyError:
               val = 0
        else:
            try:
                if Data["result"]["data"][keyname] != []:
                    val = 1
                else:
                    val = 0
            except KeyError:
                val = 0
    return val
    
# function to get data for respective key
def getKeyData(keyname, Data, Id):
    # defining variables
    apiData = Data
    key_data = []
    if keyname != "data":
        if check(keyname, Data) == 1:
            key_data = getData(keyname, Data, Id)  
        
    else:
        if check("certifications", apiData) == 1:
            del apiData["result"][keyname]["certifications"]
            #print "yay"
        if check("educationRequirements", apiData) == 1:
            del apiData["result"][keyname]["educationRequirements"]
            #print "yay"
        if check("experienceLevels", apiData) == 1:
            del apiData["result"][keyname]["experienceLevels"]
            #print "yay"
        key_data = getData(keyname, apiData, Id)
    return key_data

# function to extract data 
def getData(keyname, Data, Id):

    if keyname == "data":
        # store the data in new variable
        keyJSON = Data["result"][keyname]
        # converting extracted data in to dataframe
        df_key = DataFrame(json_normalize(keyJSON))
        # adding jobID to the dataframe
        df_key['jobID'] = "explorer/jobs/"+Id

    else:
        # store the data in new variable
        keyJSON = Data["result"]["data"][keyname]
        # converting extracted data in to dataframe1
        df_key = DataFrame(list(itertools.chain(keyJSON)))
        # adding jobID to the dataframe
        df_key['jobID'] = "explorer/jobs/"+Id

    return df_key

def main():
    # extraction of job title data and appending jobID to it
    jobID = extractID("Data/jobsMarketData.csv")
    # define empty lists
    cert_data = DataFrame()
    eduReq_data = DataFrame()
    expLvl_data = DataFrame()
    job_data = DataFrame()
    # loop over each resource to get API data by stateAreaID
    for Id in jobID:
        #print Id
        url = ("http://sandbox.api.burning-glass.com/v202/explorer/jobs/"+Id+"?culture=EnglishUS&orderby=Id ASC")
        # get the api data by passing request type, url and authorization parameters
        response = extractAPIData(url)
        # converting the data into json format and extracting relevant information  
        jsonData = json.loads(response)
        # calling function check to see if the key value exists in json data extracted from API
        # function extracts and returs the data
        cert = DataFrame(getKeyData("certifications", jsonData, Id))
        eduReq = DataFrame(getKeyData("educationRequirements", jsonData, Id))
        expLvl = DataFrame(getKeyData("experienceLevels", jsonData, Id))
        job = DataFrame(getKeyData("data", jsonData, Id))
        
        
        # appending data
        cert_data = pd.concat([cert_data, cert])
        eduReq_data = pd.concat([eduReq_data, eduReq], axis=0)
        expLvl_data = pd.concat([expLvl_data, expLvl], axis=0)
        job_data = pd.concat([job_data, job], axis=0)
        
        
    # exporting the data to csv file
    cert_data.to_csv("Data/certificationsData.csv", encoding='utf-8', index = False)
    eduReq_data.to_csv("Data/eduRequirementsData.csv", encoding='utf-8', index = False)
    expLvl_data.to_csv("Data/expLevelData.csv", encoding='utf-8', index = False)
    job_data.to_csv("Data/jobData.csv", encoding='utf-8', index = False)

if __name__ == "__main__":
    main()