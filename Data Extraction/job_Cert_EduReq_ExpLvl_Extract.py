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
        if check("occupationGroup", apiData) == 1:
            del apiData["result"][keyname]["occupationGroup"]
        key_data = getData(keyname, apiData, Id)
    return key_data

# function to extract data 
def getData(keyname, Data, Id):

    if keyname == "data":
        # store the data in new variable
        keyJSON = Data["result"][keyname]
        # converting extracted data in to dataframe
        df_key = DataFrame(json_normalize(keyJSON))

    else:
        # store the data in new variable
        keyJSON = Data["result"]["data"][keyname]
        # converting extracted data in to dataframe1
        #df_key = DataFrame(list(itertools.chain(keyJSON)))
        df_key = DataFrame(json_normalize(keyJSON))
        # adding jobID to the dataframe
        df_key['occupationID'] = Id

    return df_key

def main():
    # extraction of job title data and appending jobID to it
    jobID = extractID("Data1/occupationsByState.csv")
    # define empty lists
    cert_data = DataFrame()
    eduReq_data = DataFrame()
    expLvl_data = DataFrame()
    occGroup_data = DataFrame()
    occ_data = DataFrame()
    # loop over each resource to get API data by stateAreaID
    for Id in jobID:
        #print Id
        url = ("http://sandbox.api.burning-glass.com/v206/explorer/occupations/"+Id+"?culture=EnglishUS&orderby=Id ASC")
        # get the api data by passing request type, url and authorization parameters
        response = extractAPIData(url)
        # converting the data into json format and extracting relevant information  
        jsonData = json.loads(response)
        # calling function check to see if the key value exists in json data extracted from API
        # function extracts and returs the data
        cert = DataFrame(getKeyData("certifications", jsonData, Id))
        eduReq = DataFrame(getKeyData("educationRequirements", jsonData, Id))
        expLvl = DataFrame(getKeyData("experienceLevels", jsonData, Id))
        occGroup = DataFrame(getKeyData("occupationGroup", jsonData, Id))
        occ = DataFrame(getKeyData("data", jsonData, Id))
        
        
        # appending data
        cert_data = pd.concat([cert_data, cert], axis=0)
        eduReq_data = pd.concat([eduReq_data, eduReq], axis=0)
        expLvl_data = pd.concat([expLvl_data, expLvl], axis=0)
        occGroup_data = pd.concat([occGroup_data, occGroup], axis=0)
        occ_data = pd.concat([occ_data, occ], axis=0)
        
        
    # exporting the data to csv file
    cert_data.to_csv("Data1/certificationsData.csv", encoding='utf-8', index = False)
    eduReq_data.to_csv("Data1/eduRequirementsData.csv", encoding='utf-8', index = False)
    expLvl_data.to_csv("Data1/expLevelData.csv", encoding='utf-8', index = False)
    occGroup_data.to_csv("Data1/occupationGroup.csv", encoding='utf-8', index = False)
    occ_data.to_csv("Data1/OccupationsData.csv", encoding='utf-8', index = False)
#    occ_data.to_excel("Data1/OccupationsData.xlsx", encoding='utf-8', index = False)

if __name__ == "__main__":
    main()