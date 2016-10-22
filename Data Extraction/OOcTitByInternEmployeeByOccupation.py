# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 22:57:45 2016

@author: Parikshita
@date: 09/28/2016

"""

# import pacakages
import json, itertools, pandas as pd
from pandas.io.json import json_normalize
from pandas import DataFrame
import extractID
from extractID import *
import extractAPIData
from extractAPIData import *
import job_Cert_EduReq_ExpLvl_Extract
from job_Cert_EduReq_ExpLvl_Extract import *

def extractEmpByOccupationState(occID, areaId):
    # defining empty dataframe
    data = DataFrame()
    for i in areaId:
        for Id in occID:
            #print Id
            url = ("http://sandbox.api.burning-glass.com/v206/explorer/occupations/"+Id+"/employers?areaId="+i+"&culture=EnglishUS&orderby=Id ASC")
            # get the api data by passing request type, url and authorization parameters
            response = extractAPIData(url)
            # converting the data into json format
            # and extracting relevant information  
            jsonData = json.loads(response)
            # applying using list comprehension to avoid key value error
            if[req for req in jsonData if "result" in jsonData] != []:
                reqJSON = [req for req in jsonData["result"]["data"]]
                # converting extracted data in to dataframe
                df = DataFrame(list(itertools.chain(reqJSON)))
                # checking for areaId and occupationID
                df["areaId"] = i
                df["occupationID"] = Id
                # concatinating the nested list into a dataframe
                data = pd.concat([data, df], axis=0) 
    # exporting the data to csv file    
    data.to_csv("Data1/EmployersByOccupation.csv", encoding='utf-8', index = False)
    #x.to_excel("Data1/"+res+"ByState.xlsx", encoding='utf-8', index = False)

def extractInternshipByOccupationState(internID):
    # defining empty dataframe
    occTitle_data = DataFrame()
    for Id in internID:
        #print Id
        url = ("http://sandbox.api.burning-glass.com/v206/explorer/internships/"+Id+"?culture=EnglishUS&orderby=Id ASC")
        # get the api data by passing request type, url and authorization parameters
        response = extractAPIData(url)
        # converting the data into json format
        # and extracting relevant information  
        jsonData = json.loads(response)
        # applying using list comprehension to avoid key value error
        if[req for req in jsonData if "result" in jsonData] != []:
            reqJSON = jsonData["result"]["data"]
            # converting extracted data in to dataframe
            intern_df = DataFrame(json_normalize(reqJSON))
            occTitle_df = DataFrame(list(itertools.chain.from_iterable(intern_df["occupationTitles"])))
            # adding new fields: areaId and occupationID
            occTitle_df["internshipID"] = Id
            # concatinating the nested list into a dataframe
            occTitle_data = pd.concat([occTitle_data, occTitle_df], axis=0)
    # exporting the data to csv file    
    occTitle_data.to_csv("Data1/occupationTitlesByInternship.csv", encoding='utf-8', index = False)
    
    
def main():
    occID = extractID("Data1/occupationsByState.csv")
    areaId = extractID("Data1/stateareas.csv")
    internID = extractID("Data1/internshipsbyState.csv")
    extractEmpByOccupationState(occID, areaId)
    extractInternshipByOccupationState(internID)

if __name__ == "__main__":
    main()

