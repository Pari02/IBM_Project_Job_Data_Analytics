# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 21:24:43 2016

@author: Parikshita
@date: 09/28/2016

"""

# import pacakages
import json, itertools, pandas as pd
from pandas import DataFrame
import extractID
from extractID import *
import extractAPIData
from extractAPIData import *
import job_Cert_EduReq_ExpLvl_Extract
from job_Cert_EduReq_ExpLvl_Extract import *

def extractOccupationtitlesByOccupation(ID):
    occID = ID
    # defining empty dataframe
    data = DataFrame()
    for Id in occID:
        #print Id
        url = ("http://sandbox.api.burning-glass.com/v206/explorer/occupations/"+Id+"/occupationtitles?&culture=EnglishUS&orderby=Id ASC")
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
            # checking for areaId
            if "occupationID" not in df.columns:
                df["occupationID"] = Id
            # concatinating the nested list into a dataframe
            data = pd.concat([data, df], axis=0) 
    # exporting the data to csv file    
    data.to_csv("Data1/occupationtitlesByOccupation.csv", encoding='utf-8', index = False)
    #x.to_excel("Data1/"+res+"ByState.xlsx", encoding='utf-8', index = False)

# function to extract degree data by occupation from API
def extractDegreesByOccupation(ID):
    occID = ID
    # defining empty dataframe
    degree = DataFrame()
    for Id in occID:
        #print Id
        url = ("http://sandbox.api.burning-glass.com/v206/explorer/occupations/"+Id+"/degrees?&culture=EnglishUS&orderby=Id ASC")
        # get the api data by passing request type, url and authorization parameters
        response = extractAPIData(url)
        # converting the data into json format
        # and extracting relevant information  
        jsonData = json.loads(response)
        # applying using list comprehension to avoid key value error
        df = DataFrame(getKeyData("degreeData", jsonData, Id))
        # concatinating the nested list into a dataframe
        degree = pd.concat([degree, df], axis=0) 
    # exporting the data to csv file    
    degree.to_csv("Data1/degreesByOccupation.csv", encoding='utf-8', index = False)
    
# function to extract skill by occupation and state
def extractSkillByOccupation(ID):
    occID = ID
    # defining empty dataframe
    skill = DataFrame()
    skillClus = DataFrame()
    for Id in occID:
        #print Id
        url = ("http://sandbox.api.burning-glass.com/v206/explorer/occupations/"+Id+"/skills?&culture=EnglishUS&orderby=Id ASC")
        # get the api data by passing request type, url and authorization parameters
        response = extractAPIData(url)
        # converting the data into json format
        # and extracting relevant information  
        jsonData = json.loads(response)
        # applying using list comprehension to avoid key value error
        if[req for req in jsonData if "result" in jsonData] != []:
            reqJSON = [req for req in jsonData["result"]["data"]]
            df = DataFrame(list(itertools.chain(reqJSON)))
            df["occupationID"] = Id 
            clus_df = DataFrame(list(df["skillCluster"]))
            clus_df["skillID"] = df["id"]
            clus_df["occupationID"] = Id
            # concatinating the nested list into a dataframe
            skill = pd.concat([skill, df], axis=0) 
            skillClus = pd.concat([skillClus, clus_df], axis = 0)
    del skill["skillCluster"]            
    # exporting the data to csv file    
    skill.to_csv("Data1/skillsByOccupation.csv", encoding='utf-8', index = False)
    skillClus.to_csv("Data1/skillClusterssByOccupationSkill.csv", encoding='utf-8', index = False)
    
def main():
    # calling extractID function to get occupation ID
    # which will be used as a parameter to get occupation titles
    occID = extractID("Data1/OccupationsData.csv")
    extractOccupationtitlesByOccupation(occID)
    extractDegreesByOccupation(occID)


if __name__ == "__main__":
    main()
