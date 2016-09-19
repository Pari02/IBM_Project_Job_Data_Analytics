# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 09:04:27 2016

@author: Parikshita
@date: 09/18/2016

"""

# import pacakages
import json, itertools, pandas as pd
from pandas import DataFrame
import extractID
from extractID import *
import extractAPIData
from extractAPIData import *

#JOB MARKET DATA, EMPLOYERS, DEGREES
# calling extractID function to get stateAreaID
# which will be used as a parameter to get job market data
stateAreaID = extractID("Data/stateareas.csv")
# loop over each resource to get API data by stateAreaID
resources = ("jobs", "employers", "degrees", "skills", "internships")
for res in resources:
    market_data = []
    for Id in stateAreaID:
        #print Id
        url = ("http://sandbox.api.burning-glass.com/v202/explorer/"+res+"?stateAreaId="+Id+"&culture=EnglishUS&orderby=Id ASC")
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
            # appending data
            market_data.append(df)     
            # concatinating the nested list into a dataframe
            df_new = pd.concat(market_data, axis=0) 
    # exporting the data to csv file
    df_new.to_csv("Data/"+res+"MarketData.csv", encoding='utf-8', index = False)

# JOB JOBTITLE
# extraction of job title data and appending jobID to it
jobID = extractID("Data/jobsMarketData.csv")
job_title = []
# loop over each resource to get API data
for Id in jobID:
    #print Id
    url = ("http://sandbox.api.burning-glass.com/v202/explorer/jobs/"+Id+"/jobtitles?culture=EnglishUS&orderby=Id ASC")
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
        # adding jobID to the dataframe
        df['jobID'] = Id
        # appending data
        job_title.append(df)     
        # concatinating the nested list into a dataframe
        df_new = pd.concat(job_title, axis=0)  
# exporting the data to csv file
df_new.to_csv("Data/job_jobtitle.csv", encoding='utf-8', index = False)

# JOB INTERNSHIP
# extraction of job intership data and appending jobID to it
job_internship = []
# loop over each resource to get API data
for Id in jobID:
    #print Id
    url = ("http://sandbox.api.burning-glass.com/v202/explorer/internships/"+Id+"?culture=EnglishUS&orderby=Id ASC")
    # get the api data by passing request type, url and authorization parameters
    response = extractAPIData(url)
    # converting the data into json format
    # and extracting relevant information  
    jsonData = json.loads(response)
    # applying using list comprehension to avoid key value error
    if[req for req in jsonData if "result" in jsonData] != []:
        reqJSON = [req in jsonData["result"]["data"]]
        # converting extracted data in to dataframe
        df = DataFrame(list(itertools.chain(reqJSON)))
        # appending data
        job_internship.append(df)     
        # concatinating the nested list into a dataframe
        df_new = pd.concat(job_internship, axis=0)  
# exporting the data to csv file
df_new.to_csv("Data/job_internships.csv", encoding='utf-8', index = False)