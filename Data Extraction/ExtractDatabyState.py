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
def extractByState():
    # calling extractID function to get stateAreaID
    # which will be used as a parameter to get job market data
    stateAreaID = extractID("Data1/stateareas.csv")
    # loop over each resource to get API data by stateAreaID
    #resources = ("occupations", "employers", "degrees", "skills", "internships")
    resources = ("internships", "employers", "occupations")
    for res in resources:
        # defining empty dataframe
        data = DataFrame()
        res = "occupations"
        for Id in stateAreaID:
            #print Id
            url = ("http://sandbox.api.burning-glass.com/v206/explorer/"+res+"?areaId="+Id+"&culture=EnglishUS&orderby=Id ASC")
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
                # removing occupationGroup from occupations for this table
                if "occupationGroup" in df.columns:
                    del df["occupationGroup"]
                # checking for areaId
                if "areaId" not in df.columns:
                    df["areaId"] = Id
                #df = df.fillna("NA")
                # concatinating the nested list into a dataframe
                data = pd.concat([data, df], axis=0) 
        # exporting the data to csv file    
        data.to_csv("Data1/"+res+"ByState.csv", encoding='utf-8', index = False)
        #x.to_excel("Data1/"+res+"ByState.xlsx", encoding='utf-8', index = False)

def main():
    extractByState()


if __name__ == "__main__":
    main()