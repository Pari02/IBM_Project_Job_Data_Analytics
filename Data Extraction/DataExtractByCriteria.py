# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 16:52:16 2016

@author: Parikshita
@date: 09/12/2016

"""

# import pacakages
import json, itertools
from pandas import DataFrame
import extractAPIData
from extractAPIData import *

resources = ("careerareas", "stateareas", "jobtitles", "skillcategories")

# loop over each resource to get API data by criteria
for res in resources:
    print res
    url = ("http://sandbox.api.burning-glass.com/v202/explorer/"+res+"?culture=EnglishUS&orderby=Id ASC")
    # get the API data 
    response = extractAPIData(url)
    # converting the data into json format
    # and extracting relevant information  
    jsonData = json.loads(response)
    # applying using list comprehension to avoid key value error
    if[req for req in jsonData if "result" in jsonData] != []:
        reqJSON = [req for req in jsonData["result"]["data"]]
        # converting extracted data in to dataframe
        df = DataFrame(list(itertools.chain(reqJSON)))
        # exporting the data to csv file
        df.to_csv("Data/"+res+".csv", encoding='utf-8', index = False)
    
        