# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 16:52:16 2016

@author: Parikshita
"""

# import pacakages
import requests
from requests_oauthlib import OAuth1
import json, itertools
from pandas import DataFrame


# define & assign parameter values for authorization
consumerKey = "IBM"
consumerSecret = "xxxxx"
tokenKey = "Explorer"
tokenSecret = "xxxxx"
resources = ("careerareas", "stateareas", "jobtitles", "jobs", "internships", "employers", "skills", "skillcategories", "degrees")

# loop over each resource to get API data by criteria
for res in resources:
    print res
    url = ("http://sandbox.api.burning-glass.com/v202/explorer/"+res+"?culture=EnglishUS&orderby=Id ASC")
    # enter authorization parameters
    auth =  OAuth1(consumerKey, consumerSecret, tokenKey, tokenSecret)
    # get the api data by passing request type, url and authorization parameters
    response = requests.request("GET", url, auth=auth).text
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
    
        