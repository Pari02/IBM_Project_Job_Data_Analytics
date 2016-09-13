# -*- coding: utf-8 -*-
"""
@author: Parikshita
@date: 09/12/2016

"""

# import pacakages
import requests
from requests_oauthlib import OAuth1
import json, csv

# define & assign parameter values for authorization
consumerKey = "IBM"
consumerSecret = "xxxxx"
tokenKey = "Explorer"
tokenSecret = "xxxx"
resources = ("careerareas", "stateareas", "jobtitles", "jobs", "internships", "employers", "skills", "skillcategories", "degrees")

# loop over each resource to get API data by criteria
for res in resources:
    print res
    url = ("http://sandbox.api.burning-glass.com/v202/explorer/"+res)
    #url = ("http://sandbox.api.burning-glass.com/v202/explorer/degrees")
    # enter authorization parameters
    auth =  OAuth1(consumerKey, consumerSecret, tokenKey, tokenSecret)
    # get the api data by passing request type, url and authorization parameters
    response = requests.request("GET", url, auth=auth).text
    # converting the data into json format
    # and extracting relevant information   
    jsonData = json.loads(response)
    reqJSON = jsonData["result"]["data"]
    # exporting the data to csv file
    file = open("Data\\"+res+"ByCri.csv", "w")
    csvwriter = csv.writer(file)
    count = 0
    for x in reqJSON:
        if count == 0:
            header = x.keys()
            csvwriter.writerow(header)
            count += 1
        csvwriter.writerow(x.values())
    file.close()
