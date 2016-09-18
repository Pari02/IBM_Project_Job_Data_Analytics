# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 12:27:56 2016

@author: Parikshita
@date: 09/18/2016

"""

# import pacakages
import requests
from requests_oauthlib import OAuth1

# funtion to pass url name to extract data from API
def extractAPIData(url):
    # define & assign parameter values for authorization
    consumerKey = "IBM"
    consumerSecret = "xxxx"
    tokenKey = "Explorer"
    tokenSecret = "xxxx"
    
    # enter authorization parameters
    auth =  OAuth1(consumerKey, consumerSecret, tokenKey, tokenSecret)
    # get the api data by passing request type, url and authorization parameters
    response = requests.request("GET", url, auth=auth).text
    
    return response