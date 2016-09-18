# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 08:03:38 2016

@author: Parikshita
@date: 09/18/2016

"""

# import packages
import pandas as pd
import os

# create function to get #id from the csv file
def extractID(filename):
    # reading the file data into variable
    file_data = pd.read_csv(filename, delimiter=",")
    # storing only id information in different variable
    id_data = file_data["id"]
    # define an empty array to store #id later
    id_num = []
    # loop to split each id_data 
    # such that it is broken into two parts 
    # first half in head and leaving #id in tail
    for i in id_data:
        head, tail = os.path.split(i)
        # appending #id to array created earlier
        id_num.append(tail)        
    return id_num
        
    
