#!/usr/bin/env python
import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir

# define methods for listing files (by types) inside NGA dir
def findCSVfiles(nga_wms_path, suffix = ".csv"):
    """ func for finding csv files in the NGA directory """
    path = nga_wms_path
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findXLSXfiles(nga_wms_path, suffix = ".xlsx"):
    """ func for finding xlsx files in the NGA directory """
    path = nga_wms_path
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findXLSfiles(nga_wms_path, suffix = ".xls"):
    """ func for finding xls files in the NGA directory"""
    path = nga_wms_path
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findTXTfiles(nga_wms_path, suffix = ".TXT"):
    """ func for finding TXT files in the specified directory """
    path = nga_wms_path
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

"""
func that calls all the find files functions 
prints all files from find funcs above and displays all csv, xlsx and txt files 
"""

def cat_files(nga_wms_path):
    """ func that prints out file names as a list, without apostrophes """
    CSVs = findCSVfiles(nga_wms_path)
    XLSXs = findXLSXfiles(nga_wms_path)
    XLSs = findXLSfiles(nga_wms_path)
    TXTs = findTXTfiles(nga_wms_path)

    for file1 in CSVs:
        print(file1)

    for file2 in XLSXs:
        print(file2)
    
    for file3 in XLSs:
        print(file3)

    for file4 in TXTs:
        print(file4)
         
