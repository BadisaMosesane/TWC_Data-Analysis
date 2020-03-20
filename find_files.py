#!/usr/bin/env python
import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir

# set working directory
os.chdir("/Users/badisa/TWC_Datasets/")

# set file paths
# water levels, water quality and nga area files paths
nga_wms_path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
nga_area_path = ("/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019/")
TWC_datasets = "/Users/badisa/TWC_Datasets/"
merged_files_path = "/Users/badisa/TWC_Datasets/Merged_files/"
shape_files_path = "/Users/badisa/TWC_Datasets/Ramotswa_Shape_files/"

# define methods for listing files (by types) inside NGA dir
def findCSVfiles(path_to_dir, suffix = ".csv"):
    """ func for finding csv files in the NGA directory """
    path = nga_wms_path
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findXLSXfiles(path_to_dir, suffix = ".xlsx"):
    """ func for finding xlsx files in the NGA directory """
    path = nga_wms_path
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findXLSfiles(path_to_dir, suffix = ".xls"):
    """ func for finding xls files in the NGA directory"""
    path = nga_wms_path
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findTXTfiles(path_to_dir, suffix = ".TXT"):
    """ func for finding TXT files in the specified directory """
    path = nga_wms_path
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

"""
func that calls all the find files functions 
prints all files from find funcs above and displays all csv, xlsx and txt files 
"""

# # call all files functions
# def call_file_funcs(): 
#     files_path = nga_wms_path
#     return findCSVfiles(files_path), findXLSXfiles(files_path), findXLSfiles(files_path), findTXTfiles(files_path) 
# call_file_funcs()

# print all files


def cat_files():
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
         
cat_files()
