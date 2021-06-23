#!/usr/bin/env python
import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir
import datetime as dt
# import xlrd

# read in the ramotswa inventory data
def read_ramotswa_inventory(nga_wms_path):
    ramotswa_inventory = pd.read_csv(f'{nga_wms_path}/Ramotswa_variables_inventory_20190904.csv',header=0, encoding = 'unicode_escape')
    return ramotswa_inventory

def ramotswa_inventory_info(ramotswa_inventory):
    return ramotswa_inventory.info()

# read in cleaned site_INFORA10A file : basic information of sites, rename and convert to lower case
def clean_inventory_files(nga_wms_path):     
    site_info = pd.read_csv(f'{nga_wms_path}/Cleaned_SITE_INFORA10A.csv')
    # convert commence column to rims date format
    site_info['commence'] = pd.to_datetime(site_info['commence'])
    site_info['commence'] = site_info['commence'].dt.strftime('%Y-%m-%d')
    # convert date format for cease column
    site_info['cease'] = pd.to_datetime(site_info['cease'])
    site_info['cease'] = site_info['cease'].dt.strftime('%Y-%m-%d')
    return site_info

# read in Ramotswa study area 10km file
def read_ramotswa_study_area(nga_wms_path):
    ramotswa_study_area = pd.read_excel(f'{nga_wms_path}/Ramotswa_study_area_plus_10km.xlsx', header=0)
    # lower case column values : ramotswa_study area 
    # Map the lowering function to all column names
    ramotswa_study_area.columns = ramotswa_study_area.columns.str.lower()
    ramotswa_study_area.rename({'descriptio': 'description'},axis=1, inplace = True)
    return ramotswa_study_area
    # save to a csv file
    # ramotswa_study_area.to_csv(f'{nga_wms_path}/merged_Ramotswa_study_area.csv', index = False)