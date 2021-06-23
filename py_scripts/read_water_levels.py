#!/usr/bin/env python
import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir

"""
Read in files, merge similar files, chunk water levels together, water_quality together etc
Use glob func to match files with similar names, match all waterLevel files and sort them 
"""

def find_water_levels(nga_wms_path):
    """ matches all waterLevel files """
    water_level_filenames = [os.path.basename(file2) for file2 in (glob.glob(f'{nga_wms_path}/*_WaterLevels.csv'))]
    print("\n Listing water levels files ...")
    print("\n", water_level_filenames)

# create water levels dataframe      
def water_levels_df(nga_wms_path):
    """ func creates a water levels dataframe """
    water_level_filenames = (glob.glob(f'{nga_wms_path}/*_WaterLevels.csv'))  
    water_levels = pd.DataFrame()
# append each water_level file to the dataframe
    for wl_file in water_level_filenames:
        wl_df = pd.read_csv(wl_file, parse_dates = True, date_parser = True)
        wl_df.rename(columns = lambda x: x.strip().lower(), inplace = True)
        water_levels = water_levels.append(wl_df, ignore_index = True, sort = False)
    return water_levels
    
def unique_BHID(wl_dataframe):
    """ returns unique bh_id's for the water levels dataframe """
    # print('\n Unique station IDs: \n {}'.format(wl_dataframe['station'].unique()))
    print("\n Unique Borehole IDs ...")
    return wl_dataframe['station'].unique()
    
# inspect water levels info
def inspect_waterLevels(wl_dataframe):
    return wl_dataframe.info()
    
# generate descriptive statistics
def water_levels_stats(wl_dataframe):
    return wl_dataframe.describe()
    


