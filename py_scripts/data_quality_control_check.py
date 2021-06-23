#!/usr/bin/env python

import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir
import datetime as dt
# import xlrd

import read_water_levels

# RIMS required data from RIMS templates: 
# all columns needed for upload to RIMS

nga_wms_path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"

RIMS_required_data = ['bh_id', 'country', 'latitude', 'longitude', 'elevation', 'bh_type', 'bh_depth', 'compl_date', 'bh_status', 'swl_m', 'swl_date', 'yield_l_s', 'yieldtype',
    'yield_date', 'ec_ms_m', 'ec_date', 'tds_mg_l', 'tds_date', 'f_mg_l', 'f_date', 'no3_mg_l', 'no3_date', 'lith', 'lith_dbase','lith_id', 'gwl_series',
    'gwl_dbase', 'gwl_id', 'gwl_strt', 'gwl_end', 'gwq_series', 'gwq_freq', 'gwq_db', 'gwq_id', 'abs_use', 'abs_avg', 'abs_rely', 'abs_series', 'abstr_db',
    'abstr_id', 'abstr_strt', 'abstr_end', 'geositetype', 'quaternarydrainageregion', 'municipaldistrictnew', 'hydrogeologicalregion', 'farm',
    'farmnumber', 'dataowner', 'watermanagementarea', 'tasteofwater', 'observedactualwateruses', 'HYDSTRA-id', 'Water_quality', 'MFID-id']

# create a dataframe with RIMS_absolute_minimum_required_data
# mimimum inlcluded 4 as of now; 'bh_id', 'country', 'latitude', 'longitude'

# create a dataframe for RIMS abs min required data 
def create_RIMS_MinrequiredData_df(RIMS_required_data):
    RIMS_absolute_minimum_required_data = pd.DataFrame(columns = ['bh_id', 'country', 'latitude', 'longitude'])
    return RIMS_absolute_minimum_required_data.columns

# def populate_df(RIMS_absolute_minimum_required_data):
#     RIMS_absolute_minimum_required_data['bh_id'] = 'BD2020'
#     RIMS_absolute_minimum_required_data['country'] = 'South Africa'
#     RIMS_absolute_minimum_required_data['latitude'] = -25.19358056
#     RIMS_absolute_minimum_required_data['longitude'] = 25.80122222
#     return RIMS_absolute_minimum_required_data

# check RIMS minumread in a file, check which columns are there, display message to terminal
def check_Minrequired_columns(RIMS_absolute_minimum_required_data):
    water_level_filenames = (glob.glob(f'{nga_wms_path}/*_WaterLevels.csv'))  
    # water_level_filenames.rename(columns = lambda x: x.strip().lower(), inplace = True)
    dfs = [pd.read_csv(file) for file in water_level_filenames]
    num_dfs = list(range(len(dfs)))
    first_file_cols = dfs[0].columns
    first_file_cols.rename(columns = lambda x: x.strip().lower(), inplace = True)
    second_file_cols = dfs[1].columns
    third = dfs[2].columns
    # print(dfs[1].columns)
    
    # check if all RIMS min Req cols are there in each dataframe
    # check_min_req_cols = set(RIMS_absolute_minimum_required_data.columns).intersection(set(first_file_cols))

    return first_file_cols



def main(): 
    # RIMS_absolute_minimum_required_data['bh_id'] = 'BD2020'
    # RIMS_absolute_minimum_required_data['country'] = 'South Africa'
    # RIMS_absolute_minimum_required_data['latitude'] = -25.19358056
    # RIMS_absolute_minimum_required_data['longitude'] = 25.80122222

    water_level_fnames = read_water_levels.find_water_levels(nga_wms_path)
    print(water_level_fnames)

    print("\n minimum required data [bh_id, country, latitude, longitude] ...\n")
    # RIMS_required_cols = create_RIMS_MinrequiredData_df(RIMS_required_data)
    # print(RIMS_required_cols)

    min_required_cols = check_Minrequired_columns(RIMS_required_data)
    print(min_required_cols)
   
if __name__ == '__main__':
    main()


"""
Dealing with Thresholds
"""

# define thresholds

# def thresholds(n):
#     if n > 0.5:
#         return 'too low'
#     elif 2<n<=4:
#         return 'good reading'
#     else:
#         return 'too high'
    
#     water_levels_df['Threshold'] = water_levels_df['swl'].apply(thresholds)


# def swl_thresholds(n):
#     if master_dataset.swl > 0.5:
#         return 'too low'
#     elif 2<n<=4:
#         return 'good reading'
#     else:
#         return 'too high'
    
#     master_dataset['swl_Thresholds'] = water_levels_df['swl'].apply(thresholds)
    
