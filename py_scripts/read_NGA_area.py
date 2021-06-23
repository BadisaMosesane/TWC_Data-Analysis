#!/usr/bin/env python

import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir
import datetime as dt
# import xlrd

"""
TO_DO: fix filename, only assigns waterLevels_wbw..._DataExport to all filenames in the DF
loop through filenames and trace where the record came from (as in which file)
"""

# merge all NGA data for the area files
def load_NGA_area_datasets(nga_area_path):
    # os.chdir(nga_area_path)
    nga_files = glob.glob("/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019/*.csv")
    nga_data = pd.DataFrame()
    nga_area_DF = [pd.read_csv(f, header=0, sep=",", encoding = 'unicode_escape') for f in nga_files]
    nga_data = pd.concat(nga_area_DF,ignore_index=True)
    # add source details to each record
    filenames = [os.path.basename(filep) for filep in (glob.glob("/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019/*.csv"))]    
    for file in filenames:
        if file.endswith('.csv'):
            nga_data['file_name'] = file
            nga_data['data_owner'] = "Department of Water and Sanitation South Africa"
            nga_data['contact_person'] = "Ramusiya, Fhedzisani"
            nga_data['email'] = "RamusiyaF@dws.gov.za" 
    return nga_data

# make a new df, copy of nga dataset and rename cols
# making a copy helps when renaming cols, avoids renaming conflicts
def clean_nga_dataset(nga_data):
    clean_nga_dataset = nga_data.copy()
    col_names = list(nga_data.columns)
    new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
    new_col_names = [x.lower() for x in new_col_names]
    dictionary = dict(zip(col_names, new_col_names))
    clean_nga_dataset = clean_nga_dataset.rename(columns = dictionary)
    clean_nga_dataset = clean_nga_dataset.drop_duplicates()
    clean_nga_dataset = clean_nga_dataset.rename(columns = {'identifier':'bh_id',
                                                    'geositetype':'bh_type',
                                                    'referencedatum':'geod_datum',
                                                    'waterlevel_waterlevelstatus':'comment_waterlevel',
                                                    'waterlevel_measurementdateandtime':'swl_date',
                                                    'waterlevel_waterlevel':'swl_m',
                                                    'waterlevel_reportinginstitution':'gwl_dbase'})
    # clean_nga_dataset = clean_nga_dataset.drop(columns='dataowner', inplace=True)
    return clean_nga_dataset

 # strip ls at the end of yieldtest and  discharge rate entries
def clean_column_entries(clean_nga_dataset):
    clean_nga_dataset = clean_nga_dataset.drop(columns='dataowner')
    clean_nga_dataset['yieldtest_dischargerate'] = clean_nga_dataset['yieldtest_dischargerate'].str[0:5]
    clean_nga_dataset['dischargerate_dischargerate'] = clean_nga_dataset['dischargerate_dischargerate'].str[0:5]
    clean_nga_dataset['depthdiameter_diameter'] = clean_nga_dataset['depthdiameter_diameter'].str[0:3]
    return clean_nga_dataset

# merged the two main dataframes; merged_WLWQ_df, and clean_nga_dataset
def create_master_DF(merged_WLWQ_df, clean_nga_dataset):
    # remove duplicate column names in the clean_nga_dataset
    print("final dataframe")
    clean_nga_dataset = clean_nga_dataset.loc[:,~clean_nga_dataset.columns.duplicated()]
    # merge the dataframes
    master_dataset = pd.concat([merged_WLWQ_df, clean_nga_dataset], axis = 0, join='outer')
    return master_dataset


        
