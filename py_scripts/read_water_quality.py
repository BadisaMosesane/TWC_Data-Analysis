import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir
import datetime as dt
# import xlrd


def merge_water_quality(water_quality_files):
    """ func matches all water quality files inside NGA dir"""
    # look for filenames containing chars "eye", pick the first 5 (those for water quality)
    return water_quality_files

# create water quality dataframe, wq_df means water quality dataframe
# files have different columns, rename cols to the same col_names
# merge all wq files into one water quality file
def create_WQ_df(water_quality_files):
    """ func reads all WQ files & creates water quality dataframe """
    merged_water_quality = pd.DataFrame()
    for filename in water_quality_files:
        # with open(filename, "rb") as f:
        wq_df = pd.read_excel(filename, header = None, parse_dates = True)
        wq_df['name'] = wq_df.iloc[0,1][0:6]
        wq_df = wq_df.iloc[4:, :]
        wq_df.columns = ['date_time','mean_discharge_cumecs', 'w_quality', 'name'] # cumecs ~ Cubic metres per second measure for water discharge
        merged_water_quality = merged_water_quality.append(wq_df, ignore_index = True)
    return merged_water_quality

# rename water quality 'name' field to bh_id
def rename_cols(water_quality_df):
    water_quality_df.rename(columns={'name': 'bh_id', 'date_time': 'date'}, inplace = True)
    #formatting water_quality dates
    water_quality_df['date'] = water_quality_df['date'].astype(str)
    water_quality_df['new_date'] = water_quality_df['date'].str[:10]
    water_quality_df['time'] = water_quality_df['date'].str[11:]
    water_quality_df['date'] = water_quality_df['new_date']
    water_quality_df = water_quality_df.drop('new_date', axis = 1)
    return water_quality_df

def map_quality_code(water_quality_df):
    # map water quality code to meaningful description
    # TO-DO look for an automated map, inside file
    water_quality_df['quality_desc'] = water_quality_df['w_quality'].map({
        255: 'Missing data', 26: 'Audited Gauge Plate Readings / dip level readings',
    170: 'Period of No Record (PNR)', 1: 'Good continuous data', 64: 'Audited Estimate',
    2: 'Good edited data', 151: 'Data Missing', 47: 'Edited and checked\044 still unaudited',
    44: 'Checked\044 still unaudited', 60: 'Above Rating'   
    })
    return water_quality_df


def insert_datasource(merged_water_quality):
    # Insert data source details i.e. data owner, contact, email and file name
    merged_water_quality['data_owner'] = "Department of Water and Sanitation South Africa"
    merged_water_quality['contact_person'] = "Ramusiya, Fhedzisani"
    merged_water_quality['email'] = "RamusiyaF@dws.gov.za"
    merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H001'), 'file_name'] = 'A1H001 Upper Eye Dinokana.xlsx'
    merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H002'), 'file_name'] = 'A1H002 Lower Eye Dinokana.xlsx'
    merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H003'), 'file_name'] = 'A1H003 Upper Eye Tweefontein.xlsx'
    merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H004'), 'file_name'] = 'A1H004 Lower Eye Tweefontein.xlsx'
    merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H005'), 'file_name'] = 'A1H005 Skilpad Eye.xlsx'
    return merged_water_quality

# create a WLWQ dataframe by merging water levels & water quality DFs
def create_WLWQ_df(merged_RIMS_water_levels, merged_water_quality):
    merged_WLWQ_df = pd.concat([merged_RIMS_water_levels,merged_water_quality], sort=False)
    return merged_WLWQ_df