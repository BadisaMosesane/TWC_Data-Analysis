#!/usr/bin/env python

import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir
import datetime as dt
import matplotlib.pyplot as plt
# import xlrd


# import twc water levels modules
import find_files
import read_water_levels
import clean_waterLevels
import read_coordinates
import read_water_quality
import load_ramotswa_datasets
import load_ramotswa_inventory
import read_NGA_area

pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)

# set file paths: water levels, water quality, nga area and shape files paths
nga_wms_path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
nga_area_path = ("/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019/")
TWC_datasets = "/Users/badisa/TWC_Datasets/"
merged_files_path = "/Users/badisa/TWC_Datasets/Merged_files/"
shape_files_path = "/Users/badisa/TWC_Datasets/Ramotswa_Shape_files/"

# pattern match water level filenames using glob
# water_level_filenames = [os.path.basename(file2) for file2 in (glob.glob('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/*_WaterLevels.csv'))]
water_level_filenames = (glob.glob(f'{nga_wms_path}/*_WaterLevels.csv'))
#create water levels dataframe
water_levels = pd.DataFrame()
for wl_file in water_level_filenames:
    wl_df = pd.read_csv(wl_file, parse_dates = True, date_parser = True)
    wl_df.rename(columns = lambda x: x.strip().lower(), inplace = True)
    water_levels = water_levels.append(wl_df, ignore_index = True, sort = False)

cleaned_water_levels = water_levels.copy()

# create a RIMS water level dataframe
RIMS_water_levels = pd.DataFrame({'bh_id':[], 'swl_date':[], 'time':[], 'country':[], 'latitude':[],
                                      'longitude':[], 'swl_m':[], 'quality':[], 'datatrans':[],
                                      'comment_water_level':[], 'data_owner':[], 'contact_person':[],'email':[],
                                      'file_name':[]})


# load site info file with coordinates 
bh_coordinates = pd.read_csv(f'{nga_wms_path}/SITE_INFORA10A_fixed.csv', usecols=['#STATION','LATITUDE', 'LONGITUDE'])

# coords = bh_coordinates[['bh_id','latitude','longitude']]

water_quality_filenames = sorted(glob.glob(f'{nga_wms_path}/*[eye]*.xlsx'))
water_quality_files = water_quality_filenames[:5]

# loading in ramotswa datasets
ramotswa_1_df = pd.read_csv(f'{nga_wms_path}/Ramotswa_data_1_20190904.csv', header = 0, encoding = 'unicode_escape')
ramotswa_2_df = pd.read_csv(f'{nga_wms_path}/Ramotswa_data_2_20190904.csv', header = 0, encoding = 'unicode_escape')
rams1 = ramotswa_1_df.copy()
    
# call water levels df
def main():
    # all_files = find_files.cat_files(nga_wms_path)
    # print(all_files)
    wl_dataframe = read_water_levels.water_levels_df(nga_wms_path)  
    unique_ids = read_water_levels.unique_BHID(wl_dataframe)
    print("\n",unique_ids)

    print("\n Now building the water levels DataFrame ...\n")
    print(wl_dataframe)

    print("\n see Water levels Info ...\n")
    inspect_wl = read_water_levels.inspect_waterLevels(wl_dataframe)
    print(inspect_wl)

    print("\n Generating Descriptive statistics that summarize the wl dataset ...\n")
    wl_stats = read_water_levels.water_levels_stats(wl_dataframe)
    print(wl_stats)

    """
    pre-processing and cleaning data
    remove trailling spaces in the station names
    """
    print("\n Removing spaces at the end of station names ...")
    
    formatted_stations = clean_waterLevels.format_station_names(cleaned_water_levels)
    print(formatted_stations)

    print("\n convert quality code to a meaningful code i.e. 93:'Dry borehole ...")
    formatted_wq = clean_waterLevels.conv_wq_code(cleaned_water_levels)
    print(formatted_wq)

    print("\n convert datatrans code to a meaningful code i.e. 7: 'Point data, no interpolaton  ...")
    formatted_datatrans = clean_waterLevels.conv_datatrans(cleaned_water_levels)
    print(formatted_datatrans)

    print("\n formatting dates to the required RIMS format  ...\n")
    formatted_date = clean_waterLevels.date_to_rims_format(cleaned_water_levels)
    print(formatted_date)

    print("\n convert water levels to absolute values ...\n")
    formatted_swl = clean_waterLevels.wl_to_abs(cleaned_water_levels)
    print(formatted_swl)

    print("\n adding file names to the datasets ...\n")
    formatted_fnames = clean_waterLevels.insert_filenames(cleaned_water_levels)
    print(formatted_fnames)

    print("\n Appending to RIMS water levels ...\n")
    append_rims_wl = clean_waterLevels.append_to_rims(cleaned_water_levels)
    print(append_rims_wl)

    print("\n Reading coordinates for each Borehole ID ...\n")
    coordinates = read_coordinates.load_coordinates(bh_coordinates)
    print(coordinates)

    print("\n Checking coordinates in RIMS ...\n")
    coords1 = read_coordinates.check_coords(bh_coordinates, RIMS_water_levels)
    print(coords1)

    print("\n Adding coordinates in RIMS ...\n")
    merged_RIMS_water_levels = read_coordinates.add_coordinates(append_rims_wl, coords1)
    print(merged_RIMS_water_levels)

    print("\n Renaming coordinates columns ...\n")
    merged_RIMS_water_levels = read_coordinates.clean_coordinates(merged_RIMS_water_levels)
    print(merged_RIMS_water_levels)
   
    # print("\n Plotting water levels")
    # read_coordinates.plot_waterLevels(merged_RIMS_water_levels)

    print("\n Merging water quality files ...\n")
    merge_water_qual = read_water_quality.merge_water_quality(water_quality_files)
    print(merge_water_qual)

    print("\n Creating water quality dataframe ...\n")
    water_quality_df = read_water_quality.create_WQ_df(water_quality_files)
    print(water_quality_df)

    print("\n Renaming water quality column names ...\n")
    merged_water_quality = read_water_quality.rename_cols(water_quality_df)
    print(merged_water_quality)

    print("\n Map water quality code to meaningful description ...\n")
    merged_water_quality = read_water_quality.map_quality_code(merged_water_quality)
    print(merged_water_quality)

    print("\n Inserting datasource details for WQ df ...\n")
    merged_water_quality = read_water_quality.insert_datasource(merged_water_quality)
    print(merged_water_quality)

    print("\n Merging water levels & water quality dataframes ...\n")
    merged_WLWQ_df = read_water_quality.create_WLWQ_df(merged_RIMS_water_levels, merged_water_quality)
    print(merged_WLWQ_df)

    print("\n loading ramotswa datasets ...\n")
    ramotswa_datasets = load_ramotswa_datasets.read_ramotswa_datasets(nga_wms_path)
    print(ramotswa_datasets)

    print("\n cleaned ramotswa datasets info...\n")
    cleaned_rams1 = load_ramotswa_datasets.clean_rams1(nga_wms_path)
    print(cleaned_rams1)

    print("\n Wrangling ramotswa 2 datasets info...\n")
    cleaned_rams2 = load_ramotswa_datasets.clean_rams2(nga_wms_path)
    print(cleaned_rams2)

    print("\n Merging ramotswa datasets into final dataframe...\n")
    merged_ramotswa = load_ramotswa_datasets.merge_DFs(cleaned_rams1,cleaned_rams2)
    print(merged_ramotswa)

    print("\n read Ramotswa inventory files ...\n")
    ramotswa_inventory = load_ramotswa_inventory.read_ramotswa_inventory(nga_wms_path)
    print(ramotswa_inventory)

    print("\n Ramotswa inventory info ...\n")
    ramotswa_inventory_info = load_ramotswa_inventory.ramotswa_inventory_info(ramotswa_inventory)
    print(ramotswa_inventory_info)


    print("\n Reading site info coords ...\n")
    clean_inventory = load_ramotswa_inventory.clean_inventory_files(nga_wms_path)
    print(clean_inventory)


    print("\n Reading Ramotswa study area ...\n")
    rams_study_area = load_ramotswa_inventory.read_ramotswa_study_area(nga_wms_path)
    print(rams_study_area)

    print("\n Loading NGA area datasets ...\n")
    nga_data = read_NGA_area.load_NGA_area_datasets(nga_wms_path)
    print(nga_data)

    print("\n Adding source details to NGA area datasets ...\n")
    nga_data = read_NGA_area.load_NGA_area_datasets(nga_wms_path)
    print(nga_data)

    print("\n Cleaning NGA area datasets ...\n")
    clean_nga_dataset = read_NGA_area.clean_nga_dataset(nga_data)
    print(clean_nga_dataset)

    print("\n Cleaning columns entries ...\n")
    clean_nga_dataset = read_NGA_area.clean_column_entries(clean_nga_dataset)
    print(clean_nga_dataset)

    print("\n Creating master TWC dataframe ...\n")
    master_df = read_NGA_area.create_master_DF(merged_WLWQ_df,clean_nga_dataset)
    print(master_df)

    print("\n Saved final dataset twc_master_dataset.csv to Users/badisa/TWC_Datasets/merged_files  ...\n")
    final_dataset = master_df.to_csv(f'{merged_files_path}/twc_master_dataset.csv', index=False)
    # print(final_dataset)


if __name__ == "__main__":
    main()
