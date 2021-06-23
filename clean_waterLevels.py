#!/usr/bin/env python
import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir
import datetime as dt

"""
Inspecting & Cleaning Water Levels 
# Enhance the datasets to conform to the RIMS standard. 
# Minimally RIMS requires datasets with bh_id, country, latitude and longitude. 
"""
def format_station_names(cleaned_water_levels):
    """ func formats st8n names, removes spaces in the station names, 'A3N0513 ' -> 'A3N0513' """
    print("\n station names before formatting: \n {}".format(cleaned_water_levels['station'].unique()))
    cleaned_water_levels['station'] = cleaned_water_levels['station'].str.replace(' ', '')
    return ("\n station names after formatting: \n {}".format(cleaned_water_levels ['station'].unique()))

# convert water quality code to a descriptive name
def conv_wq_code(cleaned_water_levels):
    """ convert quality code to a meaningful code i.e. 93:'Dry borehole' """
    print("\n Water Quality codes before converting: \n {}".format(cleaned_water_levels['quality'].unique()))
    cleaned_water_levels['quality'] = cleaned_water_levels['quality'].map({26: 'Audited Gauge Plate Readings / dip level readings', 93: 'Dry borehole', 1: 'Good continuous data'})
    return ("\n Water Quality codes after converting: \n {}".format(cleaned_water_levels['quality'].unique()))

# convert datatrans to a descriptive meaning
def conv_datatrans(cleaned_water_levels):
    print("\n Data trans code before converting: \n {}".format(cleaned_water_levels['datatrans'].unique()))
    cleaned_water_levels['datatrans'] = cleaned_water_levels['datatrans'].map({7: 'Point data, no interpolaton - Monthly readings, hand measurements',})
    return ("\n Data trans converted to a meaning: \n {}".format(cleaned_water_levels['datatrans'].unique()))


# concatenate comment and unnamed columns
def concat_cols(cleaned_water_levels):
    cleaned_water_levels = cleaned_water_levels.replace(np.NaN, ' ')
    # combine 'comment' and 'unnamed: 7' into one column
    cleaned_water_levels['comment'] = cleaned_water_levels['comment'] + cleaned_water_levels['unnamed: 7']
    return cleaned_water_levels

# use to_datetime to convert date to correct format
def date_to_rims_format(cleaned_water_levels):
    """func returns date in RIMS format """
    cleaned_water_levels['date'] = pd.to_datetime(cleaned_water_levels['date'], format = "%Y%m%d").dt.strftime('%Y-%m-%d')
    return cleaned_water_levels

# convert water levels to absolute values, non negative water levels
def wl_to_abs(cleaned_water_levels):
    """ converts water levels to absolute values """
    cleaned_water_levels['water_level'] = cleaned_water_levels['water_level'].abs()
    return cleaned_water_levels['water_level']

# Insert file name to the datasets
def insert_filenames(cleaned_water_levels):
    """ func inserts filename to the datasets """
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A1N0001'), 'file_name'] = 'A1N0001_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A1N0002'), 'file_name'] = 'A1N0002_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A1N0003'), 'file_name'] = 'A1N0003_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A3N0015'), 'file_name'] = 'A3N0015_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A3N0513'), 'file_name'] = 'A3N0513_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A3N0514'), 'file_name'] = 'A3N0514_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A3N0516'), 'file_name'] = 'A3N0516_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A3N0519'), 'file_name'] = 'A3N0519_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('A3N0521'), 'file_name'] = 'A3N0521_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('D4N1468'), 'file_name'] = 'D4N1468_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('D4N1658'), 'file_name'] = 'D4N1658_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('D4N1666'), 'file_name'] = 'D4N1666_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('D4N2515'), 'file_name'] = 'D4N2515_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('D4N2516'), 'file_name'] = 'D4N2516_WaterLevels.csv'
    cleaned_water_levels.loc[cleaned_water_levels['station'].str.contains('D4N2517'), 'file_name'] = 'D4N2517_WaterLevels.csv'
    return cleaned_water_levels['file_name']


def append_to_rims(cleaned_water_levels):
    """ func appends to RIMS structure, adds all needed fields to the dataframe """
    RIMS_water_levels = pd.DataFrame({'bh_id':[], 'swl_date':[], 'time':[], 'country':[], 'latitude':[],
                                      'longitude':[], 'swl_m':[], 'quality':[], 'datatrans':[],
                                      'comment_water_level':[], 'data_owner':[], 'contact_person':[],'email':[],
                                      'file_name':[]})
    # cleaned_water_levels = water_levels.copy()
    RIMS_water_levels['bh_id'] = cleaned_water_levels['station']
    RIMS_water_levels['swl_date'] = cleaned_water_levels['date']
    RIMS_water_levels['time'] = cleaned_water_levels['time']
    RIMS_water_levels['country'] = 'South Africa'
    RIMS_water_levels['swl_m'] = cleaned_water_levels['water_level'] 
    RIMS_water_levels['quality'] = cleaned_water_levels['quality']
    RIMS_water_levels['datatrans'] = cleaned_water_levels['datatrans']
    RIMS_water_levels['comment_water_level'] = cleaned_water_levels['comment']
    RIMS_water_levels['data_owner'] = "Department of Water and Sanitation South Africa"
    RIMS_water_levels['contact_person'] = "Ramusiya, Fhedzisani"
    RIMS_water_levels['email'] = "RamusiyaF@dws.gov.za"
    RIMS_water_levels['file_name'] = cleaned_water_levels['file_name']
    return RIMS_water_levels