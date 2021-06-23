#!/usr/bin/env python
import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir

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
