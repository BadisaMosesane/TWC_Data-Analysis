#!/usr/bin/env python

import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir
import datetime as dt
import matplotlib.pyplot as plt

# Get coordinates (Lat & Longs) of stations
# rename the col to lower case
# drop duplicates

def load_coordinates(bh_coordinates):
    # bh_coordinates = pd.read_csv(f'{nga_wms_path}/SITE_INFORA10A_fixed.csv', usecols=['#STATION','LATITUDE', 'LONGITUDE'])
    bh_coordinates.rename(columns = {'#STATION': 'bh_id', 'LATITUDE': 'latitude', 'LONGITUDE': 'longitude'}, inplace = True)
    bh_coordinates.drop_duplicates(inplace = True)
    return bh_coordinates


def check_coords(bh_coordinates, RIMS_water_levels):
    pd.options.mode.chained_assignment = None
    coords = bh_coordinates[['bh_id','latitude','longitude']]
    rims_bhid = list(RIMS_water_levels['bh_id'].unique())
    # check if coordinates isin rims BH_ID
    coord_rims = coords[coords['bh_id'].isin(rims_bhid) == True]
    coord_rims.drop_duplicates(inplace = True)
    return coords

# add coordinates to the water levels dataframe
def add_coordinates(append_rims_wl, coords1):
    merged_RIMS_water_levels = append_rims_wl.merge(coords1, on = 'bh_id')
    return merged_RIMS_water_levels

# rename coordinates cols (latitude_y and longitude_y) to desired names and 
# add bh coordinates to the RIMS_water_levels dataframe   
def clean_coordinates(merged_RIMS_water_levels):
    """method renames the lat_y and longi_y to lat and long, drops lat_x & lat_y"""
    merged_RIMS_water_levels.drop(['latitude_x', 'longitude_x'], axis=1, inplace = True )
    merged_RIMS_water_levels.rename(columns = {'latitude_y':'latitude', 'longitude_y':'longitude'}, inplace=True)
    return merged_RIMS_water_levels

# plot water levels for each bhID 
# def plot_waterLevels(merged_RIMS_water_levels):
#     A3N0015 = merged_RIMS_water_levels[merged_RIMS_water_levels['bh_id'] == 'A3N0015']
#     A3N0015.plot.line(x='swl_date', y='swl_m', figsize=(18,6), lw = 1)
#     plt.title('A3N0015 Borehole', fontsize=16)
#     plt.xlabel('Date', fontsize = 12)
#     plt.ylabel('Static Water Level', fontsize = 12)

