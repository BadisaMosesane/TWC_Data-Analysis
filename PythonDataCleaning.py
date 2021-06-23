#!/usr/bin/env python
# coding: utf-8

# # Exploratory Data Analysis for the TWC datasets from NGA and WMS
# - An attempt for a step-by-step data cleaning process leading to an EDA datapipeline & additional ML workflow & Use case
# 

# # Dev Notes 
# 
# - Start thinking about writing code as classes | beyond notebooks, some form of an app to be deployed
# - Write functions as pure functions, clojure
# - Do a config block at the top | specify how one will run the notebook, spec file_paths
# - Pass arguments to functions i.e. call a func and prompt user to put in file_path
# - Patent for groundwater collection and processing | collect and spin to an HPC machine that processes at high speeds
# - Read files into memory

# In[1]:


# Import Libraries
import pandas as pd
import numpy as np
import os
import re
import glob
from os import listdir
import geopandas as gpd
from shapely.geometry import Point, Polygon

import matplotlib.pyplot as plt
import seaborn as sns
get_ipython().run_line_magic('reload_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')
get_ipython().run_line_magic('matplotlib', 'inline')

pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)


# In[2]:


# get the current working directory
print(os.getcwd())


# In[3]:


# change the directory to where the files are

os.chdir("/Users/badisa/TWC_Datasets/")


# In[4]:


def listDirs():
    """ get the list of all files and directories in the current working directory, TWC_Datasets """
    print("All files and dirs in the TWC_Datasets directory: \n", os.listdir())
listDirs()


# # TO-DO !!!
# call python | bash scripts inside the Jupyter notebook for handling TWC datasets

# In[5]:


# do a bash-like script or python regex for listing all files in the directories, NGA, RTBA etc 
# ls -al | cd NGA/ | ls -al


# In[6]:


# define funcs for listing files (by types) inside NGA dir

def findCSVfiles(path_to_dir, suffix = ".csv"):
    """ func for finding csv files in the NGA directory """
    path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findXLSXfiles(path_to_dir, suffix = ".xlsx"):
    """ func for finding xlsx files in the NGA directory """
    path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findXLSfiles(path_to_dir, suffix = ".xls"):
    """ func for finding xls files in the NGA directory"""
    path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]

def findTXTfiles(path_to_dir, suffix = ".TXT"):
    """ func for finding TXT files in the specified directory """
    path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
    dir_list = os.listdir(path)
    return [file for file in dir_list if file.endswith(suffix)]


# In[7]:


help(findCSVfiles)


# In[8]:


# call all files functions
def call_file_funcs():
    """ func that calls all the find files functions """
    """ prints all files from funcs above and displays all csv, xlsx and txt files """
    files_path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
    return findCSVfiles(files_path), findXLSXfiles(files_path), findXLSfiles(files_path), findTXTfiles(files_path) 

call_file_funcs()


# In[9]:


# print all files, not as a list of a list
files_path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"

def cat_files():
    """ func that prints out file names as a list, without apostrophes """
    CSVs = findCSVfiles(files_path)
    XLSXs = findXLSXfiles(files_path)
    XLSs = findXLSfiles(files_path)
    TXTs = findTXTfiles(files_path)

# check for a better way of chunking vars in the for loop, i.e. use a single for loop for all filenames
    for file in CSVs:
        print(file)

    for file2 in XLSXs:
        print(file2)
    
    for file3 in XLSs:
        print(file3)

    for file4 in TXTs:
        print(file4)
         
cat_files()


# In[10]:


# check why this only returns one file !!!
def findCSV2(path_to_dir, suffix = ".csv"):
    """ func for"""
    path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
    dir_list = os.listdir(path)
    for file in dir_list:
        if file.endswith(suffix):
            return file


# In[11]:


# check | investigate this
findCSV2("/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/")


# In[12]:


# merge spreadsheets and append, chunk water levels together, water_quality together etc
# Read in and merge water levels
cat_files()


# * Note - use glob for files pattern matching

# In[13]:


""" use glob func to match files with similar names, match all waterLevel files and sort them """
water_level_filenames = (glob.glob('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/*_WaterLevels.csv'))
def match_WLF_patterns(file_path):
    """ method for matching files with similar names, match all waterLevel files and sort them """
    water_level_filenames = (glob.glob('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/*_WaterLevels.csv'))
    return water_level_filenames
match_WLF_patterns("/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/")


# In[14]:



# create water levels dataframe
water_levels = pd.DataFrame()

# append each water_level file to the dataframe
for wl_file in water_level_filenames:
    wl_df = pd.read_csv(wl_file, parse_dates = True, date_parser = True)
    wl_df.rename(columns = lambda x: x.strip().lower(), inplace = True)
    water_levels = water_levels.append(wl_df, ignore_index = True, sort = False)

# Inspect the first 10 records
water_levels.head(-10)


# In[15]:



print('current station IDs: \n {}'.format(water_levels ['station'].unique()))


# In[16]:


# merged all water levels in cwd (NGA_and_WMS_databases) into mergedWaterLevels.csv file
water_levels.to_csv("/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/mergedWaterLevels.csv", index=False)


# In[17]:


# inspect water levels info
water_levels.info()


# # Inspecting & Cleaning Water Levels
# 
# The idea is to enhance the datasets to conform to the RIMS standard. Minimally RIMS requires datasets with bh_id, country, latitude and longitude. On inspecting the merged_water_levels data, the following issues were identified, noted and rectified
# * Spaces at the end of station names (alias for borehole ID) e.g. 'A1N0001 ' instead of 'A1N0001'
# * Date field not captured in the required format, e.g. 19980904 instead of yyyy/mm/dd. Use datetime module and convert date to yyy/mm/dd.
# * Quality and datatrans captured as codes (e.g. 26). It will be desirable to add code descriptions so we understand what code 26 means using  'Quality Code Description.xlsx' and 'Quality codes.xls'. Use map function to convert code number to description of code.
# * There is a 'unnamed: 7 column', perhaps drop the column and/or merge with comments column.
# * Water level entries captured as negative (e.g -28.93), change water level to swl as per RIMS??
# * Get latitudes and longitudes for each borehole ID | station
# * Provide source and contact details for each record | envisaged need for tracing where the data came from
# * Rename columns according to rims. For example Station name to borehole id (bh_id) as in RIMS
# * Decide which columns are needed or relevant, Drop unecessary columns: water_level, datatrans ??
# 
# 

# In[18]:


# use Pandas to desribe the water_levels
water_levels.describe()


# # Note: made a copy of water levels

# In[19]:


# format station names, remove spaces at the end of station names
cleaned_water_levels = water_levels.copy()
def format_station_names():
    """ func formats st8n names, removes spaces in the station names, 'A3N0513 ' -> 'A3N0513' """
    print("\n station names before formatting: \n {}".format(water_levels['station'].unique()))
    cleaned_water_levels['station'] = water_levels['station'].str.replace(' ', '')
    print("\n station names after formatting: \n {}".format(cleaned_water_levels ['station'].unique()))
format_station_names()


# In[20]:


# convert water quality code to a descriptive name
def conv_wq_code():
    """ convert quality code to a meaningful code i.e. 93:'Dry borehole' """
print("\n Water Quality codes before converting: \n {}".format(cleaned_water_levels['quality'].unique()))
cleaned_water_levels['quality'] = cleaned_water_levels['quality'].map({26: 'Audited Gauge Plate Readings / dip level readings', 93: 'Dry borehole', 1: 'Good continuous data'})
print("\n Water Quality codes after converting: \n {}".format(cleaned_water_levels['quality'].unique()))
conv_wq_code()


# In[21]:


# convert datatrans to a descriptive meaning
print("\n Data trans code before converting: \n {}".format(cleaned_water_levels['datatrans'].unique()))
cleaned_water_levels['datatrans'] = cleaned_water_levels['datatrans'].map({7: 'Point data, no interpolaton - Monthly readings, hand measurements',})
print("\n Data trans converted to a meaning: \n {}".format(cleaned_water_levels['datatrans'].unique()))


# # TO-DO
# * check the need for the comment and unnamed: 7 columns
# * whats the use of comment??

# In[22]:


# concatenate comment and unnamed columns
cleaned_water_levels = cleaned_water_levels.replace(np.NaN, ' ')
# combine 'comment' and 'unnamed: 7' into one column
water_levels ['comment'] = water_levels ['comment'] + water_levels ['unnamed: 7']
cleaned_water_levels


# In[ ]:





# 
# 
# # Issues
# * If you run the format_date func twice it adds new hyphens on the date "1998--0-9-04
# * TO-DO  check python | pandas inbuilt func for converting date formats to yyyy-mm-dd in an elegant way 
# * to_datetime writes all dates to 1970-01-01

# In[23]:


# convert date string into the format yyyy-mm-dd

def format_date():
    """ func converts date field in the format yyyy-mm-dd """
    cleaned_water_levels ['date'] = cleaned_water_levels['date'].astype(str)
    cleaned_water_levels ['year'] = cleaned_water_levels ['date'].str[:4]
    cleaned_water_levels ['month'] = cleaned_water_levels ['date'].str[4:6]
    cleaned_water_levels ['day'] = cleaned_water_levels['date'].str[6:]
    cleaned_water_levels ['new_date'] = cleaned_water_levels ['year'] + "-" + cleaned_water_levels ['month'] + "-" + cleaned_water_levels ['day']
    # instead of putting new date column, replace newdate col with col named date
    cleaned_water_levels ['date'] = cleaned_water_levels ['new_date']
    # drop additional fields, year month, new_date
    cleaned_water_levels.drop(["year","month","year","new_date"] , axis=1)
    return cleaned_water_levels


# In[24]:


# call func format_date to return the waterlevels df with the required date format
format_date()


# # Issue
# * func outputs the right date format, but the actual date is incorrect, defaults all dates to 1970-01-01

# In[25]:


# use to_datetime to convert date
"""
def date_to_rims_format():
    func returns date in RIMS format
    water_levels['date'] = pd.to_datetime(water_levels['date']).dt.strftime('%Y-%m-%d')
    return water_levels
date_to_rims_format()

"""


# In[26]:


# remove additional fields, year, month, day, new_date
cleaned_water_levels.drop(["year","month","day","new_date"] , axis=1)


# In[27]:


# convert water levels to absolute values
#water_levels['water_level'].abs()
def wl_to_abs():
    """ converts water levels to absolute values """
    cleaned_water_levels['water_level'] = cleaned_water_levels['water_level'].abs()
    return cleaned_water_levels
wl_to_abs()


# In[28]:


# Insert file name to the datasets
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


# In[29]:


cleaned_water_levels


# # Append to RIMS structure
# * RIMS template takes bh_iD, country, latitude, longitude, swl_m, swl_date, comment_water_level, data owner, contact_person, email and filename
# 
# # Noted
# * RIMS_water_levels gets assigned values from a copy of water levels. If we assign values to RIMS_water_levels cols directly from original water_levels cols | dataframe, mapping coordinates on RIMS_water_levels complains bitterly and doesnt map!
# 
# * renamed the dataframe columns, removed additional dates and arranged quality, next to datatrans 
#  

# In[30]:


# create a RIMS water level dataframe
RIMS_water_levels = pd.DataFrame({'bh_id':[], 'swl_date':[], 'time':[], 'country':[], 'latitude':[],                                   'longitude':[], 'swl_m':[], 'quality':[], 'datatrans':[],                                    'comment_water_level':[], 'data_owner':[], 'contact_person':[],'email':[],                                   'file_name':[]})

RIMS_water_levels ['bh_id'] = cleaned_water_levels['station']
RIMS_water_levels['swl_date'] = cleaned_water_levels['date']
RIMS_water_levels ['time'] = cleaned_water_levels['time']
RIMS_water_levels ['country'] = 'South Africa'
RIMS_water_levels ['swl_m'] = cleaned_water_levels['water_level'] 
RIMS_water_levels ['quality'] = cleaned_water_levels['quality']
RIMS_water_levels ['datatrans'] = cleaned_water_levels['datatrans']
RIMS_water_levels['comment_water_level'] = cleaned_water_levels['comment']
RIMS_water_levels['data_owner'] = "Department of Water and Sanitation South Africa"
RIMS_water_levels['contact_person'] = "Ramusiya, Fhedzisani"
RIMS_water_levels['email'] = "RamusiyaF@dws.gov.za"
RIMS_water_levels['file_name'] = cleaned_water_levels['file_name']


# In[31]:


cleaned_water_levels['station'].unique()


# In[32]:


RIMS_water_levels


# # Issues
# * Coordinates file has multiple separators: , and ; 
# * E.g. Monitoring,Crocodile (West) and Marico,(None),Unknown,Ngaka Modiri Molema,Ramotshere Moiloa,Unknown,Unknown,Unknown,,,,,,,,,,,,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
# * Noted error (Passed header names mismatches usecols) when trying to read csv with usecols=['#STATION', 'LATITUDE', 'LONGITUDE']
# * Workaround: Python script for reading all lines inside the SITE_INFORA10A.csv and separate with , instead of ;
# 

# In[33]:


# regex

import re

txt = "The,rain,in,Spain"
x = re.sub(",", ":", txt)
x


# # Issue loading SITE_INFORA10A file
# 
# * use a regex to clean the file
# * python script | build a function and import it on Jupyter notebook
# * TO-DO: Create a Regex func for cleaning thw files, regex will be faster and  avoid creating another file 
# * NOTE: Cleaned (delimiter set to comma) the SITE_INFORA10A.csv and renamed to SITE_INFORA10A_fixed.csv

# In[34]:


# Get coordinates (Lat & Longs) of stations
# rename the col to lower case
# drop duplicates

bh_coordinates = pd.read_csv('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/SITE_INFORA10A_fixed.csv', usecols=['#STATION','LATITUDE', 'LONGITUDE'])
bh_coordinates.rename(columns = {'#STATION': 'bh_id', 'LATITUDE': 'latitude', 'LONGITUDE': 'longitude'}, inplace = True)
bh_coordinates.drop_duplicates(inplace = True)

bh_coordinates.head()


# In[35]:


# check unique boreholes ID in coordinates file
# see if boreholes ID in bh_coordinates are unique to water levels

print("\n station names in water levels: \n", RIMS_water_levels['bh_id'].unique())
print("\n station names in bh coordinates: \n", bh_coordinates['bh_id'].unique())


# In[36]:


"""
pandas has an options system that lets you customize some aspects of its behaviour, display-related 
options being those the user is most likely to adjust.

"""
pd.options.mode.chained_assignment = None

coords = bh_coordinates[['bh_id','latitude','longitude']]
rims_bhid = list(RIMS_water_levels['bh_id'].unique())

# check if coordinates isin rims BH_ID
coord_rims = coords [coords ['bh_id'].isin(rims_bhid) == True]
coord_rims.drop_duplicates(inplace = True)
coord_rims.info()


# In[37]:


# for each borehole, add coordinates to the RIMS_water_levels dataframe
# define a var for merging coordinates with RIMS_water_levels
# merge using 'on = bh_id'

merged_RIMS_water_levels = RIMS_water_levels.merge(coord_rims, on = 'bh_id')
merged_RIMS_water_levels.info()

# rename coordinates cols (latitude_y and longitude_y) to desired names and 
# add bh coordinates to the RIMS_water_levels dataframe

def clean_coordinates():
    """method renames the lat_y and longi_y to lat and long, drops lat_x & lat_y"""
    merged_RIMS_water_levels.drop(['latitude_x', 'longitude_x'], axis=1, inplace = True )
    merged_RIMS_water_levels.rename(columns = {'latitude_y':'latitude', 'longitude_y':'longitude'}, inplace=True)
    return merged_RIMS_water_levels


# In[38]:


# call clean_coordinates()

clean_coordinates()


# # Noted!
# * Upon merging coordinates with RIMS_water_levels using on = bh_id, new cols are created
# * latitude and longitude gets renamed to latitude_y and longitude_y respectively
# * additional cols latitude_x and longitude_x

# In[39]:


# values counts for bh_id
merged_RIMS_water_levels.bh_id.value_counts()


# # Final water levels DF

# In[40]:


# save final water levs as csv
merged_RIMS_water_levels.to_csv("/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/merged_RIMS_water_levels.csv", index=False)


# In[41]:


merged_RIMS_water_levels.head(2)


# # Plotting Water levels

# In[42]:


A3N0015 = merged_RIMS_water_levels[merged_RIMS_water_levels['bh_id'] == 'A3N0015']


# In[43]:


A3N0015.plot.line(x='swl_date', y='swl_m', figsize=(18,6), lw = 1)

plt.title('A3N0015 Borehole', fontsize=16)
plt.xlabel('Date', fontsize = 12)
plt.ylabel('Static Water Level', fontsize = 12)


# # To-DO
# 
# Plot multiple plots for each borehole ID 

# # Merging water quality files
# 
# 

# In[44]:



water_quality_filenames = sorted(glob.glob('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/*[eye]*.xlsx'))
water_quality_files = water_quality_filenames[:5]

def merge_water_quality():
    """ func takes path to water quality files and merges all WQ files """
    """ use glob func to match all water quality files inside NGA dir"""
    #TODO: construct a proper file pattern matching for Water Quality files
    # look for filenames containing chars "eye", pick the first 5 (those for water quality)
    #water_quality_files = water_quality_filenames[:5]
    return water_quality_files

merge_water_quality()


# In[45]:


# create water quality dataframe, wq_df means water quality dataframe
# files have different columns, rename cols to the same col_names
# merge all water quality files into one water quality file

merged_water_quality = pd.DataFrame()
for f in water_quality_files:
    wq_df = pd.read_excel(f, header = None, parse_dates = True)
    wq_df ['name'] = wq_df.iloc[0,1][0:6]
    wq_df = wq_df.iloc[4:, :]
    wq_df.columns = ['date_time','mean_discharge_cumecs', 'w_quality', 'name'] # cumecs ~ Cubic metres per second measure for water discharge
    merged_water_quality = merged_water_quality.append(wq_df, ignore_index = True)
    
# see last 20 records in the dataframe   
merged_water_quality.head(-20)


# In[46]:


# save merged WQ files to an xlsx file called mergedWaterQuality.xlsx
merged_water_quality.to_excel("mergedWaterQuality.xlsx", index = False)
merged_water_quality.to_csv("mergedWaterQuality.csv", index = False)


# In[47]:


merged_water_quality.head()


# In[48]:


merged_water_quality['name'].unique()


# # Noted
# * There is no overlap between water levels and water quality IDs
# * water quality IDs and water levels are not equal

# In[49]:


merged_RIMS_water_levels['bh_id'].unique()


# In[50]:


merged_water_quality['name'].unique()


# In[51]:


# check if cols are the same in the waterlevels and water quality dataframes
merged_RIMS_water_levels['bh_id'].equals(merged_water_quality['name'])


# # Notes
# * renamed 'name' to bh_id for uniformity with water levels column naming
# * renamed date_time on water_quality df to separate fields, date and time

# In[52]:


# rename water quality 'name' field to bh_id

merged_water_quality.rename(columns={'name': 'bh_id', 'date_time': 'date'}, inplace = True)
merged_water_quality


# In[53]:


# merged_water_quality['date'] = merged_water_quality['date'].dt.strftime('%Y-%m-%d')
def wq_date_time():
    """ func for formatting water_quality dates"""
    merged_water_quality ['date'] = merged_water_quality['date'].astype(str)
    merged_water_quality ['new_date'] = merged_water_quality ['date'].str[:10]
    merged_water_quality ['time'] = merged_water_quality ['date'].str[11:]
    merged_water_quality['date'] = merged_water_quality['new_date']
    #merged_water_quality = merged_water_quality.drop('new_date', axis = 1)
    return merged_water_quality    


# In[54]:


wq_date_time()


# In[55]:


# drop new-date
merged_water_quality = merged_water_quality.drop('new_date', axis = 1)


# In[56]:


merged_water_quality


# In[57]:


# assess first and last dates on WQ dataframe
print(merged_water_quality['date'].sort_values().min())
print(merged_water_quality['date'].sort_values().max())


# In[58]:


# value counts
merged_water_quality['mean_discharge_cumecs'].value_counts()


# In[59]:


# Unique water quality codes
merged_water_quality['w_quality'].unique()


# In[60]:


# map water quality code to meaningful description
# TO-DO look for an automated map, inside file
merged_water_quality['quality_desc'] = merged_water_quality['w_quality'].map({
    255: 'Missing data', 26: 'Audited Gauge Plate Readings / dip level readings',
170: 'Period of No Record (PNR)', 1: 'Good continuous data', 64: 'Audited Estimate',
2: 'Good edited data', 151: 'Data Missing', 47: 'Edited and checked\044 still unaudited',
44: 'Checked\044 still unaudited', 60: 'Above Rating'   
})

merged_water_quality


# In[61]:


merged_water_quality['quality_desc'].unique()


# In[62]:


# Insert data source details i.e. data owner, contact, email and file name
merged_water_quality['data_owner'] = "Department of Water and Sanitation South Africa"
merged_water_quality['contact_person'] = "Ramusiya, Fhedzisani"
merged_water_quality['email'] = "RamusiyaF@dws.gov.za"
merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H001'), 'file_name'] = 'A1H001 Upper Eye Dinokana.xlsx'
merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H002'), 'file_name'] = 'A1H002 Lower Eye Dinokana.xlsx'
merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H003'), 'file_name'] = 'A1H003 Upper Eye Tweefontein.xlsx'
merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H004'), 'file_name'] = 'A1H004 Lower Eye Tweefontein.xlsx'
merged_water_quality.loc[merged_water_quality['bh_id'].str.contains('A1H005'), 'file_name'] = 'A1H005 Skilpad Eye.xlsx'


# # Final water quality DF

# In[63]:


merged_water_quality.head()


# # Stacking together dataframes with different columns
# * simple concat is not good enough, Horizontal stacking is inaccurate!

# # Play with pandas left outer joins
# * Perform pandas left outer join on two DFs, merged_RIMS_water_levels & water_quality
# * a left join uses join keys to combine two DataFrames. Unlike an inner join, a left join 
# * will return all of the rows from the left DataFrame, even those rows whose join key(s) do \
# * not have values in the right DataFrame. Rows in the left DataFrame that are missing values \
# * for the join key(s) in the right DataFrame will simply have null (i.e., NaN or None) values for
# * those columns in the resulting joined DataFrame.

# In[64]:


# concatenate two dataframes

merged_WLWQ_df = pd.concat([merged_RIMS_water_levels,merged_water_quality],sort=False)

merged_WLWQ_df.head()


# In[65]:


# inspect columns
merged_WLWQ_df.columns


# In[66]:


# clean columns

merged_WLWQ_df.rename(columns={'time_x': 'time', 'data_owner_x':'data_owner', 'data_owner_y':'data_owner',                               'contact_person_x':'contact_person','contact_person_y':'contact_person',                               'email_x': 'email','email_y': 'email','file_name_x':'file_name', 'file_name_y':                               'file_name', 'time_y':'time','quality_desc_x':'quality_desc', 'quality_desc_y':'quality_desc'},                     inplace= True)
merged_WLWQ_df.head()


# In[67]:


# fill unmatched values with 9999 instead of NaN
merged_WLWQ_df = merged_WLWQ_df.replace(np.nan,'9999')
merged_WLWQ_df.head()


# In[68]:


# bh_IDs from water quality merged
merged_WLWQ_df['bh_id'].unique()


# In[69]:


# check for mean discharge cumecs > 0 | =0.095
# TO_DO allow search on the dataframe e.g. show mean_cumecs > 1
merged_WLWQ_df['w_quality'].where(merged_WLWQ_df['mean_discharge_cumecs'] == 0.095)


# In[70]:


# final merged dataframe combining the two dataframes | merged_RIMS_water_levels, merged_water_quality
# save to a csv
merged_WLWQ_df.to_csv("merged_WLWQ_df.csv", index=False)


# In[71]:


ls -al


# In[72]:


cat merged_WLWQ_df.csv


# # Investigating Ramotswa datasets
# * EDA on the Ramotswa data
# 

# In[73]:


# read in the csv files
ramotswa_1_df = pd.read_csv('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/Ramotswa_data_1_20190904.csv', header = 0, encoding = 'unicode_escape')
ramotswa_2_df = pd.read_csv('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/Ramotswa_data_2_20190904.csv', header = 0, encoding = 'unicode_escape')


# In[74]:


# Have a look at the data 
ramotswa_1_df.head()


# In[75]:


# read in Ramotswa 2

ramotswa_2_df.head()


# # Cleaning Ramotswa 1 & 2 files
# 
# * Extract variable result column
# * Rename columns
# * File names
# * Check units with RIMS
# * Check names with RIMS
# * Check water levels (negative sign, strip)
# * Fill in quality description and datatrans: use code above to fill in
# 
#     

# In[76]:


rams1 = ramotswa_1_df.copy()


# In[77]:


rams1.info()


# In[78]:


# enumerate all cols in rams1

for i,v in enumerate (rams1.columns):
    print(i,v)


# In[79]:


# use np.r_ to build array efficiently
"""
 numpy.r_ = <numpy.lib.index_tricks.RClass object at 0x1b5e390>

    Translates slice objects to concatenation along the first axis.

    This is a simple way to build up arrays quickly. There are two use cases.

        If the index expression contains comma separated arrays, then stack them along their first axis.
        If the index expression contains slice notation or scalars then create a 1-D array with a range indicated by the slice notation.

"""
cleaned_rams1 = ramotswa_1_df.iloc[:, np.r_[:19, 19,22,25,28,31,34,37,40,43,46,49,52,                               55,58,61,64,67,70,73,76,79,82,85,88,91,94,97,100,103,106]]
cleaned_rams1.info()


# In[80]:


# rename col Monitoring Point ID

cleaned_rams1.rename(columns = {'Monitoring Point ID':'ID Monitoring Point'}, inplace = True)


# In[81]:


for i,v in enumerate (cleaned_rams1.columns):
    print(i,v)


# In[82]:


# inspect cols
cleaned_rams1.columns


# In[83]:


# rename cols in cleaned_rams1 df
# shorter cols name does it

cleaned_rams1.rename(columns = lambda x : str(x)[:15], inplace = True)

cleaned_rams1.rename(columns = {
    
'ID Monitoring P': 'Monitoring Point ID', 'Monitoring Poin': 'Monitoring Point Name',
'Located on Feat': 'Located on Feature Name', 'Drainage Region': 'Drainage Region Name',
'Feature Referen': 'Feature Reference Code', 'Monitoring Acti': 'Monitoring Active',
'Sample Start Da':'Sample Start Date', 'Sample Start Ti':'Sample Start Time', 'Sample Start De':'Sample Start Depth',
'ASAR-Diss-Water': 'ASAR', 'CORR-Diss-Water': 'CORR', 'Ca-Diss-Water (': 'Ca',
'Cl-Diss-Water (': 'Cl', 'DMS-Tot-Water (': 'DMS', 'EC-Phys-Water (': 'EC',
'F-Diss-Water (F': 'F', 'HARD-Mg-Calc-Wa': 'HARD-Mg-Calc', 'HARD-Tot-Water': 'HARD-Tot',
'K-Diss-Water (P': 'K', 'KJEL N-Tot-Wate': 'KJEL N-Tot', 'LANGL-Index-Wat': 'LANGL-Index',
'Mg-Diss-Water (': 'Mg', 'NH3(25)-Union-D': 'NH3(25)-Union', 'NH4-N-Diss-Wate': 'NH4-N',
'NO3+NO2-N-Diss-': 'NO3+NO2-N', 'Na-Diss-Water (': 'Na', 'P-Tot-Water (TO': 'P-Tot', 'PO4-P-Diss-Wate': 'PO4-P',
'RYZNAR-Index-Wa': 'RYZNAR-Index', 'SAR-Diss-Water ':  'SAR', 'SO4-Diss-Water ':  'SO4', 'Si-Diss-Water (': 'Si',
'TAL-Diss-Water ':  'TAL', 'pH-Diss-Water (': 'pH', 'pHs-Calc-Water ': 'pHs-Calc',
'NO2-N-Diss-Wate': 'NO2-N', 'N-Tot-Calc-Wate': 'N-Tot-Calc'  

}, inplace = True)

cleaned_rams1.info()


# # Noted
# * saved the file_name with an underscore

# In[84]:


# insert data source details i.e. data owner, contact, email, and filename into cleaned_rams1 df

cleaned_rams1['dataowner'] = "Department of Water and Sanitation South Africa"
cleaned_rams1['contact_person'] = "Ramusiya, Fhedzisani"
cleaned_rams1['email'] = "RamusiyaF@dws.gov.za"
cleaned_rams1['file_name'] = "Ramotswa_data_1_20190904.csv"


# # Dig on Monitoring Point IDs 
# 
# * where do monitoring point IDs come from??
# 
# Notes
# 
# * for monitoring point name: e.g "ZQMBM45 TWEEFONTEIN UPPER EYE AT TWEEFONTEIN"
# * ZQMBM45 is a ZQM number, a WQ monitoring point, 
# * TWEEFONTEIN, farm, literal name for the NGA number  
# * UPPER EYE AT TWEEFONTEIN, likely a spring

# In[85]:


# see first few records

cleaned_rams1.head()


# In[86]:


# clean Ramotswa 2 df
cleaned_rams2 = ramotswa_2_df.copy()


# In[87]:


for i,v in enumerate(cleaned_rams2.columns):
    print(i,v)


# In[88]:


cleaned_rams2 = ramotswa_2_df.iloc[:, np.r_[:19, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49, 52, 55, 58, 61, 64, 67, 70, 73, 76, 79, 82, 85, 88, 91]]
cleaned_rams2.info()


# In[89]:


# see cols
cleaned_rams2.columns


# In[90]:


cleaned_rams2.info()


# In[91]:


#cleaned_rams2.rename(columns = {'Monitoring Point ID': 'ID Monitoring Point'}, inplace = True)


# In[92]:


for i,v in enumerate(cleaned_rams2):
    print(i,v)


# In[93]:


# shorten long column names
cleaned_rams2.rename(columns={

'ID Monitoring Point': 'Monitoring Point ID',
'Al-Diss-Water (ALUMINIUM) (mg/L) Result':'Al-Diss-Water',
'As-Diss-Water (ARSENIC) (mg/L) Result':'As-Diss-Water',
'B-Diss-Water (BORON) (mg/L) Result':'B-Diss-Water',
'Ba-Diss-Water (BARIUM) (mg/L) Result':'Ba-Diss-Water',
'COD (CHEMICAL OXYGEN DEMAND) (mg/L) Result':'COD',
'Cd-Diss-Water (CADMIUM) (mg/L) Result':'Cd-Diss-Water',
'Cr-Diss-Water (CHROMIUM) (mg/L) Result':'Cr-Diss-Water',
'Cu-Diss-Water (COPPER) (mg/L) Result':'Cu-Diss-Water',
'Fe-Diss-Water (IRON) (mg/L) Result':'Fe-Diss-Water',
'Mn-Diss-Water (MANGANESE) (mg/L) Result':'Mn-Diss-Water',
'Mo-Diss-Water (MOLYBDENUM) (mg/L) Result':'Mo-Diss-Water',
'Ni-Diss-Water (NICKEL) (mg/L) Result':'Ni-Diss-Water',
'Pb-Diss-Water (LEAD) (mg/L) Result':'Pb-Diss-Water',
'Sr-Diss-Water (STRONTIUM) (mg/L) Result':'Sr-Diss-Water',
'V-Diss-Water (VANADIUM) (mg/L) Result':'V-Diss-Water',
'Zn-Diss-Water (ZINC) (mg/L) Result':'Zn-Diss-Water',
'E.COLI-Susp-Water (ESCHERICHIA COLI) (cfu/100mL) Result':'E.COLI-Susp-Water',
'E.COLI-SuspMPN-Water (ESCHERICHIA COLI BY MPN METHOD) (MPN/100mL) Result':'E.COLI-SuspMPN',
'Eh-Phys-Water (REDOX POTENTIAL) (mV) Result':'Eh-Phys-Water',
'Hydrogen-3-Water (TRITIUM ATOM RATIO WITH RESPECT TO HYDROGEN (3H/1H)) (TU) Result':'Hydrogen-3-Water',
'O-Abs-Water (OXYGEN ABSORBED) (mg/L) Result':'O-Abs-Water',
'O-Diss-Water (OXYGEN DISSOLVED) (mg/L) Result':'O-Diss-Water',
'SOLIDS-Susp-Water (TOTAL SUSPENDED SOLIDS) (mg/L) Result':'SOLIDS-Susp-Water',
'TC-SuspMPN-Water (TOTAL COLIFORM COUNT BY MPN METHOD) (MPN/100mL) Result':'TC-SuspMPN-Water',
'TEMP-Phys-Water (TEMPERATURE) (°C) Result': 'TEMP-Phys-Water'

}, inplace=True)


# In[94]:


cleaned_rams2.columns


# In[95]:


# insert data source details i.e. data owner, contact, email, and filename into cleaned_rams2 df

cleaned_rams2['dataowner'] = "Department of Water and Sanitation South Africa"
cleaned_rams2['contact_person'] = "Ramusiya, Fhedzisani"
cleaned_rams2['email'] = "RamusiyaF@dws.gov.za"
cleaned_rams2['file_name'] = "Ramotswa_data_2_20190904.csv"


# In[96]:


cleaned_rams2.head()


# # Identify join keys 
# 
# * To identify appropriate join keys we first need to know which field(s) are shared between the files (DataFrames)
# 
# Noted
# * The 2 ramotswa DFs don't have equal columns
# * rams 1 has 53 columns while rams 2 has 48 columns
# * Columns on rams 1 and not in rams 2 include: ASAR, CORR, CA, AL-DISS-Water

# In[97]:


# Ramotswa 1 columns
cleaned_rams1.columns


# In[98]:


# Ramotswa 2 columns
cleaned_rams2.columns


# In[99]:


# check if the dataframes are equal
cleaned_rams2.equals(cleaned_rams1)


# In[100]:


# count number of rows per DF before merging
print('cleaned_rams1 has: {} rows'.format(cleaned_rams1.shape))
print('cleaned_rams2 has: {} rows'.format(cleaned_rams2.shape))


# In[101]:


# merge the two Ramotswa DFs
# use an outer join

# get common cols in rams1 and rams2
common_cols = cleaned_rams1.columns.intersection(cleaned_rams2.columns)
common_cols


# In[102]:


# merged_ramotswa2 = pd.merge(cleaned_rams1, cleaned_rams2, on = , how = 'outer', sort= False)
# merged_ramotswa3 = pd.merge(left=cleaned_rams1, right=cleaned_rams2, how = 'inner', \
#                          left_on = 'Monitoring Point ID', right_on = 'Monitoring Point ID')
# merged_ramotswa3

# stack dataframes together vertically
merged_ramotswa = pd.concat([cleaned_rams1, cleaned_rams2],axis=0, ignore_index=True, sort=False)
merged_ramotswa


# In[103]:


# Display merged Ramotswa files info
merged_ramotswa.columns


# # Tidy up csv 
# 
# * Noted: Pandas outer joins appends _x to cols
# 
# * After merging two DFs, columns are renamed with _x

# In[104]:


# strip ' ' in column names i.e. 'HARD-Tot-Water '
"""
merged_ramotswa['HARD-Tot-Water '] = merged_ramotswa['HARD-Tot-Water '].astype(str)
merged_ramotswa['HARD-Tot-Water '] = merged_ramotswa['HARD-Tot-Water '].str.replace(' ', '')

"""
merged_ramotswa.rename(columns={'HARD-Tot-Water ': 'HARD-Tot-Water'}, inplace = True)


# In[105]:


merged_ramotswa.columns


# In[106]:


# use regex to rename cols 
# iterate through the cols list and strip _x 

a = merged_ramotswa.columns

for fname in a:
    x = re.sub("_x","",fname)
    print(x)


# In[107]:


# regex example

import re

txt = "The,rain,in,Spain"
x = re.sub(",", ":", txt)
x


# In[108]:


# save to a csv
merged_ramotswa.to_csv('merged_ramotswa.csv', index=False)


# # Tidying Ramotswa Inventory files
# 
# * This file contains the data extracted as requested, contains some monitoting variables
# 

# In[109]:


# read in the data
ramotswa_inventory = pd.read_csv('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/Ramotswa_variables_inventory_20190904.csv',                                 header=0, encoding = 'unicode_escape')
ramotswa_inventory


# In[110]:


# see the info
ramotswa_inventory.info()


# In[111]:


# describe data
ramotswa_inventory.describe()


# In[112]:


# investigate unique IDs
ramotswa_inventory['Monitoring Point ID'].unique()


# In[113]:


# check value counts for each ID
ramotswa_inventory['Monitoring Point ID'].value_counts()


# In[114]:


# assess Measuring units
ramotswa_inventory['Measuring Unit'].unique()


# In[115]:


ramotswa_inventory['Measuring Unit'].nunique()


# In[116]:


# assess monitoring variables
ramotswa_inventory['Monitoring Variable'].unique()


# In[117]:


ramotswa_inventory['Monitoring Variable'].nunique()


# # Cleaning the inventory files
# 
# Noted: 
# 
# * Missing values for Located on Feature Name, Located on Type and Measuring Unit.
# * Columns Feature Reference Code has no data
# * Units of measurements not standard
# 
# 

# In[118]:


# read in cleaned site_INFORA10A file
# basic information of sites, rename and convert to lower case
site_info = pd.read_csv('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/Cleaned_SITE_INFORA10A.csv')

site_info.head()


# In[119]:


site_info.info()


# In[120]:


# convert commence column to rims date format
site_info['commence'] = pd.to_datetime(site_info['commence'])
site_info['commence'] = site_info['commence'].dt.strftime('%Y-%m-%d')

# convert date format for cease column
site_info['cease'] = pd.to_datetime(site_info['cease'])
site_info['cease'] = site_info['cease'].dt.strftime('%Y-%m-%d')


site_info.head()


# In[121]:


# save sites info to csv file
site_info.to_csv('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/merged_site_info.csv', index = False)


# # Assessing Ramotswa study area plus 10km
# * find out about the study area
# 

# In[122]:


# read in Ramotswa study area 10km file
ramotswa_study_area = pd.read_excel('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/Ramotswa_study_area_plus_10km.xlsx', header=0)


# In[123]:


ramotswa_study_area.head()


# In[124]:


# ramotswa study area info
ramotswa_study_area.info()


# In[125]:


# lower case column values : ramotswa_study area 
# Map the lowering function to all column names
ramotswa_study_area.columns = ramotswa_study_area.columns.str.lower()

ramotswa_study_area.head()


# In[126]:


ramotswa_study_area.rename({'descriptio': 'description'},axis=1, inplace = True)


# In[127]:


ramotswa_study_area.head()


# In[128]:


# save to a csv file
ramotswa_study_area.to_csv('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/merged_Ramotswa_study_area.csv', index = False)


# # EDA on NGA_Data_for_the_area_13_09_2019
# 
# * Assess abstraction, basic_wbw, casings, depthdiameter, discharge, equipments_wbw fieldmeasurements, lithology,
# * water_strikes, water_levels, yield test etc.
# 
# 
# * The info is Basically collected at construction phase (borehole construction)
# 
# 1. Basic borehole information
#     * Lithology
#     * Borehole diameter
#     * Borehole casing
#     * Depth of borehole
# 
# 2. Testing and/or monitoring phase
#     * Equipment
#     * Discharge
#     * Water levels
#     * Water strikes
#     * Yield tests
#     * Field measurements
#     
# * TO_DO: shorten file paths, use relative paths

# In[129]:


# change dir to where NGA data for the area files are 
os.chdir("/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019/")


# In[130]:


# look for csv files to work with
def find_csvs(path_to_dir, suffix = ".csv" ):
    filenames = listdir(path_to_dir)
    return sorted([filename for filename in filenames if filename.endswith(suffix)])
find_csvs('/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019')


# In[131]:


# read in all files
# use glob func to match all NGA data for the area files and sort them """
"""

nga_data_fnames = sorted(glob.glob('/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019/*.csv'))
def match_nga_patterns(file_path):
    method for matching all NGA data files and sort them 
    #nga_data_fnames = (glob.glob('/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019/*.csv'))
    return nga_data_fnames

match_nga_patterns('/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019')

"""


# In[132]:


# loop through each file and add data source details
all_nga_files = list()

def merge_nga_files():
    """merge all nga data for the area files"""
    for file in os.listdir():
        if file.endswith('.csv'):
            nga_area_df = pd.read_csv(file)
            # add source details to each file 
            nga_area_df['file_name'] = file
            nga_area_df['data_owner'] = "Department of Water and Sanitation South Africa"
            nga_area_df['contact_person'] = "Ramusiya, Fhedzisani"
            nga_area_df['email'] = "RamusiyaF@dws.gov.za"
            all_nga_files.append(nga_area_df)

merge_nga_files()


# In[133]:


# see head of the dataset
nga_dataset = pd.concat(all_nga_files, axis = 0, ignore_index = True, sort = False)
nga_dataset.head()


# In[134]:


# save to csv file
#nga_dataset.to_csv('/Users/badisa/TWC_Datasets/Merged_files/nga_dataset.csv', index= False)


# In[135]:


# dataset info
nga_dataset.info()


# In[136]:


# GeositeInfo_Identifier value counts
nga_dataset.GeositeInfo_Identifier.value_counts()


# In[137]:


# yield test unique
nga_dataset['YieldTest_DischargeRate'].unique()[:3]


# In[138]:


# assess Discharge Rate
nga_dataset ['DischargeRate_DischargeRate'].unique()[:3]


# In[139]:


# assess Depth Diameter
nga_dataset['DepthDiameter_Diameter'].unique()[:3]


# # Tidy up dataframe
# 
#     strip Geosites from column names
#     strip l/s from YieldTest_DischargeRate column values
#     strip l/s from DischargeRate_DischargeRate column values
#     DepthDiameter_Diameter, 122 mm (5.0)". Replace np.nan with -9999
#     Letters, makes small and camal case first letter
#     Group text values, label missing unknown.
#     Group numeric values, label missing -9999
# 
# 

# In[140]:


pd.set_option('display.max_columns', 132)


# In[141]:


# make a new df, copy of nga dataset and rename cols
# making a copy helps when renaming cols, avoids renaming conflicts
clean_nga_dataset = nga_dataset.copy()
col_names = list(nga_dataset.columns)
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


# In[142]:


clean_nga_dataset.head()


# In[143]:


# strip l/s & compare  yieldtest_dischargerate before and after cleaning
def clean_yieldTest():
    print('Before: \n', clean_nga_dataset['yieldtest_dischargerate'].unique()[:5])
    clean_nga_dataset['yieldtest_dischargerate'] = clean_nga_dataset['yieldtest_dischargerate'].str[0:5]
    print('After: \n', clean_nga_dataset['yieldtest_dischargerate'].unique()[:5])

clean_yieldTest()   


# In[144]:


# strip ls on discharge rate
print('Before: \n', clean_nga_dataset['dischargerate_dischargerate'].unique()[:5])
clean_nga_dataset['dischargerate_dischargerate'] = clean_nga_dataset['dischargerate_dischargerate'].str[0:5]
print('After: \n', clean_nga_dataset['dischargerate_dischargerate'].unique()[:5])


# In[145]:


# strip mm in depth Diameter
print('Before: \n', clean_nga_dataset['depthdiameter_diameter'].unique()[:3])
clean_nga_dataset['depthdiameter_diameter'] = clean_nga_dataset['depthdiameter_diameter'].str[0:3]
print('After: \n', clean_nga_dataset['depthdiameter_diameter'].unique()[:3])


# In[146]:


clean_nga_dataset['file_name'].nunique()


# In[147]:


clean_nga_dataset['file_name'].unique()


# In[148]:


clean_nga_dataset.drop('dataowner', axis=1, inplace=True)


# In[149]:


# save cleaned dataset as csv
#drop duplicate dataowner column
clean_nga_dataset.to_csv('/Users/badisa/TWC_Datasets/Merged_files/cleaned_nga_dataset.csv', index= False)


# In[150]:


clean_nga_dataset.head()


# # See all cols in the cleaned NGA dataset
# 
# * check if the dataset contains all cols from individual files
# 

# In[151]:


for i in enumerate(clean_nga_dataset.columns):
    print(i)


# In[152]:


# see similar column names in both dataframes
set(clean_nga_dataset.columns).intersection(set(merged_WLWQ_df.columns))


# In[153]:


combine_cols = clean_nga_dataset[['email','email_x']]

pd.concat([combine_cols,combine_cols.unstack().reset_index(drop=True).rename('Time')],axis=1)
#pd.concat([ref, ref.unstack().reset_index(drop=True).rename('c5')], axis=1)


# # Load  Individual files
# * clean each individual file

# In[154]:


# load in abstraction file
abstraction = pd.read_csv ('Abstraction_wbw_2019912_93856_DataExport_1.csv')


# In[155]:


col_names = list(abstraction.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
abstraction = abstraction.rename(columns = dictionary)
abstraction = abstraction.drop_duplicates ()


# In[156]:


abstraction.info()


# In[157]:


# see abstraction head

abstraction.head()


# In[158]:


# load in Basic_wbw_2019912_93716_DataExport_1.csv file
basic_wbw = pd.read_csv ('Basic_wbw_2019912_93716_DataExport_1.csv')


# In[159]:


# rename cols
col_names = list(basic_wbw.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
basic_wbw = basic_wbw.rename(columns = dictionary)
basic_wbw = basic_wbw.drop_duplicates ()


# In[160]:


basic_wbw.info()


# In[161]:


basic_wbw.head()


# In[162]:


# read in casings
casings = pd.read_csv ('Casings_wbw_2019912_94828_DataExport_1.csv')  

col_names = list(casings.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
casings = casings.rename(columns = dictionary)
casings = casings.drop_duplicates ()

casings.info()


# In[163]:


#load in depth diameters
depthDiam = pd.read_csv ('DepthDiam_wbw_2019912_94753_DataExport_1.csv')

col_names = list(depthDiam.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
depthDiam = depthDiam.rename(columns = dictionary)
depthDiam = depthDiam.drop_duplicates ()

depthDiam.info()


# In[164]:


depthDiam.head()


# In[165]:


# load  in Discharge data
discharge = pd.read_csv ('Discharge_wbw_2019912_9413_DataExport_1.csv')

col_names = list(discharge.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
discharge = discharge.rename(columns = dictionary)
discharge = discharge.drop_duplicates ()


discharge.info()


# In[166]:


discharge.head()


# In[167]:


# load in equipment data
equipment = pd.read_csv ('Equipment_wbw_2019912_94934_DataExport_1.csv')   

col_names = list(equipment.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
equipment = equipment.rename(columns = dictionary)
equipment = equipment.drop_duplicates ()

equipment.info()


# In[168]:


equipment.head()


# In[169]:


# load in field measurements

fieldMeasurements = pd.read_csv ('FieldMeasurements_wbw_2019912_9470_DataExport_1.csv')

col_names = list(fieldMeasurements.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
fieldMeasurements = fieldMeasurements.rename(columns = dictionary)
fieldMeasurements = fieldMeasurements.drop_duplicates ()

fieldMeasurements.info()


# In[170]:


fieldMeasurements.head()


# In[171]:


# load in Lithology
lithology = pd.read_csv ('Lithology_wbw_2019912_95245_DataExport_1.csv')

col_names = list(lithology.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
lithology = lithology.rename(columns = dictionary)
lithology = lithology.drop_duplicates ()

lithology.info()


# In[172]:


lithology.head()


# In[173]:


# other numbers

otherNumbers = pd.read_csv ('OtherNumbers_wbw_2019912_94720_DataExport_1.csv') 


col_names = list(otherNumbers.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
otherNumbers = otherNumbers.rename(columns = dictionary)
otherNumbers = otherNumbers.drop_duplicates ()

otherNumbers.info()


# In[174]:


otherNumbers.head()


# In[175]:


WaterLevels = pd.read_csv ('WaterLevels_wbw_2019912_93751_DataExport_1.csv')

col_names = list(WaterLevels.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
WaterLevels = WaterLevels.rename(columns = dictionary)
WaterLevels = WaterLevels.drop_duplicates ()

WaterLevels.info()


# In[176]:


WaterLevels.head()


# In[177]:


# water strike
waterStrike = pd.read_csv ('WaterStrike_wbw_2019912_9517_DataExport_1.csv')

col_names = list(waterStrike.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
waterStrike = waterStrike.rename(columns = dictionary)
waterStrike = waterStrike.drop_duplicates ()

waterStrike.info()


# In[178]:


waterStrike.head()


# In[179]:


# yield test
yieldtest = pd.read_csv ('Yieldtest_wbw_2019912_95158_DataExport_1.csv')

col_names = list(yieldtest.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
yieldtest = yieldtest.rename(columns = dictionary)
yieldtest = yieldtest.drop_duplicates ()


yieldtest.info()


# In[180]:


yieldtest.head()


# In[181]:


# Prosper lon data
loan_data = pd.read_csv ('prosperLoanData.csv')

col_names = list(loan_data.columns)
new_col_names = [w.replace('GeositeInfo_','') for w in col_names]
new_col_names = [x.lower() for x in new_col_names]
dictionary = dict(zip(col_names, new_col_names))
loan_data = loan_data.rename(columns = dictionary)
loan_data = loan_data.drop_duplicates ()


loan_data.info()


# In[182]:


loan_data.head()


# # Merging all NGA data for the area files
# 
# Noted: 
#  * concat() and merge() do not allow more than 2 argumuents
#  * i.e we can't do pd.merge(t,u,v,w,x,y, on="identifier, how="inner" sort=False)
#  * complains with: merge() got multiple values for argument 'on'
# 

# In[183]:


# make a master dataframe for NGA Data for the area
"""
merged_NGA_area = pd.merge(abstraction, basic_wbw, casings, depthDiam, discharge, equipment,\
                           fieldMeasurements, lithology, otherNumbers, water_levels, waterStrike,\
                           yieldtest, loan_data, on="identifier", how="inner", copy=False)
"""


# # Curate a master DF
# 
# * Make one Big Dataset containing water levels, water quality and NGA Area dataframes

# In[184]:


clean_nga_dataset.head(2)


# In[185]:


merged_WLWQ_df.head(2)


# * merge master_df with clean_NGA_dataset
# * master df is a merge of water levels + water quality
# * clean_nga_dataset: contains info from Abs,bas_wbw, depthDiam, discharge, equip etc

# In[186]:


# see similar column names in both dataframes
set(clean_nga_dataset.columns).intersection(set(merged_WLWQ_df.columns))


# # Func to get a list of duplicate columns
# * See duplicate cols

# In[187]:


# func to find duplicate columns
"""

def getDuplicateColumns(df):
    '''
    Get a list of duplicate columns.
    It will iterate over all the columns in dataframe and find the columns whose contents are duplicate.
    :param df: Dataframe object
    :return: List of columns whose contents are duplicates.
    '''
    duplicateColumnNames = set()
    # Iterate over all the columns in dataframe
    for x in range(df.shape[1]):
        # Select column at xth index.
        col = df.iloc[:, x]
        # Iterate over all the columns in DataFrame from (x+1)th index till end
        for y in range(x + 1, df.shape[1]):
            # Select column at yth index.
            otherCol = df.iloc[:, y]
            # Check if two columns at x 7 y index are equal
            if col.equals(otherCol):
                duplicateColumnNames.add(df.columns.values[y])
 
    return list(duplicateColumnNames)
"""


# # Use func to find the duplicate columns in the created DataFrame

# In[188]:


# Get list of duplicate columns
"""

duplicateColumnNames = getDuplicateColumns(clean_nga_dataset)
 
print('Duplicate Columns are as follows')
for col in duplicateColumnNames:
    print('Column name : ', col)
    
"""


# # Merge Operations: merged_WLWQ & clean_nga_dataset

# In[189]:


# Different merge operations
final_dataset = pd.concat([merged_WLWQ_df, clean_nga_dataset], axis=0, ignore_index=True)


# In[216]:


final_dataset = pd.merge([merged_WLWQ_df, clean_nga_dataset],how="outer", left_on="bh_id", right_on="bh_id")
final_dataset


# In[ ]:


# merge the 2 main dataframes 
# combine merged_WLWQ_df and clean_nga_datase

list_of_dfs = [merged_WLWQ_df, clean_nga_dataset]
from itertools import chain
list_of_dicts = [cur_df.T.to_dict().values() for cur_df in list_of_dfs]    
merged_NGA_area = pd.DataFrame(list(chain(*list_of_dicts)))


# In[ ]:


merged_NGA_area['country'] = 'RSA'


# In[ ]:


merged_NGA_area['country']


# In[ ]:


merged_NGA_area.to_csv('/Users/badisa/TWC_Datasets/Merged_files/final_dataset.csv', index= False)


# # Add Thresholds
# 
# * display some message if values are outliers
# * water quality, levels etc, too dry, too low

# In[ ]:


# define thresholds

"""

def thresholds(n):
    if n > 0.5:
        return 'too low'
    elif 2<n<=4:
        return 'good reading'
    else:
        return 'too high'
    
    water_levels_df['Threshold'] = water_levels_df['swl'].apply(thresholds)
    
"""


# # Visualizing the data
# 
# * plot static water levels for each borehole over time
# * Before plotting, convert column headings from string to integer data type, since they represent numerical values
# * visualize outliers

# In[ ]:


A3N0015 = merged_RIMS_water_levels[merged_RIMS_water_levels['bh_id'] == 'A3N0015']
A3N0015.plot.line(x='swl_date', y='swl_m', figsize=(18,6), lw = 1)

plt.title('A3N0015 Borehole', fontsize=16)
plt.xlabel('Date', fontsize = 12)
plt.ylabel('Static Water Level', fontsize = 12)


# In[ ]:


merged_RIMS_water_levels['bh_id'].unique()


# In[190]:


# plot static water levels over time 
# sort_dates = merged_RIMS_water_levels.sort('swl_date')
merged_RIMS_water_levels = merged_RIMS_water_levels.sort_values(by='swl_date',ascending=True)

merged_RIMS_water_levels.plot.line(x='swl_date', y='swl_m', figsize=(16,6))
plt.title('Static Water Levels over time',fontsize=16)
plt.xlabel('Years', fontsize =12)
plt.ylabel('Static water level',fontsize=12)


# In[191]:


# plot swl graph for each borehole
# stack the plots vertically

for bhid in merged_RIMS_water_levels['bh_id'].unique():
    ID = bhid
    bhid = merged_RIMS_water_levels[merged_RIMS_water_levels['bh_id'] == bhid]
    bhid.plot.line(x='swl_date', y='swl_m', figsize=(10.0,3.0), lw = 1)
    #bhid_title = 'ID : {}'.format(bhid)
    plt.title(ID + " "+ 'borehole', fontsize=16)
    plt.xlabel('Date', fontsize = 12)
    plt.ylabel('Static Water Level', fontsize = 12)


# # Check outliers in water levels and w_quality
# 
# * Noted: water quality and datatrans have been mapped to descriptive names and not INTs / FLOATS, making it difficult to plot
# 
# * Converted water quality, datatrans back to codes so we can plot outliers

# In[192]:


# see how the dataframe head
merged_WLWQ_df.head(1)


# # Map quality back to codes

# In[193]:


merged_WLWQ_df['quality'] = merged_WLWQ_df['quality'].map({'Audited Gauge Plate Readings / dip level readings':26, 'Dry borehole':93, 'Good continuous data':1})


# In[194]:


merged_WLWQ_df.head(2)


# In[195]:


# insert new col for descriptive water quality
merged_WLWQ_df['quality_desc'] = merged_WLWQ_df['quality']
merged_WLWQ_df.head()


# In[196]:


# add descriptive water qualities in quality_desc
merged_WLWQ_df['quality_desc'] = merged_WLWQ_df['quality_desc'].map({26:'Audited Gauge Plate Readings / dip level readings', 93:'Dry borehole', 1:'Good continuous data'})
merged_WLWQ_df.head(2)


# In[197]:


merged_WLWQ_df.to_csv("merged_WLWQ_df.csv", index=False)


# In[198]:


# categorical plots for static water levels against water quality
# %matplotlib inline

sns.set(style="ticks", color_codes=True)
sns.catplot(x='swl_m', y='quality', data=merged_WLWQ_df)

plt.show()


# In[199]:


# Generate the mean, max and min for swl_m
# start with the first 3

filenames = sorted(glob.glob('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/*_WaterLevels.csv'))
filenames = filenames[0:3]
for file in filenames:
    print(file)    


# In[200]:


data = pd.read_csv(file, names = ['swl_date','swl_m'] )

fig = plt.figure(figsize=(10.0, 3.0))

axes1 = fig.add_subplot(1, 3, 1)
axes2 = fig.add_subplot(1, 3, 2)
axes3 = fig.add_subplot(1, 3, 3)

axes1.set_ylabel('average')
axes1.plot(np.mean(data, axis=0))
axes2.set_ylabel('max')
axes2.plot(np.max(data, axis=0))
axes3.set_ylabel('min')
axes3.plot(np.min(data, axis=0))

fig.tight_layout()
plt.show()


# # TO_DO

# overlaying 
# 
# plot borehole on map, 
# 
# heatmap over the area, plot each borehole with a diffrernt, 
# 
# animate each level
# 
# df = != Nan
# 
# concat join not merge join

# # Map borehole locations

# In[201]:


# see head of the dataframe
merged_WLWQ_df.head(2)


# In[202]:


# check duplicate borehole locations

dups = [merged_WLWQ_df.duplicated(["bh_id", "longitude", "latitude"])]
dups


# # Turning regular Pandas DataFrame into a geo-DataFrame
# 
# * this require us to specify as parameters the original DataFrame, 
# * our coordinate reference system (CRS)
# * and the geometry of our new Dataframe

# In[203]:


# load shape file
#country_map = gpd.read_file('/Users/badisa/TWC_Datasets/\
#Ramotswa_Shape_files/RAMOTSW_TBA_SOUTH_AFRICA_BOREHOLES.shp')
country_map = gpd.read_file('/Users/badisa/TWC_Datasets/Ramotswa_Shape_files/South_Africa_Polygon.shp')
fig, ax = plt.subplots(figsize = (15,15))
country_map.plot(ax = ax)


# In[204]:


# Visualize borehole locations on map

bh_locations = pd.read_csv('merged_WLWQ_df.csv')
crs = {'init': 'epsg:4326'}
bh_locations.head(1)


# In[205]:


# create points, A Point is essentially a single object that describes 
# the longitude and latitude of a data-point

geometry = [Point(xy) for xy in zip(bh_locations["longitude"], bh_locations["latitude"])]
geometry[:3]


# In[206]:


geo_df = gpd.GeoDataFrame(bh_locations,
                         crs = crs,
                         geometry = geometry)

geo_df.head()


# In[207]:


# ensure you have the actual geometry objects in the dataframe
# and not only string representation
# convert the column with strings (Well Known text) to actual geometries

from shapely import wkt
#df['geometry'] = df['geometry'].apply(wkt.loads)

geo_df['geometry'] = geo_df['geometry'].apply(wkt.loads)


# In[209]:


# lay the data on the map plotted above

fig, ax = plt.subplots(figsize = (15,15))
country_map.plot(ax = ax, alpha = 0.4, color="blue")
geo_df[geo_df['bh_id'] == 0].plot(ax = ax, markersize =20,                                  color = "blue", marker = "o", label = "Neg")
geo_df[geo_df['bh_id'] == 1].plot(ax = ax, markersize =20,                                  color = "red", marker = "b", label = "Pos")
plt.legend(prop={'size':15})


# In[210]:


# cartopy and basemap tutorial
# plot the map with a single line
# geopandas library func that does that


# In[ ]:





# In[ ]:





# In[ ]:




