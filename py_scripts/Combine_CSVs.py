#!/usr/bin/env python
import os
import glob
import pandas as pd

# set working directory
os.chdir("/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/")

# set file paths
# water levels, water quality and nga area files paths
nga_wms_path = "/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/"
nga_area_path = ("/Users/badisa/TWC_Datasets/NGA/NGA_Data_for_the_area_13_09_2019/")
TWC_datasets = "/Users/badisa/TWC_Datasets/"
merged_files_path = "/Users/badisa/TWC_Datasets/Merged_files/"
shape_files_path = "/Users/badisa/TWC_Datasets/Ramotswa_Shape_files/"

"""
Find all csv files in the directory
Use glob pattern matching -> extension = 'csv'
Save result in list -> twc_filenames 
"""

extension = 'csv'

#water_levels_filenames = [i for i in glob.glob('*.{}'.format(extension))]
water_levels_filenames = [os.path.basename(file1) for file1 in glob.glob(f'{nga_wms_path}/*_WaterLevels.csv')]

# print all filenames
print("Water levels filenames:\n", water_levels_filenames)

# combine all files in the list
combined_CSVs = pd.concat([pd.read_csv(f) for f in water_levels_filenames])

# export to csv
print("\n Exporting the data to a csv file -> combined_csv.csv ...")
combined_CSVs.to_csv("/Users/badisa/TWC_Datasets/Merged_files/combined_csv.csv", index=False, encoding='utf-8-sig')


