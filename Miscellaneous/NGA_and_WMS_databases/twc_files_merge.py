#!/usr/bin/env python3
import os
import pandas as pd
import glob
import numpy
import matloplib.pyplot 

# sort filenames in alphabetical order using inbuilt sort func
water_level_filenames = sorted(glob.glob('/Users/badisa/TWC_Datasets/NGA/NGA_and_WMS_databases/*_WaterLevels.csv')
print(water_level_filenames)
# loop through files and print them
#for fname in water_level_filenames:
#   print(fname)

