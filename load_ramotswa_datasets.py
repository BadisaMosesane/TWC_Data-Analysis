#!/usr/bin/env python
import os
import pandas as pd
import numpy as np 
import glob
import re
from os import listdir
import datetime as dt
# import xlrd

# read in the csv files
def read_ramotswa_datasets(nga_wms_path):
    ramotswa_1_df = pd.read_csv(f'{nga_wms_path}/Ramotswa_data_1_20190904.csv', header = 0, encoding = 'unicode_escape')
    rams1 = ramotswa_1_df.copy()
    return ramotswa_1_df

def clean_rams1(nga_wms_path):
    ramotswa_1_df = pd.read_csv(f'{nga_wms_path}/Ramotswa_data_1_20190904.csv', header = 0, encoding = 'unicode_escape')
    cleaned_rams1 = ramotswa_1_df.iloc[:, np.r_[:19, 19,22,25,28,31,34,37,40,43,46,49,52,\
                               55,58,61,64,67,70,73,76,79,82,85,88,91,94,97,100,103,106]]

    cleaned_rams1.rename(columns = {'Monitoring Point ID':'ID Monitoring Point'}, inplace = True)
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

    # insert data source details i.e. data owner, contact, email, and filename into cleaned_rams1 df
    cleaned_rams1['dataowner'] = "Department of Water and Sanitation South Africa"
    cleaned_rams1['contact_person'] = "Ramusiya, Fhedzisani"
    cleaned_rams1['email'] = "RamusiyaF@dws.gov.za"
    cleaned_rams1['file_name'] = "Ramotswa_data_1_20190904.csv"

    return cleaned_rams1
    

def clean_rams2(nga_wms_path):
    # clean Ramotswa 2 df
    ramotswa_2_df = pd.read_csv(f'{nga_wms_path}/Ramotswa_data_2_20190904.csv', header = 0, encoding = 'unicode_escape')
    cleaned_rams2 = ramotswa_2_df.copy()
    cleaned_rams2 = ramotswa_2_df.iloc[:, np.r_[:19, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49, 52, 55, 58, 61, 64, 67, 70, 73, 76, 79, 82, 85, 88, 91]]
    
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

    # insert data source details i.e. data owner, contact, email, and filename into cleaned_rams2 df
    cleaned_rams2['dataowner'] = "Department of Water and Sanitation South Africa"
    cleaned_rams2['contact_person'] = "Ramusiya, Fhedzisani"
    cleaned_rams2['email'] = "RamusiyaF@dws.gov.za"
    cleaned_rams2['file_name'] = "Ramotswa_data_2_20190904.csv"
    
    return cleaned_rams2

# merge Ramotswa datasets vertically
def merge_DFs(cleaned_rams1, cleaned_rams2):  
    merged_ramotswa = pd.concat([cleaned_rams1, cleaned_rams2],axis=0, ignore_index=True, sort=False)
    merged_ramotswa.rename(columns={'HARD-Tot-Water ': 'HARD-Tot-Water'}, inplace = True)
    return merged_ramotswa

