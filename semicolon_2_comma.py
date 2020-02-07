#!/usr/bin/env python
  
import sys
import csv
import os.path
import re
from csv import reader, writer

# get input from the user
fileName = input("Please enter the name of file to be fixed: ")

# check if file exists

while not os.path.isfile(fileName):
	print('Sorry! File {0} not found.'.format(fileName))
	fileName = input("Please enter the name of file to be fixed: ")

# read in file
reader = csv.reader(open(fileName,"r"), delimiter=";")

cleanedFile = input('Enter the name of the new file: ' )

writer = csv.writer(open(cleanedFile,"w"), delimiter=",")

cleanedFile = input('Enter the name of the new file: ' )
writer.writerows(reader)



