#!/usr/bin/env python

import sys
import csv
import os
import re
from csv import reader, writer

reader = csv.reader(open("SITE_INFORA10A.CSV","r"), delimiter=";")
writer = csv.writer(open("cleanedFile.csv","w"), delimiter=",")
writer.writerows(reader)


"""

infile = open("SITE_INFORA10A.CSV", "r")
outfile = open("cleanedFile.csv", "w")

csvReader = csv.reader(infile)

for line in csvReader:
	x = re.sub(";", ",", line)
	print(x)

infile.close()
outfile.close()

"""




