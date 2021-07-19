"""
	A quick tool to generate a 10GB file for testing purposes
"""

import os

output = "datafiles/tengigtest.csv"
outputFile = open(output, 'a')

tengig = 10737418240

while os.path.getsize(output) < tengig:
	with open("datafiles/samplefile.sql") as inputFile:
		next(inputFile)
		for line in inputFile:
			outputFile.write(line)
	outputFile.write('\n')
