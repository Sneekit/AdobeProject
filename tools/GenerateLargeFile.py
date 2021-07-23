# A quick tool to generate a 10GB file for testing purposes

import os

output = "datafiles/tengigtest.csv"
outputfile = open(output, "a")

tengig = 10737418240

while os.path.getsize(output) < tengig:
	with open("datafiles/samplefile.sql") as inputfile:
		next(inputfile)
		for line in inputfile:
			outputfile.write(line)
	outputfile.write('\n')
