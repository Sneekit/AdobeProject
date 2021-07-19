from Helpers import UrlParser, LogLevel, Logger
import sys
import os
import time
import math
from boto3 import client as botoclient
from datetime import date

# constants for the program to use
LOCALTEST = True
DATESTAMP = date.today().strftime(("%Y-%m-%d"))
RESULTFILE = f"/tmp/{DATESTAMP}_SearchKeywordPerformance.tab"
TEMPFILE = f"/tmp/{DATESTAMP}_tempfile"
LOGFILE = f"/tmp/{DATESTAMP}_Log.txt"
S3CLIENT = botoclient('s3')
BUCKET = ""
KEY = ""

# create logger object
log = Logger(LogLevel.DEBUG, LOGFILE)

# Arguments
# 1 - S3 File to Process

if LOCALTEST == False and len(sys.argv) < 2:
	print("Syntax: python3 ProcessFile.py <s3 filename>")
	quit(1)

def download_s3_file(filename):
	"""
		Download a the file from S3 to process.
	"""

	try:
		pos = filename.index('/')
		BUCKET = filename[:pos]
		KEY = filename[pos+1:]

		INFILE = "/tmp/inputdata"
		S3CLIENT.download_file(BUCKET, KEY, INFILE)
	except Exception as ex:
		log.write(LogLevel.ERROR, f"Error downloading {BUCKET}/{KEY} from S3: {ex}")
		quit(1)

def process_input_file():
	"""
		Iterate through the input file line by line to parse and clean data.
	"""

	# dictionary that stores relevant ip addresses
	addressDict = dict()

	# dictionary that stores grouped results
	resultsDict = dict()

	linenumber = 0
	starttime = time.time()

	with open(INFILE) as inputFile:
		# skip header record
		next(inputFile)

		for line in inputFile:
			linenumber += 1

			# error catch bad lines so one bad line doesn't fail the entire file
			try: 
				# get an array of items for each line in the file and store it as a 'row'
				row = line.split(sep = '\t')

				if len(row) != 12:
					log.write(LogLevel.ERROR, f"Line: {linenumber}\t Record does not contain 12 columns. Possible invalid file format.")
					continue

				# get the events from column 5
				events = row[4].split(',')

				# handle external reference storage
				if 'esshopzilla' not in row[11]:
					with UrlParser(row[11]) as parsedurl:
						if len(parsedurl.Parameters):
							# store domain and search details in dictionary with associated ip address
							addressDict[row[3]] = f"{parsedurl.Domain.capitalize()}|{parsedurl.get_keywords()}"

				# handle an actualized revenue record if we have a matching external reference for the ip address
				if events is not None and '1' in events and row[3] in addressDict:
					totalrevenue = 0
					productlist = row[10].split(',')

					# iterate through product list to calculate total revenue for this actualized event
					if len(productlist) > 0:
						for product in productlist:
							productinfo = product.split(';')
							if len(productinfo) >= 4:
								revenue = float(productinfo[3])
								if revenue >= 0:
									totalrevenue += revenue
							else:
								log.write(LogLevel.ERROR, f"Line: {linenumber}\t Invalid Product Attribute in Product List: {product}")
					else:
						# no product in productlist
						log.write(LogLevel.ERROR, f"Line: {linenumber}\t Record shows a verified purchase, but no products are listed.")

					if totalrevenue > 0:
						# retrieve domain information from the ip address dictionary
						domaininfo = addressDict[row[3]]

						# the domain information is grouped by domain and keywords, as it is already grouped sum up the values now
						if domaininfo in resultsDict:
							resultsDict[domaininfo] += totalrevenue
						else:
							resultsDict[domaininfo] = totalrevenue

						log.write(LogLevel.DEBUG, f'purchase found for ip {row[3]}')
					else:
						# totalrevenue is 0 or lower
						log.write(LogLevel.ERROR, f"Line: {linenumber}\t Record shows a verified purchase, but total revenue could not be determined.")

			except Exception as ex:
				# error procesing line
				log.write(LogLevel.ERROR, f"Line: {linenumber}\t unhandled exception processing line: {ex}")

	# write the grouped results out to a csv file for sorting
	with open(TEMPFILE, 'w') as outputFile:
		for domain, revenue in resultsDict.items():
			# parse keywords from domain
			domaininfo = domain.split("|")
			# write domain and revenue to output file
			outputFile.write(f"{domaininfo[0]}\t{domaininfo[1]}\t{revenue}\n")

	# track how long parsing took
	timedelta = time.time() - starttime
	minutes = math.floor(timedelta % 60)
	seconds = round(timedelta - (minutes * 60), 3)
	log.write(LogLevel.DEBUG, f"Preprocess Time: {minutes}:{seconds}")

def sort_results():
	"""
		Sorts the results from the procesed file.
	"""

	# print in header
	with open(RESULTFILE, 'w') as sortedFile:
		sortedFile.write("Search Engine Domain\tSearch Keyword\tRevenue\n")

	# hard to beat a classic unix sort command
	os.system(f"sort -k 3nr {TEMPFILE} >> {RESULTFILE}")

	# remove processed file from storage, worth noting that both files will exist on disk at the same time
	os.remove(TEMPFILE)

def upload_s3_files():
	"""
		Upload log file and result file to S3 bucket
	"""

	S3CLIENT.upload_file(BUCKET,f"/outound/{os.path.basename(LOGFILE)}",LOGFILE)
	S3CLIENT.upload_file(BUCKET,f"/outound/{os.path.basename(RESULTFILE)}",RESULTFILE)

# MAIN ENTRY POINT
if LOCALTEST == True:
	INFILE = "datafiles/samplefile.sql"
else:
	download_s3_file(sys.argv[1])

process_input_file()
sort_results()

log.write(LogLevel.INFO, f"File finished processing.")
log.write(LogLevel.INFO, f"Exceptions: {log.ErrorCount}")

if LOCALTEST == False:
	try:
		upload_s3_files()
	except Exception as ex:
		print(f"Error uploading files to S3: {ex}")

os.remove(LOGFILE)
os.remove(RESULTFILE)
