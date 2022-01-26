from Helpers import S3Client, UrlParser, LogLevel, Logger
import sys
import os
import time
import math
from datetime import date

# Hey Sariah


# constants for the program to use
LOCALTEST = False
DATESTAMP = date.today().strftime(("%Y-%m-%d"))
FILEDIR = "datafiles" if LOCALTEST else "/tmp"
RESULTFILE = f"{FILEDIR}/{DATESTAMP}_SearchKeywordPerformance.tab"
TEMPFILE = f"{FILEDIR}/{DATESTAMP}_tempfile"
LOGFILE = f"{FILEDIR}/{DATESTAMP}_Log.txt"

# create S3 client and Logger
S3 = S3Client()
LOG = Logger(LogLevel.DEBUG, LOGFILE)

def get_s3_stream():
	"""
		Opens a Stream to the s3 file provided in the command line argument.

		Returns
		---------
		filestream (Stream)
			A stream pointed toward the s3 object passed in from the command line.
	"""

	# parse s3 path from arguments
	S3.parse_path(sys.argv[1])

	try:
		LOG.write(LogLevel.INFO, f"Processing File: s3://{S3.Bucket}/{S3.Key}")
		return S3.Resource.Object(S3.Bucket, S3.Key).get()['Body'].iter_lines()
	except Exception as ex:
		LOG.write(LogLevel.ERROR, f"Error creating stream from s3://{S3.Bucket}/{S3.Key}: {ex}")
		quit(1)

def parse_input_file(filestream, outputfile):
	"""
		Iterate through the input stream line by line to parse and clean data.

		Parameters
		----------
		filestream (string)
			The stream to read line by line
		outputfile (string)
			The local storage location to save the processed and merged file
	"""

	# dictionary that stores relevant ip addresses (IP: DomainInfo)
	addressdict = dict()
	# dictionary that stores grouped sum of revenue based on domain info (Domain|KeyWords: Revenue)
	resultsdict = dict()
	# dictionary for storing column names and values
	row = dict()

	linenumber = 0
	starttime = time.time()

	# skip header record
	next(filestream)

	for line in filestream:
		linenumber += 1

		if type(line) is bytes:
			line = line.decode("utf8")

		# error catch bad lines so one bad line doesn't fail the entire file
		try: 
			# parse each line in to a row dictionary
			columns = line.split(sep = "\t")
			if len(columns) != 12:
				LOG.write(LogLevel.ERROR, f"Line: {linenumber}\t Record does not contain 12 columns. Possible invalid file format.")
				continue

			row['ip'] = columns[3]
			row['events'] = columns[4]
			row['productlist'] = columns[10]
			row['url'] = columns[11]

			# handle external reference storage
			if 'esshopzilla' not in row['url']:
				with UrlParser(row['url']) as parsedurl:
					if len(parsedurl.Parameters):
						# store domain and search details in dictionary with associated ip address
						addressdict[row['ip']] = f"{parsedurl.Domain}|{parsedurl.get_keywords()}"

			# get the events from column 5
			events = row['events'].split(",")

			# handle an actualized revenue record if we have a matching external reference for the ip address
			if events is not None and '1' in events and row['ip'] in addressdict:
				totalrevenue = 0
				productlist = row['productlist'].split(",")

				# iterate through product list to calculate total revenue for this actualized event
				if len(productlist) > 0:
					for product in productlist:
						productinfo = product.split(";")
						if len(productinfo) >= 4:
							revenue = float(productinfo[3])
							if revenue >= 0:
								totalrevenue += revenue
						else:
							LOG.write(LogLevel.ERROR, f"Line: {linenumber}\t Invalid Product Attribute in Product List: {product}")
				else:
					# no product in productlist
					LOG.write(LogLevel.ERROR, f"Line: {linenumber}\t Record shows a verified purchase, but no products are listed.")

				if totalrevenue > 0:
					# retrieve domain information from the ip address dictionary
					domaininfo = addressdict[row['ip']]

					# the domain information is grouped by domain and keywords, as it is already grouped sum up the values now
					if domaininfo in resultsdict:
						resultsdict[domaininfo] += totalrevenue
					else:
						resultsdict[domaininfo] = totalrevenue

					LOG.write(LogLevel.DEBUG, f"Purchase found for ip {row['ip']}")
				else:
					# totalrevenue is 0 or lower
					LOG.write(LogLevel.ERROR, f"Line: {linenumber}\t Record shows a verified purchase, but total revenue could not be determined.")

		except Exception as ex:
			# error procesing line
			LOG.write(LogLevel.ERROR, f"Line: {linenumber}\t unhandled exception processing line: {ex}")

	# write the grouped results out to a csv file for sorting
	with open(outputfile, "w", buffering = 16 * 1024 * 1024) as outfile:
		for domain, revenue in resultsdict.items():
			# parse keywords from domain
			domaininfo = domain.split("|")
			# write domain and revenue to output file
			outfile.write(f"{domaininfo[0]}\t{domaininfo[1]}\t{revenue}\n")

	# track how long parsing took
	display_processtime(starttime, "Parsing")

def sort_results(filename, outputfile):
	"""
		Sorts the results from the given file based on column 3 descending. Removes the input file when it is complete.

		Parameters
		----------
		filename (string)
			The file in local storage to process
		outputfile (string)
			The local storage location to save the sorted file
	"""

	starttime = time.time()

	# print in header
	with open(outputfile, 'w') as sortfile:
		sortfile.write("Search Engine Domain\tSearch Keyword\tRevenue\n")

	# hard to beat a classic unix sort command
	os.system(f"sort -k 3nr -t '\t' {filename} >> {outputfile}")

	# remove processed file from storage, worth noting that both files will exist on disk at the same time
	os.remove(filename)

	# track how long sorting took
	display_processtime(starttime, "Sorting")

def process_s3_files():
	"""
		Upload log file and result file to S3 bucket, and move processed file to 'processed' directory.
	"""

	# flush the log file
	LOG.flush()

	S3.Client.upload_file(LOGFILE, S3.Bucket, f"outbound/{os.path.basename(LOGFILE)}")
	S3.Client.upload_file(RESULTFILE, S3.Bucket, f"outbound/{os.path.basename(RESULTFILE)}")

	# copy processed file from inbound to processed directory
	copysource = {
		'Bucket': S3.Bucket,
		'Key' : S3.Key
	}
	S3.Resource.meta.client.copy(copysource, S3.Bucket, f"processed/{DATESTAMP}_{S3.BaseName}")
	S3.Resource.Object(S3.Bucket, S3.Key).delete()

def display_processtime(starttime, process):
	"""
		Display minutes and seconds that have passed since starttime was defined

		Parameters
		----------
		starttime (string)
			The start time to compare to now
		process (string)
			The process name that will appear in the log
	"""

	timedelta = time.time() - starttime
	minutes = math.floor(timedelta % 60)
	seconds = round(timedelta - (minutes * 60), 4)
	LOG.write(LogLevel.DEBUG, f"{process} Time: {minutes}:{seconds}")

def main():
	"""
		Main entry point for parse program.
	"""

	if LOCALTEST == False and len(sys.argv) < 2:
		print("Syntax: python3 ProcessFile.py <s3 filename>")
		quit(1)

	if LOCALTEST == True:
		filestream = open(f"{FILEDIR}/samplefile.sql")
		parse_input_file(filestream, TEMPFILE)
	else:
		filestream = get_s3_stream()
		parse_input_file(filestream, TEMPFILE)

	sort_results(TEMPFILE, RESULTFILE)

	LOG.write(LogLevel.INFO, f"File finished processing.")
	LOG.write(LogLevel.INFO, f"Exceptions: {LOG.ErrorCount}")

	if LOCALTEST == False:
		try:
			process_s3_files()
		except Exception as ex:
			LOG.write(LogLevel.ERROR, f"Error uploading files to S3: {ex}")

		# clear up files if not running a local test
		os.remove(LOGFILE)
		os.remove(RESULTFILE)

main()
