class StringFunctions:
	"""
		A class that contains functions for handling strings
	"""

	def find_between(input, startstring, endstring):
		"""
			Returns the first occurance of a substring between two substrings within a given string

			Parameters
			----------
			input : (string)
				The string to search within
			startstring : (string)
				The substring that acts as the header to the desired return value
			endstring : (string)
				The substring that acts as the footer to the desired return value
			
			Returns
			----------
			string : (string)
				The substring between the given strings if it was found. Empty string otherwise.
		"""
		try:
			start = input.index(startstring) + len(startstring)
			end = input.index(endstring, start)
			return input[start:end]
		except ValueError:
			return ""

class UrlParser(object):
	"""
		A UrlParser class for storing relevant url properties
	"""

	def __init__(self, url, **kwargs):
		"""
			Returns information on a url including a dictionary of parameters

			Parameters
			----------
			Url (string)
				The url to parse
			Arguments (kwargs)
				[ scheme : str = ... | subdomain : str = ... | domain : str = ... | topdomain : str = ... parameters : dict = {...} ]
			
			Returns
			----------
			UrlParser : (object)
				an object containing components of the url
		"""

		self.Url = url
		self.Scheme = kwargs.get('scheme', "")
		self.SubDomain = kwargs.get('subdomain', "")
		self.Domain = kwargs.get('domain', "Unknown")
		self.TopDomain = kwargs.get('topdomain', "")
		self.Parameters = kwargs.get('parameters', dict())
		self.Keywords = kwargs.get('keywords', "")
		self.parse_url(self.Url)

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		pass

	def parse_url(self, url):
		"""
			Parses URL information for the url provided in the constructor, and applies the information to the relevant properties of the UrlParser object.

			Parameters
			----------
			Url (string)
				The url to parse
		"""

		# parse the scheme
		pos = url.index("://")
		if pos >= 0:
			self.Scheme = url[:pos + 3]
			url = url[pos+3:]

		# parse the domains
		pos = url.index("/")
		if pos >= 0:
			domains = url[:pos].split(".")

			if len(domains):
				self.TopDomain = domains[-1]
				domains.pop()
			if len(domains):
				self.Domain = domains[-1]
				domains.pop()
			if len(domains):
				self.SubDomain = domains[-1]
				domains.pop()

			url = url[pos + 1:]

		# parse the parameters
		pos = url.index("?")
		if pos >= 0:
			parameters = url[pos+1:].split('&')
			for param in parameters:
				values = param.split('=')
				if len(values) > 1:
					self.Parameters[values[0]] = values[1]

	def get_keywords(self):
		"""
			Gets the keywords from the properties parsed by the UrlParser.

			Returns
			----------
			Keywords (string)
				The keywords used in a search. If no parameters were in the url a blank string is returned.
				Properties are ranked by most popular search engine formats first. If no match is found within
				the properties, "Unknown" is returned.
		"""

		if not len(self.Parameters):
			return ""
		elif 'q' in self.Parameters:
			return self.Parameters['q'].replace('+', " ").replace('%20', " ").title()
		elif 'p' in self.Parameters:
			return self.Parameters['p'].replace('+', " ").replace('%20', " ").title()
		elif 'text' in self.Parameters:
			return self.Parameters['text'].replace('+', " ").replace('%20', " ").title()
		elif 'wd' in self.Parameters:
			return self.Parameters['wd'].replace('+', " ").replace('%20', " ").title()
		else:
			return "Unknown"

class LogLevel():
	"""
		A class that contains an Enum of Log Levels
	"""
	NONE = 0
	ERROR = 1
	INFO = 2
	DEBUG = 3

class Logger(object):
	"""
		A simple logger that writes to a log file.
	"""

	def __init__(self, level = 1, filename = "logfile.txt"):
		self.LogFile = None
		self.ErrorCount = 0
		self.Level = level
		self.FileName = filename

	def write(self, level, message):
		"""
			Writes to a log file at the given level.

			Parameters
			----------
			level (LogLevel)
				The level of this log message.
			message (string)
				The message to write to the log. New line is added by this method.
		"""
		messagestr = ""

		if level == LogLevel.NONE:
			return
		elif level == LogLevel.INFO:
			messagestr = "Info"
		elif level == LogLevel.ERROR:
			messagestr = "Error"
			self.ErrorCount += 1
		elif level == LogLevel.DEBUG:
			messagestr = "Debug"

		self.check_file()
		self.LogFile.write(f"[{messagestr}]\t{message}\n")
	
	def check_file(self):
		"""
			Open the log file if it hasn't been opened yet. Prevents a file from being created if nothing is being written to the log.
		"""

		if self.LogFile is None:
			self.LogFile = open(self.FileName, 'w')