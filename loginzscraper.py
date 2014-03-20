from bs4 import BeautifulSoup
from time import localtime, strftime, sleep
from os import remove, fsync
import argparse
import random
import sys
import urllib2

"""
Globals
"""
USER_AGENT = 'Loginz-Scraper1.0 (github.com/Gradous/Loginz-Scraper)'
SCRAPED_SET = set() # contains tuples of (user, pass, rating) to dup check
NO_RESULT = u'Не найдено' # Russian is neat

"""
Main scraping/spidering function
"""
def scrape(url):

	# loginz.org has multiple pages
	def page_scrape(webpage):
		result_check = BeautifulSoup(webpage).findAll(NO_RESULT)
		if not result_check:
			return False
		return True
	try:
		page_count = 1
		# loginz.org will allow for infinite "page numbers" to be placed in the
		# URL, but it will simply return the actual last page for each. This
		# loop will break when a duplicate account is detected in the set
		# (page_scrape returns False)
		while(page_count):
			req_url = 'http://loginz.org/view/' + url + '/' + str(page_count)
			loginz_req = urllib2.Request(req_url, 
				headers={'User-agent' : USER_AGENT})
			loginz_response = urllib2.urlopen(loginz_req)
			if not page_scrape(loginz_response.read()):
				break
			loginz_response.close()
			page_count += 1

		# final close
		loginz_response.close()

		# Buckets for parsing, will stay empty if no results
		usernames = []
		passwords = []
		rates = []

		for s in SCRAPED_SET:
			usernames.append(s[0])
			passwords.append(s[1])
			rates.append(s[2])

		# return the list of tuples for later parsing
		return zip(usernames, passwords, rates, votes, ages)
	except urllib2.HTTPError, e:
		print "Error code: ", e.code
		# in the odd case of 404, keep going
		if e.code == 404:
			print url, "- HTTP 404"
			return None
		else:
			raise e(e.fp.read())


"""
Write out the results to a file
TODO: Add more stats (numpy?)
"""
def write_result(url, results, log):
	# result tuple = (user, pass, success %, votes, age)
	with open(log, 'a+') as logfile:
		stats = [] # for some averages and whatnot
		for r in results:
			logfile.write(url + ',')
			logfile.write(','.join([d if d is not None else "#None#" for d in r])\
			.encode("UTF-8"))
			logfile.write('\n')
		logfile.flush()
		fsync(logfile)

def parse_args():
	parser = argparse.ArgumentParser(description='Scrape Loginz.org for valid accounts')
	parser.add_argument('-f', '--file', nargs=1, help='Site list for scraping',
		default=['alexa_top_1m.csv'])
	parser.add_argument('-g', '--generate', nargs=1,
		help="""Use the Alexa list instead and write out working sites
		to a new file.""")
	parser.add_argument('-n', '--no-results', action='store_false',
		help="Don't write results to file")
	parser.add_argument('-m', '--max-sites', nargs=1, help="Max sites to parse",
		default=[1000000])
	parser.add_argument('-o', '--output', nargs=1,
		default=['result_' + strftime("%m-%d-%Y_%H-%M-%S", localtime()) + '.txt'],
		help='Result output file. Defaults to current date and time.')
	parser.add_argument('-s', '--skip', nargs=1, default=[1], 
		help='Skip to entry X before scraping')
	return parser.parse_args()

def update_gen_file(gen_file, result_number, url):
	if gen_file:
		gen_file.write(str(result_number) + ',' + url + '\n')
		gen_file.flush()
		fsync(gen_file)

def report_results(url, result, gen_file, result_num, writeout, log):
	print url, "has", len(result), "results!"
	# write out the working sites to a new file?
	update_gen_file(gen_file, result_num, url)
	if writeout:
		write_result(url, result, log)

def main(scrape_file, gen_file, min_wait=1.0, max_wait=3.5, **kwargs):
	# seed for waiting
	random.seed()

	if gen_file == scrape_file:
		raise IOError("HEY! Don't use the same file for two things!!!")

	try:
		with open(scrape_file, 'r') as to_scrape:
			if (gen_file):
				genfile = open(gen_file[0], 'w+')
			site_counter = kwargs['site_counter'] # loop break, default=1
			result_number = 1 # counter for filtered set
			for site in to_scrape:
				url = site.rsplit(',')[1].strip()
				url_num = site.rsplit(',')[0].strip()
				# --skip option takes effect here
				if int(url_num) == int(site_counter):
					# get the result, None = failure
					site_result = scrape(url)
					if site_result:
						# record the results
						report_results(url, site_result, gen_file,
							result_number, kwargs['writeout'],
							kwargs['logfile'])
						result_number += 1
					if site_counter >= int(kwargs['site_counter']) +\
					 int(kwargs['max_sites'] - 1):
						break
					# don't want to DoS...
					sleep(random.uniform(min_wait, max_wait))
					site_counter += 1
			
	except IOError, e:
		raise IOError("File " + e.filename + " does not exist!")
	except KeyboardInterrupt, e2:
		# ask to delete the incomplete logfile
		if kwargs['writeout']:
			if raw_input("Interrupted. Delete the results" +\
				" file? (Y/N) ").upper() == 'Y':
				try: # just in case python didn't actually write yet...
					remove(kwargs['logfile'])
				except OSError, e:
					pass

if __name__ == "__main__":	
	args = parse_args()
	main(writeout=args.no_results, max_sites=int(args.max_sites[0]),
		logfile=args.output[0], scrape_file=args.file[0],
		gen_file=args.generate, site_counter=int(args.skip[0]))
