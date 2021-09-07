import argparse
import multiprocessing
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib import request
from bs4 import BeautifulSoup
from dotenv import load_dotenv

MAX_WORKERS = multiprocessing.cpu_count()
GOOGLE_SEARCH_API_BASE = "https://www.googleapis.com/customsearch/v1"


# 1. start from a set of seed pages obtained from a major search engine
# given a query (a set of keywords) provided by auser, your crawler should contact a major search engine 
# and get the top, say, 10 results for this query

# 2. output a log file
# a list of all visited URLs ordered by visited time
#    size of each page in bytes
#    depth of each page
#    proproty score
#    time of the download

# 3. PageRank or number of links discovered in the webpage
# novelty and importance in priority queue
# novelty: initially MAX_VALUE, decrease it whenever same link shows up
# importance: initially 0, more appearance, higher score

# 4. use dictionary to avoid visited page; discard status >= 400 pages; only crawl MIME type = text/html page

# 5. Robot exclusion protocol
# /robots.txt, do not crawl Disallow
# Ex: Disallow: /local/tab/
# Or, simply skip entire site: Disallow: /
# also, ignore all .cgi sites

def parse_args():
  parser = argparse.ArgumentParser(description='Web crawler')
  parser.add_argument("-d", "--depth", help="Maximum crawling depth", action="store_true", default=3)
  parser.add_argument("-k", "--keyword", help="Search keyword", action="store_true", default="python")
  return parser.parse_args()

def load_url(obj, timeout=3000):
  with request.urlopen(obj, timeout=timeout) as conn:
    return conn.read().decode('utf-8')

# {
#   "kind": "customsearch#result",
#   "title": "The Python Standard Library — Python 3.9.7 documentation",
#   "htmlTitle": "The \u003cb\u003ePython\u003c/b\u003e Standard Library — \u003cb\u003ePython\u003c/b\u003e 3.9.7 documentation",
#   "link": "https://docs.python.org/3/library/",
#   "displayLink": "docs.python.org",
#   "snippet": "It also describes some of the optional components that are commonly included in Python distributions. Python's standard library is very extensive, offering a ...",
#   "htmlSnippet": "It also describes some of the optional components that are commonly included in \u003cb\u003ePython\u003c/b\u003e distributions. \u003cb\u003ePython&#39;s\u003c/b\u003e standard library is very extensive, offering a&nbsp;...",
#   "cacheId": "nTas6KVcOSQJ",
#   "formattedUrl": "https://docs.python.org/3/library/",
#   "htmlFormattedUrl": "https://docs.\u003cb\u003epython\u003c/b\u003e.org/3/library/"
# },
def get_seed_urls(init_url):
  results = load_url(init_url)
  return [item['formattedUrl'] for item in json.loads(results)['items']]

if __name__ == "__main__":
  load_dotenv()
  args = parse_args()
  URLS = get_seed_urls(
    "{}?key={}&cx={}&q={}".format(
      GOOGLE_SEARCH_API_BASE, 
      os.getenv("GOOGLE_SEARCH_API_KEY"), 
      os.getenv("GOOGLE_SEARCH_ENGINE_ID"), 
      args.keyword)
  )

  with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
    for future in as_completed(future_to_url):
      url = future_to_url[future]
      try:
        data = future.result()
      except Exception as exc:
        print('%r generated an exception: %s' % (url, exc))
      else:
        print('%r page is %d bytes' % (url, len(data)))