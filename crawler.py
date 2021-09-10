import argparse
import multiprocessing
import json
import asyncio
import aiohttp
import logging
import traceback
import time
from os import path, getenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from dotenv import load_dotenv

MAX_WORKERS = multiprocessing.cpu_count()
GOOGLE_SEARCH_API_BASE = "https://www.googleapis.com/customsearch/v1"

IGNORE_TYPE = {".img", ".jpg", ".png", ".jpeg", ".gif", ".mp3", ".mp4", ".cgi", ".wav", ".avi", "wmv", "flv"}

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


async def get_req(url, timeout=5):
  try:
    resp = await session.get(url, timeout=timeout)
    resp.raise_for_status()
    return await resp.text(), True
  except:
    logging.error(traceback.format_exc())
    return None, False


# given an initial search query and return an array of seed urls
async def get_seed_urls(init_url):
  results, ok = await get_req(init_url)
  if not ok:
    raise Exception("Failed to get results from root server")
  return [item['formattedUrl'] for item in json.loads(results)['items']]


# implement the robot exclusion protocol
# this is not perferct, some paths use wildcard characters, ex: /*/api
async def robot_exclusion(url):
  exclusion_set = set()
  try:
    async with session.get(url) as resp:
      if resp.status >= 400:
        return exclusion_set
      txt = await resp.text()
      parsed_url = urlparse(url)
      self_req_scheme, self_netloc = parsed_url.scheme, parsed_url.netloc
      exclusion_set.update(
        list(map(lambda x: urljoin(f"{self_req_scheme}://{self_netloc}", x.replace("Disallow:", "", 1).strip()), 
          filter(lambda x: x.startswith("Disallow:"), txt.splitlines())
      )))
      return exclusion_set
  except:
    logging.error(traceback.format_exc())
  return exclusion_set


# A JOB as a thread pool task, which takes an url and return the sub-urls from given HTML docs
async def url_job(url):
  parsed_url = urlparse(url)
  self_req_scheme, self_netloc = parsed_url.scheme, parsed_url.netloc
  html_doc, ok = await get_req(url)
  if not ok:
    return
  soup = BeautifulSoup(html_doc, "html.parser")
  url_set = set()
  exclusion_set = await robot_exclusion(urljoin(f"{self_req_scheme}://{self_netloc}", "robots.txt"))
  for link in soup.find_all("a"):
    result = urlparse(link.get("href"))
    url = urljoin(f"{self_req_scheme}://{result.netloc if result.netloc else self_netloc}", result.path)
    if not url:
      continue
    file_name, file_extension = path.splitext(url)
    if file_extension in IGNORE_TYPE or url in exclusion_set:
      continue
    url_set.add(url)
  return url_set


async def main():
  global session
  async with aiohttp.ClientSession() as session:
    start = time.perf_counter()
    load_dotenv()
    args = parse_args()
    URLS = await get_seed_urls(f"{GOOGLE_SEARCH_API_BASE}?key={getenv('GOOGLE_SEARCH_API_KEY')}&cx={getenv('GOOGLE_SEARCH_ENGINE_ID')}&q={args.keyword}")
    s = set()
    for t in await asyncio.gather(*[url_job(url) for url in URLS]):
      s.update(t)
    print(f"Time elapsed: {time.perf_counter() - start:.3f}s, {len(s)} of URL found")

if __name__ == "__main__":
  routine = main()
  try:
    asyncio.run(routine)
  except Exception as e:
    print("[{}]: {}".format(type(e).__name__, e))
  finally:
    print("Exiting...")
  # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
  #   future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
  #   for future in as_completed(future_to_url):
  #     url = future_to_url[future]
  #     try:
  #       data = future.result()
  #     except Exception as exc:
  #       print('%r generated an exception: %s' % (url, exc))
  #     else:
  #       print('%r page is %d bytes' % (url, len(data)))