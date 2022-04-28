#!/usr/bin/env python3

import argparse
import requests
import urllib.request
from requests.compat import urljoin
from datetime import datetime, timedelta
from google.cloud import storage
from lxml.html import etree
from multiprocessing.pool import ThreadPool
from time import time as timer

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def extract_links_with_ext(url, extension):
    response = requests.get(url)
    doc_tree = etree.HTML(response.content)
    hrefs = doc_tree.xpath('//a/@href')
    partial_links = [ref for ref in hrefs if ref.endswith(extension)]
    return list({urljoin(response.url, ref) for ref in partial_links})

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "not a valid date: {0!r}".format(s)
        raise argparse.ArgumentTypeError(msg)

def get_file_from_url(url):
    try:
        return urllib.request.urlopen(url).read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f'file not found for {url}')
            return None
        else:
            raise

def upload_to_gcs(data, filename, bucket):
    blob = bucket.blob(filename)
    blob.upload_from_string(data)
    return blob.public_url

def process_files(arg_tuple):
    data = get_file_from_url(arg_tuple[0])
    if data:
        public_url = upload_to_gcs(data, arg_tuple[1], arg_tuple[2])


parser = argparse.ArgumentParser(description="upload files from blockchair to gcs for a given range of dates")
parser.add_argument('-s', '--start_date', help='start date, format yyyy-mm-dd', required=True, type=valid_date,)
parser.add_argument('-e', '--end_date', help='end date (inclusive), format yyyy-mm-dd', required=True, type=valid_date,)
parser.add_argument('-b', '--bucket', help='destination bucket (gcs)', required=True)
parser.add_argument('-p', '--parallelism', help='process files in parallel batches', default=1, type=int)
parser.add_argument('--skip_bucket_check', help='skip checking if the file already exists in gcs', action='store_true')
args = parser.parse_args()

# BASE_URL is not a cli arg because the file and date
# format is specific to this url
BASE_URL = 'https://wtf-data.kll.io/'
FILE_PREFIX = ''
BUCKET_ID = args.bucket
START_DATE = args.start_date
END_DATE = args.end_date
BATCH_SIZE = args.parallelism
SKIP = args.skip_bucket_check

dates_generated = [START_DATE + timedelta(days=x) for x in range(0, (END_DATE-START_DATE).days + 1)]
bucket = storage.Client().get_bucket(BUCKET_ID)

date_start = timer()
for date in dates_generated:
    datefmt = date.strftime("%Y/%m/%d")
    url = f'{BASE_URL}/{datefmt}'

    files = extract_links_with_ext(url, 'parq')

    for batch in chunker(files, BATCH_SIZE):
        start = timer()
        # before getting the file from source,
        # check if we already have it
        # this allows to re-run the script for the same
        # time range without redoing all the work

        ready_for_upload = []
        for f in batch:
            filename = f.split('/')[-1]
            file_dest = f'{datefmt}/{filename}'
            if not SKIP:
                if not bucket.blob(file_dest).exists():
                    ready_for_upload.append((f, file_dest, bucket))
                else:
                    print(f'{filename} already present. skipping')
            else:
                ready_for_upload.append((f, file_dest, bucket))

        with ThreadPool(BATCH_SIZE) as pool:
            pool.map(process_files, ready_for_upload)
            print(f'processed {len(ready_for_upload)} files in {timer() - start} seconds')
    print(f"processed {date} in {timer() - date_start} seconds")
