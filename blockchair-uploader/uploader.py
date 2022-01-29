#!/usr/bin/env python3

import argparse
import urllib.request
from datetime import datetime, timedelta
from google.cloud import storage

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

parser = argparse.ArgumentParser(description="upload files from blockchair to gcs for a given range of dates")
parser.add_argument('-s', '--start_date', help='start date, format yyyy-mm-dd', required=True, type=valid_date,)
parser.add_argument('-e', '--end_date', help='end date (inclusive), format yyyy-mm-dd', required=True, type=valid_date,)
parser.add_argument('-b', '--bucket', help='destination bucket (gcs)', required=True)
args = parser.parse_args()

# BASE_URL is not a cli arg because the file and date
# format is specific to this url
BASE_URL = 'https://gz.blockchair.com/bitcoin/blocks'
FILE_PREFIX = 'blockchair_bitcoin_blocks_'
BUCKET_ID = args.bucket
START_DATE = args.start_date
END_DATE = args.end_date


client = storage.Client()
bucket = client.get_bucket(BUCKET_ID)
dates_generated = [START_DATE + timedelta(days=x) for x in range(0, (END_DATE-START_DATE).days + 1)]

for date in dates_generated:
    datefmt = date.strftime("%Y%m%d")
    filename = f'{FILE_PREFIX}{datefmt}.tsv.gz'
    url = f'{BASE_URL}/{filename}'

    # before getting the file from blockchair,
    # check if we already have it
    # this allows to re-run the script for the same
    # time range without redoing all the work
    if not bucket.blob(filename).exists():
        data = get_file_from_url(url)
        if data:
            public_url = upload_to_gcs(data, filename, bucket)
            print(f'successfully uploaded: {public_url}')
    else:
        print(f'{filename} already present. skipping')

