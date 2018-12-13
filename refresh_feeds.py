#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import logging
import boto3
import argparse
from merritt_atom import MerrittAtom
import requests
import json

REGISTRY_BASE = 'https://registry.cdlib.org/'

numeric_level = getattr(logging, 'INFO', None)
logging.basicConfig(
    level=numeric_level,
    format='%(asctime)s (%(name)s) [%(levelname)s]: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

def main():

    parser = argparse.ArgumentParser(description='refresh merritt atom feeds')
    parser.add_argument("--pynuxrc", help="rc file for use by pynux")
    parser.add_argument("--bucket", help="S3 bucket where feed is stashed")
    parser.add_argument("--dir", help="local directory where feed is written" )
    parser.add_argument("--nostash", action='store_true', help="write feed to local directory and do not stash on S3")

    argv = parser.parse_args()

    kwargs = {}
    if argv.pynuxrc:
        kwargs['pynuxrc'] = argv.pynuxrc
    if argv.bucket:
        kwargs['bucket'] = argv.bucket
    if argv.dir:
        kwargs['dir'] = argv.dir
    if argv.nostash:
        kwargs['nostash'] = argv.nostash

    feeds = get_feed_info()

    # create and stash new feed for each collection
    for key, value in feeds.items():
        print(value['nuxeo_endpoint'])
        if value['nuxeo_endpoint'] == 'asset-library/UCM/Ramicova':
            ma = MerrittAtom(key, merritt_id=value['merritt_id'], nuxeo_path=value['nuxeo_endpoint'], **kwargs)
            ma.process_feed()

def get_feed_info():
    ''' get list of collections for which to create feeds, based on registry info '''

    feed_md = {}

    url = "{}/api/v1/collection/?harvest_type=NUX&format=json".format(REGISTRY_BASE)

    while True:
        res = requests.get(url)
        res.raise_for_status()
        md = json.loads(res.content)

        for collection in md['objects']:
            if collection['merritt_extra_data'] and collection['merritt_id']:
                collection_id = collection['resource_uri'].split('/')[-2]
                feed_md[collection_id] = {'nuxeo_endpoint': collection['merritt_extra_data'], 'merritt_id': collection['merritt_id']}


        next = md['meta']['next']
        if not next:
            break

        url = "{}{}".format(REGISTRY_BASE, next)
 
    return feed_md

def refresh_existing(kwargs):
    ''' refresh feeds already on s3 '''

    # get a list of current feeds on S3
    bucketbase = 'static.ucldc.cdlib.org'
    prefix = 'merritt/'
 
    s3 = boto3.resource('s3')

    bucket = s3.Bucket(bucketbase)

    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith('.atom'):
            # get collection ID for each existing ATOM file
            filename = obj.key.split(prefix)[1]
            basename = filename.split('.')[0]
            collection_id = basename.split('_')[-1]
            logger.info("collection_id: {}".format(collection_id))

            # create and stash new feed for each
            ma = MerrittAtom(collection_id, **kwargs)
            ma.process_feed()

if __name__ == "__main__":
    sys.exit(main())
