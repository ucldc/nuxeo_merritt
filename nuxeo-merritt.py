import argparse
from collections import namedtuple
from datetime import datetime
import json
import math
import os
import sys

import requests
from urllib.parse import quote, urlparse

import boto3

REGISTRY_BASE_URL = 'https://registry.cdlib.org'
NUXEO_TOKEN = os.environ.get('NUXEO_TOKEN')
NUXEO_API = os.environ.get('NUXEO_API')
METADATA = os.environ.get('NUXEO_MERRITT_METADATA')
FEEDS = os.environ.get('NUXEO_FEEDS')

nuxeo_request_headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-NXDocumentProperties": "*",
    "X-NXRepository": "default",
    "X-Authentication-Token": NUXEO_TOKEN
    }

DataStorage = namedtuple(
    "DateStorage", "uri, store, bucket, path"
)

def parse_data_uri(data_uri: str):
    data_loc = urlparse(data_uri)
    return DataStorage(
        data_uri, data_loc.scheme, data_loc.netloc, data_loc.path)

def load_object_to_s3(bucket, key, content):
    s3_client = boto3.client('s3')
    print(f"Writing s3://{bucket}/{key}")
    try:
        s3_client.put_object(
            ACL='bucket-owner-full-control',
            Bucket=bucket,
            Key=key,
            Body=content)
    except Exception as e:
        print(f"ERROR loading to S3: {e}")

def write_object_to_local(dir, filename, content):
    if not os.path.exists(dir):
        os.makedirs(dir)

    fullpath = os.path.join(dir, filename)
    print(f"Writing file://{fullpath}")
    with open(fullpath, "w") as f:
        f.write(content)

class NuxeoMetadataFetcher(object):
    def __init__(self, params):
        self.collection_id = params.get('collection_id')
        self.current_folder = {
            'path': params.get('path'),
            'uid': params.get('uid')
        }
        self.version = params.get('version')
        self.page_size = 100
        self.fetch_children = True

    def fetch(self):
        page_prefix = ['r']

        # get documents in root folder
        self.get_pages_of_documents(self.current_folder, page_prefix)
        
        # get documents in all folders under root
        self.folder_traversal(self.current_folder, page_prefix)

        # return something?

    def folder_traversal(self, root_folder, page_prefix):
        page_index = 0
        next_page_available = True
        while next_page_available:
            response = self.get_page_of_folders(root_folder, page_index)
            next_page_available = response.json().get('isNextPageAvailable')
            if not response.json().get('entries', []):
                next_page_available = False
                continue
            page_prefix.append(f"fp{page_index}")

            for i, folder in enumerate(response.json().get('entries', [])):
                page_prefix.append(f"f{i}")
                self.get_pages_of_documents(folder, page_prefix)
                page_prefix.pop()

            page_prefix.pop()
            page_index += 1

    def get_page_of_folders(self, folder: dict, page_index: int):
        query = (
            "SELECT * FROM Organization "
            f"WHERE ecm:path STARTSWITH '{folder['path']}' "
            "AND ecm:isVersion = 0 "
            "AND ecm:isTrashed = 0"
        )

        request = {
            'url': f"{NUXEO_API.rstrip('/')}/search/lang/NXQL/execute",
            'headers': nuxeo_request_headers,
            'params': {
                'pageSize': '100',
                'currentPageIndex': page_index,
                'query': query
            }
        }

        try:
            response = requests.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        
        return response

    def get_pages_of_documents(self, folder, page_prefix):
        page_index = 0
        next_page_available = True
        while next_page_available:
            response = self.get_page_of_parent_documents(folder, page_index)
            next_page_available = response.json().get('isNextPageAvailable')
            if not response.json().get('entries', []):
                next_page_available = False
                continue

            documents = [doc for doc in response.json().get('entries', [])]

            storage = parse_data_uri(METADATA)
            filename = f"{'-'.join(page_prefix)}-p{page_index}.jsonl"
            jsonl = "\n".join([json.dumps(record) for record in documents])
            jsonl = f"{jsonl}\n"
            if storage.store == 'file':
                dir = os.path.join(storage.path, self.version, self.collection_id)
                write_object_to_local(dir, filename, jsonl)
            elif storage.store == 's3':
                base_folder = storage.path
                s3_key = f"{base_folder.lstrip('/')}/{self.version}/{self.collection_id}/{filename}"
                load_object_to_s3(storage.bucket, s3_key, jsonl)
            
            for record in response.json().get('entries', []):
                self.get_pages_of_component_documents(record)

            page_index += 1

    def get_page_of_parent_documents(self, folder: dict, page_index: int):
        query = (
            "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:parentId = '{folder['uid']}' AND "
            "ecm:isVersion = 0 AND "
            "ecm:isTrashed = 0 ORDER BY ecm:name"
        )

        request = {
            'url': f"{NUXEO_API.rstrip('/')}/search/lang/NXQL/execute",
            'headers': nuxeo_request_headers,
            'params': {
                'pageSize': '100',
                'currentPageIndex': page_index,
                'query': query
            }
        }

        try:
            response = requests.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        
        return response

    def get_pages_of_component_documents(self, record:dict):
        page_index = 0
        next_page_available = True
        while next_page_available:
            response = self.get_page_of_components(record, page_index)
            next_page_available = response.json().get('isNextPageAvailable')
            if not response.json().get('entries', []):
                next_page_available = False
                continue

            documents = [doc for doc in response.json().get('entries', [])]

            storage = parse_data_uri(METADATA)
            filename = f"{record['uid']}-p{page_index}.jsonl"
            jsonl = "\n".join([json.dumps(record) for record in documents])
            jsonl = f"{jsonl}\n"
            if storage.store == 'file':
                dir = os.path.join(storage.path, self.version, self.collection_id, "children")
                write_object_to_local(dir, filename, jsonl)
            elif storage.store == 's3':
                base_folder = storage.path
                s3_key = f"{base_folder.lstrip('/')}/{self.version}/{self.collection_id}/{filename}"
                jsonl = "\n".join([json.dumps(record) for record in documents])
                load_object_to_s3(storage.bucket, s3_key, jsonl)

            page_index += 1

    def get_page_of_components(self, record: dict, page_index: int):
        query = (
            "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:ancestorId = '{record['uid']}' AND "
            "ecm:isVersion = 0 AND "
            "ecm:isTrashed = 0 ORDER BY ecm:name"
        )

        request = {
            'url': f"{NUXEO_API.rstrip('/')}/search/lang/NXQL/execute",
            'headers': nuxeo_request_headers,
            'params': {
                'pageSize': '100',
                'currentPageIndex': page_index,
                'query': query
            }
        }
        
        try:
            response = requests.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch components: {request}")
            raise(e)
        
        return response


def get_registry_merritt_collections():
    url = f'{REGISTRY_BASE_URL}/api/v1/collection/?harvest_type=NUX&format=json'

    merritt_collections = []
    while True:
        response = requests.get(url)
        response.raise_for_status()
        response = response.json()
        for collection in response['objects']:
            if collection['merritt_extra_data'] and collection['merritt_id']:
                collection_id = collection['resource_uri'].split('/')[-2]
                merritt_collections.append = {
                    'collection_id': collection_id,
                    'nuxeo_path': collection['merritt_extra_data'],
                    'merritt_id': collection['merritt_id']
                }

        next = response['meta']['next']
        if not next:
            break

        url = f"{REGISTRY_BASE_URL}{next}"

    return merritt_collections

def get_registry_collection(collection_id):
    url = f'{REGISTRY_BASE_URL}/api/v1/collection/{collection_id}'
    response = requests.get(url)
    response.raise_for_status()
    response = response.json()
    if response['merritt_extra_data'] and response['merritt_id']:
        return {
            'collection_id': collection_id,
            'nuxeo_path': response['merritt_extra_data'],
            'merritt_id': response['merritt_id']
        }
    else:
        raise Exception(f"Collection `{collection_id}` is not set up for Merritt.")

def get_nuxeo_uid_for_path(path):
    escaped_path = quote(path, safe=' /')
    url = u'/'.join([NUXEO_API, "path", escaped_path.strip('/')])
    headers = nuxeo_request_headers
    request = {'url': url, 'headers': headers}
    response = requests.get(**request)
    response.raise_for_status()
    json_response = response.json()
    uid = json_response['uid']

    return uid

def create_atom_feed(version, collection_id):
    # get metadata from storage
    records = []
    data = parse_data_uri(METADATA)
    if data.store == 'file':
        metadata_dir = os.path.join(data.path, version, collection_id)
        for file in os.listdir(metadata_dir):
            with open(os.path.join(metadata_dir, file), "r") as f:
                records.extend(f.readlines())
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=data.bucket,
            Prefix=version
        )
        for page in pages:
            for item in page['Contents']:
                #print(f"getting s3 object: {item['Key']}")
                response = s3_client.get_object(
                    Bucket=data.bucket,
                    Key=item['Key']
                )
                for line in response['Body'].iter_lines():
                    records.append(line)
    else:
        raise Exception(f"Unknown data scheme: {data.store}")
    
    print(f"{len(records)=}")
    
    #for record in records:

    # create feed

    # write feed to storage

def main(params):
    # get list of collections for which to create ATOM feeds
    if params.all:
        collections = get_registry_merritt_collections()
    else:
        collections = [get_registry_collection(params.collection)]

    # fetch metadata to storage
    if params.version:
        version = params.version
    else:
        version = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        for collection in collections:
            path = collection['nuxeo_path']
            uid = get_nuxeo_uid_for_path(path)
            fetcher_payload = {
                "collection_id": collection['collection_id'],
                "path": path,
                "uid": uid,
                "version": version
            }

            fetcher = NuxeoMetadataFetcher(fetcher_payload)
            fetcher.fetch()

    for collection in collections:
        # create ATOM feed
        #create_atom_feed(version, collection['collection_id'])
        pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create Nuxeo atom feed(s) for Merritt')
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help='Create all feeds', action='store_true')
    top_folder.add_argument('--collection', help='single collection ID')
    parser.add_argument('--version', help='Metadata version. If provided, metadata will be fetched from S3.')

    args = parser.parse_args()
    sys.exit(main(args))