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
from lxml import etree

REGISTRY_BASE_URL = 'https://registry.cdlib.org'
NUXEO_TOKEN = os.environ.get('NUXEO_TOKEN')
NUXEO_API = os.environ.get('NUXEO_API')
METADATA_STORE = os.environ.get('NUXEO_MERRITT_METADATA')
FEED_STORE = os.environ.get('NUXEO_FEEDS')

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

            storage = parse_data_uri(METADATA_STORE)
            metadata_path = os.path.join(storage.path, self.collection_id, self.version)
            filename = f"{'-'.join(page_prefix)}-p{page_index}.jsonl"
            jsonl = "\n".join([json.dumps(record) for record in documents])
            jsonl = f"{jsonl}\n"
            if storage.store == 'file':
                write_object_to_local(metadata_path, filename, jsonl)
            elif storage.store == 's3':
                s3_key = f"{metadata_path.lstrip('/')}/{filename}"
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

            storage = parse_data_uri(METADATA_STORE)
            metadata_path = os.path.join(storage.path, self.collection_id, self.version, "children")
            filename = f"{record['uid']}-p{page_index}.jsonl"
            jsonl = "\n".join([json.dumps(record) for record in documents])
            jsonl = f"{jsonl}\n"
            if storage.store == 'file':
                write_object_to_local(metadata_path, filename, jsonl)
            elif storage.store == 's3':
                s3_key = f"{metadata_path.lstrip('/')}/{filename}"
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

def collection_has_updates(collection):
    data = parse_data_uri(METADATA_STORE)
    metadata_path = os.path.join(data.path, collection['collection_id'])
    if data.store == 'file':
        if os.path.exists(metadata_path):
            versions = [listing for listing in os.listdir(metadata_path)]
        else:
            versions = []
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        prefix = metadata_path.lstrip('/')
        pages = paginator.paginate(
            Bucket=data.bucket,
            Prefix=prefix
        )
        for page in pages:
            versions = [item['Key'] for item in page['Contents']]
    else:
        raise Exception(f"Unknown data scheme: {data.store}")

    has_updates = False
    if versions:
        versions.sort()
        latest_feed_version = versions[-1]
        latest_nuxeo_update = get_nuxeo_collection_latest_update_date(collection)
        if latest_feed_version < latest_nuxeo_update:
            has_updates = True
    else:
        has_updates = True

    return has_updates

def get_nuxeo_collection_latest_update_date(collection):
    query = (
            "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:ancestorId = '{collection['uid']}' AND "
            "ecm:isVersion = 0 AND "
            "ecm:isTrashed = 0 ORDER BY lastModified desc"
        )

    request = {
        'url': f"{NUXEO_API.rstrip('/')}/search/lang/NXQL/execute",
        'headers': nuxeo_request_headers,
        'params': {
            'pageSize': '1',
            'currentPageIndex': 0,
            'query': query
        }
    }

    try:
        response = requests.get(**request)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"{collection:<6}: error querying Nuxeo: {request}")
        raise(e)

    documents = [doc for doc in response.json().get('entries', [])]
    last_modified = documents[0]['lastModified']

    return last_modified

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

atom_namespace = "http://www.w3.org/2005/Atom"
dublin_core_namespace = "http://purl.org/dc/elements/1.1/"
nuxeo_namespace = "http://www.nuxeo.org/ecm/project/schemas/tingle-california-digita/ucldc_schema"

def create_atom_feed(version, collection):
    opensearch_namespace = "http://a9.com/-/spec/opensearch/1.1/"
    namespace_map = {
            None: atom_namespace,
            "nx": nuxeo_namespace,
            "dc": dublin_core_namespace,
            "opensearch": opensearch_namespace}
    
    # create XML root
    root = etree.Element(etree.QName(atom_namespace, "feed"), nsmap=namespace_map)

    # add entry for each digital object
    documents = []
    data = parse_data_uri(METADATA_STORE)
    metadata_path = os.path.join(data.path, collection['collection_id'], version)
    if data.store == 'file':
        for file in os.listdir(metadata_path):
            fullpath = os.path.join(metadata_path, file)
            if os.path.isfile(fullpath):
                with open(fullpath, "r") as f:
                    #documents.extend(f.readlines())
                    for line in f.readlines():
                        record = json.loads(line)
                        create_media_json(record)
                        entry = create_record_entry(record)
                        root.insert(0, entry)
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        prefix = metadata_path.lstrip('/')
        pages = paginator.paginate(
            Bucket=data.bucket,
            Prefix=prefix
        )
        for page in pages:
            for item in page['Contents']:
                if not item['Key'].startswith(f'{prefix}/children/'):
                    #print(f"getting s3 object: {item['Key']}")
                    response = s3_client.get_object(
                        Bucket=data.bucket,
                        Key=item['Key']
                    )
                    for line in response['Body'].iter_lines():
                        record = json.loads(line)
                        create_media_json(record)
                        entry = create_record_entry(record)
                        root.insert(0, entry)
    else:
        raise Exception(f"Unknown data scheme: {data.store}")
    
    # add header info
    merritt_id = etree.Element(etree.QName(atom_namespace, "merritt_collection_id"))
    merritt_id.text = collection['merritt_id']
    root.insert(0, merritt_id)

    feed = etree.ElementTree(root)
    feed_string = etree.tostring(feed, pretty_print=True, encoding='unicode')

    # write feed to storage
    storage = parse_data_uri(FEED_STORE)
    filepath = os.path.join(storage.path, collection['collection_id'], version)
    filename = f"ucldc_collection_{collection['collection_id']}.atom"
    if storage.store == 'file':
        write_object_to_local(filepath, filename, feed_string)
    elif storage.store == 's3':
        s3_key = f"{filepath.lstrip('/')}/{filename}"
        load_object_to_s3(storage.bucket, s3_key, feed_string)

def create_record_entry(record):
    # atom id (URI)
    entry = etree.Element(etree.QName(atom_namespace, "entry"))
    atom_id = etree.SubElement(entry, etree.QName(atom_namespace, "id"))
    parts = urlparse(NUXEO_API)
    atom_id.text = f"{parts.scheme}://{parts.netloc}/nuxeo/nxdoc/default/{record['uid']}/view_documents"

    # atom title
    atom_title = etree.SubElement(entry, etree.QName(atom_namespace, "title"))
    atom_title.text = record["title"]

    # atom updated
    # TODO
    # atom_updated = etree.SubElement(entry, etree.QName(atom_namespace, "updated"))
    # atom_updated.text = record['lastModified']

    # atom author
    atom_author = etree.SubElement(entry, etree.QName(atom_namespace, "author"))
    atom_author.text = "UC Libraries Digital Collection"

    # metadata file link
    #self._insert_full_md_link(entry, nxid)

    # media json link
    # if is_parent:
    #     self._insert_media_json_link(entry, nxid)

    # main content file link
    #self._insert_main_content_link(entry, nxid)

    # auxiliary file link(s)
    #self._insert_aux_links(entry, nxid)

    # dc creator
    for creator in [creator['name'] for creator in record['properties']['ucldc_schema:creator']]:
        dc_creator = etree.SubElement(entry, etree.QName(dublin_core_namespace, "creator"))
        dc_creator.text = creator
    
    # dc title
    dc_title = etree.SubElement(entry, etree.QName(dublin_core_namespace, "title"))
    dc_title.text = record['title']

    # dc date
    dates = [date['date'] for date in record['properties']['ucldc_schema:date']]
    if dates:
        dc_date = etree.SubElement(entry, etree.QName(dublin_core_namespace, "date"))
        dc_date.text = dates[0]

    # dc identifier (a.k.a. local identifier) - Nuxeo ID
    nuxeo_identifier = etree.SubElement(entry, etree.QName(dublin_core_namespace, "identifier"))
    nuxeo_identifier.text = record['uid']

    # UCLDC identifier (a.k.a. local identifier) - ucldc_schema:identifier -- this will be the ARK if we have it
    identifier = record['properties']['ucldc_schema:identifier']
    if identifier:
        ucldc_identifier = etree.SubElement(entry, etree.QName(nuxeo_namespace, "identifier"))
        ucldc_identifier.text = identifier

    # UCLDC collection identifier
    collection = record['properties']['ucldc_schema:collection']
    if collection:
        ucldc_collection_id = etree.SubElement(entry, etree.QName(nuxeo_namespace, "collection"))
        ucldc_collection_id.text = collection

    return entry

def create_media_json(record):
    pass

def main(params):
    # get list of collections for which to create ATOM feeds
    if params.all:
        collections = get_registry_merritt_collections()
    else:
        collections = [get_registry_collection(params.collection)]

    if params.version:
        version = params.version
        for collection in collections:
            collection['has_updates'] = True
    else:
        version = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        for collection in collections:
            collection['uid'] = get_nuxeo_uid_for_path(collection['nuxeo_path'])
            # check to see if any records have been added or updated since last run
            collection['has_updates'] = collection_has_updates(collection)
            if collection['has_updates']:
                # fetch fresh metadata from Nuxeo
                fetcher_payload = {
                    "collection_id": collection['collection_id'],
                    "path": collection['nuxeo_path'],
                    "uid": collection['uid'],
                    "version": version
                }
                fetcher = NuxeoMetadataFetcher(fetcher_payload)
                fetcher.fetch()

    for collection in collections:
        if collection['has_updates']:
            # create ATOM feed
            print(f"{collection['collection_id']:<6}: creating ATOM feed")
            create_atom_feed(version, collection)
        else:
            print(f"{collection['collection_id']:<6}: no updates")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create Nuxeo atom feed(s) for Merritt')
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help='Create all feeds', action='store_true')
    top_folder.add_argument('--collection', help='single collection ID')
    parser.add_argument('--version', help='Metadata version. If provided, metadata will be fetched from S3.')

    args = parser.parse_args()
    sys.exit(main(args))