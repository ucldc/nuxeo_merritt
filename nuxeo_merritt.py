import argparse
from collections import namedtuple
import datetime
from dateutil.parser import parse as dateutil_parse
import json
import os
import shutil
import sys
from urllib.parse import quote, urlparse

import requests

import boto3
from lxml import etree

REGISTRY_BASE_URL = 'https://registry.cdlib.org'
NUXEO_TOKEN = os.environ.get('NUXEO_MERRITT_NUXEO_TOKEN')
NUXEO_API = os.environ.get('NUXEO_MERRITT_NUXEO_API')
METADATA_STORE = os.environ.get('NUXEO_MERRITT_METADATA')
MEDIA_JSON_STORE = os.environ.get('NUXEO_MERRITT_MEDIA_JSON')
FEED_STORE = os.environ.get('NUXEO_MERRITT_FEEDS')

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
    #print(f"Writing s3://{bucket}/{key}")
    try:
        s3_client.put_object(
            ACL='bucket-owner-full-control',
            Bucket=bucket,
            Key=key,
            Body=content)
    except Exception as e:
        print(f"ERROR loading to S3: {e}")

    return f"s3://{bucket}/{key}"

def write_object_to_local(dir, filename, content):
    if not os.path.exists(dir):
        os.makedirs(dir)

    fullpath = os.path.join(dir, filename)
    #print(f"Writing file://{fullpath}")
    with open(fullpath, "w") as f:
        f.write(content)

    return f"file://{fullpath}"

def store_parent_metadata_page(collection_id, version, page_prefix, page_index, records):
    filename = f"{'-'.join(page_prefix)}-p{page_index}.jsonl"
    jsonl = "\n".join([json.dumps(record) for record in records])
    jsonl = f"{jsonl}\n"
  
    storage = parse_data_uri(METADATA_STORE)
    metadata_path = os.path.join(storage.path, collection_id, version)
    if storage.store == 'file':
        write_object_to_local(metadata_path, filename, jsonl)
    elif storage.store == 's3':
        s3_key = f"{metadata_path.lstrip('/')}/{filename}"
        load_object_to_s3(storage.bucket, s3_key, jsonl)
    else:
        raise Exception(f"Unknown data scheme: {storage.store}")

def store_component_metadata_page(collection_id, version, parent_uid, page_index, records):
    filename = f"{parent_uid}-p{page_index}.jsonl"
    jsonl = "\n".join([json.dumps(record) for record in records])
    jsonl = f"{jsonl}\n"

    storage = parse_data_uri(METADATA_STORE)
    metadata_path = os.path.join(storage.path, collection_id, version, "children")
    if storage.store == 'file':
        write_object_to_local(metadata_path, filename, jsonl)
    elif storage.store == 's3':
        s3_key = f"{metadata_path.lstrip('/')}/{filename}"
        load_object_to_s3(storage.bucket, s3_key, jsonl)
    else:
        raise Exception(f"Unknown data scheme: {storage.store}")
    
def store_media_json(collection_id, media_json: dict):
    filename = f"{media_json['id']}-media.json"
    content = json.dumps(media_json)

    storage = parse_data_uri(MEDIA_JSON_STORE)
    media_json_path = os.path.join(storage.path, collection_id)
    if storage.store == 'file':
        write_object_to_local(media_json_path, filename, content)
        return f"file://{media_json_path}/{filename}"
    elif storage.store == 's3':
        s3_key = f"{media_json_path.lstrip('/')}/{filename}"
        load_object_to_s3(storage.bucket, s3_key, content)
        return f"https://{storage.bucket}.s3.us-west-2.amazonaws.com/{s3_key}"
    else:
        raise Exception(f"Unknown data scheme: {storage.store}")

def get_stored_metadata_versions(collection_id):
    data = parse_data_uri(METADATA_STORE)
    metadata_path = os.path.join(data.path, collection_id)
    if data.store == 'file':
        if os.path.exists(metadata_path):
            versions = [listing for listing in os.listdir(metadata_path)]
        else:
            versions = []
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        prefix = metadata_path.lstrip('/')
        prefix = f"{prefix}/"
        pages = paginator.paginate(
            Bucket=data.bucket,
            Prefix=prefix,
            Delimiter='/'
        )
        versions = []
        for page in pages:
            common_prefixes = page.get('CommonPrefixes', [])
            versions.extend([prefix['Prefix'].split('/')[-2] for prefix in common_prefixes])
    else:
        raise Exception(f"Unknown data scheme: {data.store}")

    return versions

def get_parent_metadata_records(collection_id, version):
    records = []
    data = parse_data_uri(METADATA_STORE)
    metadata_path = os.path.join(data.path, collection_id, version)
    if data.store == 'file':
        store_uri = f"file://{metadata_path}"
        for file in os.listdir(metadata_path):
            fullpath = os.path.join(metadata_path, file)
            if os.path.isfile(fullpath):
                with open(fullpath, "r") as f:
                    for line in f.readlines():
                        records.append(line)
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        prefix = metadata_path.lstrip('/')
        store_uri = f"s3://{data.bucket}/{prefix}"
        pages = paginator.paginate(
            Bucket=data.bucket,
            Prefix=prefix
        )
        for page in pages:
            for item in page.get('Contents', []):
                if not item['Key'].startswith(f'{prefix}/children/'):
                    #print(f"getting s3 object: {item['Key']}")
                    response = s3_client.get_object(
                        Bucket=data.bucket,
                        Key=item['Key']
                    )
                    for line in response['Body'].iter_lines(): 
                        records.append(line)
    else:
        raise Exception(f"Unknown data scheme: {data.store}")

    if not records:
        raise Exception(
            f"No metadata records found for collection {collection_id} "
            f"at {store_uri}"
        )

    return records

def get_component_metadata_records(collection_id, version, parent_uid):
    records = []
    data = parse_data_uri(METADATA_STORE)
    component_path = os.path.join(data.path, collection_id, version, "children")
    if data.store == 'file':
        if os.path.exists(component_path):
            for file in os.listdir(component_path):
                if file.startswith(parent_uid):
                    fullpath = os.path.join(component_path, file)
                    if os.path.isfile(fullpath):
                        with open(fullpath, "r") as f:
                            for line in f.readlines():
                                records.append(line)
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        prefix = component_path.lstrip('/')
        pages = paginator.paginate(
            Bucket=data.bucket,
            Prefix=prefix
        )
        for page in pages:
            for item in page.get('Contents', []):
                if item['Key'].startswith(f"{prefix}/{parent_uid}"):
                    #print(f"getting s3 object: {item['Key']}")
                    response = s3_client.get_object(
                        Bucket=data.bucket,
                        Key=item['Key']
                    )
                    for line in response['Body'].iter_lines():
                        records.append(line)
    else:
        raise Exception(f"Unknown data scheme: {data.store}")
    
    return records

def delete_old_metadata(collection_id):
    versions = get_stored_metadata_versions(collection_id)
    if len(versions) > 1:
        versions.sort()
        data = parse_data_uri(METADATA_STORE)
        for version in versions[:-1]:
            version_path = os.path.join(data.path, collection_id, version)
            if data.store == 'file':
                shutil.rmtree(version_path)
            elif data.store == 's3':
                s3_client = boto3.client('s3')
                paginator = s3_client.get_paginator('list_objects_v2')
                prefix = version_path.lstrip('/')
                pages = paginator.paginate(
                    Bucket=data.bucket,
                    Prefix=prefix
                )
                keys_to_delete = [item['Key'] for page in pages for item in page['Contents']]
                response = s3_client.delete_objects(
                    Bucket=data.bucket,
                    Delete={"Objects": [{"Key": key} for key in keys_to_delete]}
                )
                if "Deleted" in response:
                    print(f"Deleted s3://{data.bucket}/{prefix} ({len(response['Deleted'])} objects total)")
                if "Errors" in response:
                    raise Exception(
                        f"Error deleting objects with prefix `{prefix}` from bucket `{data.bucket}`\n"
                        f"Errors: {response['Errors']}"
                    )

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
            f"WHERE ecm:ancestorId = '{folder['uid']}' "
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

            # write page of parent metadata to storage
            store_parent_metadata_page(self.collection_id, self.version, page_prefix, page_index, documents)
            
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

            store_component_metadata_page(self.collection_id, self.version, record['uid'], page_index, documents)

            page_index += 1

    def get_page_of_components(self, record: dict, page_index: int):
        query = (
            "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:ancestorId = '{record['uid']}' AND "
            "ecm:isVersion = 0 AND "
            "ecm:isTrashed = 0 "
            "ORDER BY ecm:pos"
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

def get_stored_feed_uri(collection_id):
    data = parse_data_uri(FEED_STORE)
    feed_path = data.path
    filename = f"ucldc_collection_{collection_id}.atom"
    if data.store == 'file':
        fullpath = os.path.join(feed_path, filename)
        if os.path.exists(fullpath):
            feed_storage_uri = fullpath
        else:
            feed_storage_uri = None
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        prefix = feed_path.lstrip('/')
        key = f"{prefix}/{filename}"
        try:
            s3_client.get_object(
                Bucket = data.bucket,
                Key = key
            )
            return f"s3://{data.bucket}/{key}"
        except s3_client.exceptions.NoSuchKey:
            feed_storage_uri = None
    else:
        raise Exception(f"Unknown data scheme: {data.store}")

    return feed_storage_uri

def get_latest_metadata_version(collection):
    versions = get_stored_metadata_versions(collection['collection_id'])

    if versions:
        versions.sort()
        return versions[-1]
    else:
        return None

def metadata_needs_update(collection):
    latest_metadata_version = get_latest_metadata_version(collection)
    #print(f"Latest ATOM feed version: {latest_metadata_version}")
    latest_nuxeo_update = get_nuxeo_collection_latest_update_date(collection)
    #print(f"Latest Nuxeo update:      {latest_nuxeo_update}")

    if not latest_metadata_version:
        return True
    elif dateutil_parse(latest_metadata_version) < dateutil_parse(latest_nuxeo_update):
        return True
    else:
        return False

def get_nuxeo_collection_latest_update_date(collection):
    # the ORDER BY clause doesn't work
    query = (
            "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:ancestorId = '{collection['uid']}' AND "
            "ecm:isVersion = 0 AND "
            "ecm:isTrashed = 0 "
            "ORDER BY lastModified desc"
        )

    request = {
        'url': f"{NUXEO_API.rstrip('/')}/search/lang/NXQL/execute",
        'headers': nuxeo_request_headers,
        'params': {
            'query': query
        }
    }

    try:
        response = requests.get(**request)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"{collection:<6}: error querying Nuxeo: {request}")
        raise(e)

    # the ORDER BY clause on the query doesn't work, so we have to sort ourselves
    last_modified_dates = [doc['lastModified'] for doc in response.json().get('entries', [])]
    last_modified_dates.sort()

    return last_modified_dates[-1]

def get_registry_merritt_collections():
    url = f'{REGISTRY_BASE_URL}/api/v1/collection/?harvest_type=NUX&format=json'

    merritt_collections = []
    while True:
        response = requests.get(url)
        response.raise_for_status()
        response = response.json()
        for collection in response['objects']:
            if collection['merritt_extra_data'] and collection['merritt_id']:
                merritt_collections.append(get_collection_data(collection))

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
        return get_collection_data(response)
    else:
        raise Exception(f"Collection `{collection_id}` is not set up for Merritt.")

def get_collection_data(registry_collection):
    collection_id = registry_collection['resource_uri'].split('/')[-2]
    return {
        'collection_id': collection_id,
        'nuxeo_path': registry_collection['merritt_extra_data'],
        'merritt_id': registry_collection['merritt_id'],
        'name': registry_collection['name'],
        'url': os.path.join(
            NUXEO_API,
            registry_collection['merritt_extra_data']
        )
    }

def get_nuxeo_uid_for_path(path):
    escaped_path = quote(path, safe=' /')
    url = u'/'.join([NUXEO_API, "path", escaped_path.strip('/')])
    headers = nuxeo_request_headers
    request = {'url': url, 'headers': headers}
    response = requests.get(**request)
    response.raise_for_status()
    return response.json()['uid']

atom_namespace = "http://www.w3.org/2005/Atom"
dublin_core_namespace = "http://purl.org/dc/elements/1.1/"
nuxeo_namespace = "http://www.nuxeo.org/ecm/project/schemas/tingle-california-digita/ucldc_schema"
opensearch_namespace = "http://a9.com/-/spec/opensearch/1.1/"

def create_atom_feed(version, collection):
    feed_filename = f"ucldc_collection_{collection['collection_id']}.atom"

    # create XML root
    namespace_map = {
        None: atom_namespace,
        "nx": nuxeo_namespace,
        "dc": dublin_core_namespace,
        "opensearch": opensearch_namespace
    }
    root = etree.Element(etree.QName(atom_namespace, "feed"), nsmap=namespace_map)

    # add entry and create media.json for each digital object
    for record in get_parent_metadata_records(collection['collection_id'], version):
        record = json.loads(record)
        entry = create_record_entry_and_media_json(record, collection, version)
        root.insert(0, entry)

    # merritt ID
    merritt_id = etree.Element(etree.QName(atom_namespace, "merritt_collection_id"))
    merritt_id.text = collection['merritt_id']
    root.insert(0, merritt_id)

    # paging info. this is just dumb for now.
    storage = parse_data_uri(FEED_STORE)
    s3_url = f"https://{storage.bucket}.s3.us-west-2.amazonaws.com{storage.path}/{feed_filename}"
    last_link = etree.Element(etree.QName(atom_namespace, "link"), rel="last", href=s3_url)
    root.insert(0, last_link)
    first_link = etree.Element(etree.QName(atom_namespace, "link"), rel="first", href=s3_url)
    root.insert(0, first_link)
    self_link = etree.Element(etree.QName(atom_namespace, "link"), rel="self", href=s3_url)
    root.insert(0, self_link)

    # collection alt link
    feed_link_alt = etree.Element(
        etree.QName(atom_namespace, "link"),
        rel="alternate",
        href=collection['url'],
        title=collection['name']
    )
    root.insert(0, feed_link_alt)

    # feed author
    feed_author = etree.Element(etree.QName(atom_namespace, "author"))
    feed_author.text = "UC Libraries Digital Collection"
    root.insert(0, feed_author)

    # feed title
    feed_title = etree.Element(etree.QName(atom_namespace, "title"))
    feed_title.text = "UCLDC Metadata Feed"
    root.insert(0, feed_title)

    # feed ID
    feed_id = etree.Element(etree.QName(atom_namespace, "id"))
    feed_id.text = s3_url
    root.insert(0, feed_id)

    # feed updated date/time
    feed_updated = etree.Element(etree.QName(atom_namespace, "updated"))
    feed_updated.text = version
    root.insert(0, feed_updated)

    # write feed to storage
    feed = etree.ElementTree(root)
    feed_string = etree.tostring(feed, pretty_print=True, encoding='unicode')
    filepath = storage.path
    if storage.store == 'file':
        feed_uri = write_object_to_local(filepath, feed_filename, feed_string)
    elif storage.store == 's3':
        s3_key = f"{filepath.lstrip('/')}/{feed_filename}"
        feed_uri = load_object_to_s3(storage.bucket, s3_key, feed_string)

    print(f"Collection {collection['collection_id']}: DONE. Feed URI: {feed_uri}")

def create_record_entry_and_media_json(record, collection, version):
    # create ATOM entry for parent object
    entry = etree.Element(etree.QName(atom_namespace, "entry"))
    entry = add_object_metadata_fields_to_entry(entry, record)
    entry = add_file_links_to_entry(entry, record)
    
    object_last_modified = record['lastModified']

    # get components and the date they were last updated
    media_json_struct_map = []
    nuxeo_uri_parts = urlparse(NUXEO_API)
    components = get_component_metadata_records(collection['collection_id'], version, record['uid'])
    for component in components:
        component = json.loads(component)
        add_file_links_to_entry(entry, component)
        if dateutil_parse(component['lastModified']) > dateutil_parse(record['lastModified']):
            object_last_modified = component['lastModified']

        media_json_struct_map.append({
            'id': component['uid'],
            'href': f"{nuxeo_uri_parts.scheme}://{nuxeo_uri_parts.netloc}/nuxeo/nxdoc/default/{component['uid']}/view_documents",
            'title': component['title']
        })

    # object last updated
    # if complex, we want this to be the lastModified of complex object as a whole
    atom_updated = etree.SubElement(entry, etree.QName(atom_namespace, "updated"))
    atom_updated.text = object_last_modified

    # create media json
    media_json = {
        'id': record['uid'],
        'href': f"{nuxeo_uri_parts.scheme}://{nuxeo_uri_parts.netloc}/nuxeo/nxdoc/default/{record['uid']}/view_documents",
        'title': record['title']
    }
    if media_json_struct_map:
        media_json['structMap'] = media_json_struct_map
    media_json_uri = store_media_json(collection['collection_id'], media_json)

    # add media json link to entry
    etree.SubElement(
        entry,
        etree.QName(atom_namespace, "link"),
        rel="alternate",
        href=media_json_uri,
        type="application/json",
        title="Structural metadata for this object"
    )

    return entry

def add_object_metadata_fields_to_entry(entry, record):
    # atom id (URI)
    entry = etree.Element(etree.QName(atom_namespace, "entry"))
    atom_id = etree.SubElement(entry, etree.QName(atom_namespace, "id"))
    parts = urlparse(NUXEO_API)
    atom_id.text = f"{parts.scheme}://{parts.netloc}/Nuxeo/nxdoc/default/{record['uid']}/view_documents"

    # atom title
    atom_title = etree.SubElement(entry, etree.QName(atom_namespace, "title"))
    atom_title.text = record["title"]

    # atom author
    atom_author = etree.SubElement(entry, etree.QName(atom_namespace, "author"))
    atom_author.text = "UC Libraries Digital Collection"

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
        ucldc_collection_id.text = collection[0]

    return entry

def add_file_links_to_entry(entry, record):
    # metadata file link
    parts = urlparse(NUXEO_API)
    full_metadata_url = f"{parts.scheme}://{parts.netloc}/Merritt/{record['uid']}.xml"
    etree.SubElement(
        entry, 
        etree.QName(atom_namespace, "link"), 
        rel="alternate", 
        href=full_metadata_url, 
        type="application/xml", 
        title="Full metadata for this object from Nuxeo"
    )

    # main content file links
    try:
        file_content = record['properties']['file:content']
    except KeyError:
        raise KeyError("Nuxeo object metadata does not contain 'properties/file:content' element. Make sure 'X-NXDocumentProperties' header includes 'file'")

    if file_content:
        content_file_url = file_content.get('data')
        content_file_url = content_file_url.replace('/nuxeo/', '/Nuxeo/')
        checksum = file_content.get('digest')
        # TODO add content_type
        main_content_link_element = etree.SubElement(
            entry,
            etree.QName(atom_namespace, "link"),
            rel="alternate",
            href=content_file_url,
            title="Main content file"
        )

        if checksum:
            checksum_element = etree.SubElement(
                main_content_link_element,
                etree.QName(opensearch_namespace, "checksum"),
                algorithm="MD5"
            )
            checksum_element.text = checksum

    # auxiliary file links
    aux_files = []
    for attachment in record['properties'].get('files:files', []):
        af = {}
        attachment_file = attachment.get('file')
        if attachment_file:
            url = attachment_file.get('data')
            if url:
                af['url'] = url.replace('/nuxeo/', '/Nuxeo/')
                af['checksum'] = attachment_file.get('digest')
                aux_files.append(af)

    for extra_file in record['properties'].get('extra_files:file', []):
        af = {}
        extra_file_blob = extra_file.get('blob')
        if extra_file_blob:
            url = extra_file_blob.get('data')
            if url:
                af['url'] = url.replace('/nuxeo/', '/Nuxeo/')
                af['checksum'] = extra_file_blob.get('digest')
                aux_files.append(af)

    for af in aux_files:
        link_aux_file = etree.SubElement(
            entry,
            etree.QName(atom_namespace, "link"),
            rel="alternate",
            href=af['url'],
            title="Auxiliary file"
        )
        checksum_element = etree.SubElement(
            link_aux_file,
            etree.QName(opensearch_namespace, "checksum"),
            algorithm="MD5"
        )
        checksum_element.text = af['checksum']

    return entry

def main(params):
    # get list of collections for which to create ATOM feeds
    if params.all:
        collections = get_registry_merritt_collections()
    else:
        collections = [get_registry_collection(params.collection)]

    if params.version:
        version = params.version
        for collection in collections:
            collection['feed_needs_update'] = True
    else:
        version = datetime.datetime.now()
        version = version.replace(tzinfo=datetime.timezone.utc)
        version = version.isoformat()
        for collection in collections:
            collection['uid'] = get_nuxeo_uid_for_path(collection['nuxeo_path'])
            # check to see if any records have been added or updated 
            # since metadata was last fetched
            collection['metadata_needs_update'] = metadata_needs_update(collection)
            if collection['metadata_needs_update']:
                collection['feed_needs_update'] = True
                # fetch fresh metadata from Nuxeo
                print(f"{collection['collection_id']}: fetching metadata from Nuxeo at path {collection['nuxeo_path']}")
                fetcher_payload = {
                    "collection_id": collection['collection_id'],
                    "path": collection['nuxeo_path'],
                    "uid": collection['uid'],
                    "version": version
                }
                fetcher = NuxeoMetadataFetcher(fetcher_payload)
                fetcher.fetch()
            else:
                # If feed doesn't exist, create it using latest fetched metadata
                if get_stored_feed_uri(collection['collection_id']):
                    collection['feed_needs_update'] = False
                else:
                    collection['feed_needs_update'] = True
                    version = get_latest_metadata_version(collection)

    for collection in collections:
        if collection['feed_needs_update']:
            # create ATOM feed and media.json
            print(f"Collection {collection['collection_id']}: creating ATOM feed and media.json")
            create_atom_feed(version, collection)
            delete_old_metadata(collection['collection_id'])
        else:
            print(f"Collection {collection['collection_id']}: NO UPDATES")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create Nuxeo atom feed(s) for Merritt')
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help='Create all feeds', action='store_true')
    top_folder.add_argument('--collection', help='single collection ID')
    parser.add_argument('--version', help='Metadata version. If provided, metadata will be fetched from S3.')

    args = parser.parse_args()
    sys.exit(main(args))