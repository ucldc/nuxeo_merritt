#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
from lxml import etree
import collections
import pprint
pp = pprint.PrettyPrinter(indent=4)

parser = argparse.ArgumentParser(description='check for duplicates in feed')
parser.add_argument("path", help="filepath for feed")
argv = parser.parse_args()

filepath = argv.path

print(f"filepath: {filepath}")
tree = etree.parse(filepath)
root = tree.getroot()
feed = root.iterfind('{http://www.w3.org/2005/Atom}feed/')

ids = root.iter("{http://purl.org/dc/elements/1.1/}identifier")

count = 0
idlist = []
for identifier in ids:
   count = count + 1
   #print(identifier.text), count
   idlist.append(identifier.text)

dups = [item for item, count in collections.Counter(idlist).items() if count > 1]
pp.pprint(dups)
print(len(idlist))
print(len(dups))
