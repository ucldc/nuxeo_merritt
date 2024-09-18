# nuxeo_merritt

Code for pushing Nuxeo content into Merritt.

----------------

##Interpreting the results of deposits from Nuxeo to Merritt

Each object deposited in Merritt contains the following key files:


<b>1) mrt-erc.txt</b><br>
Merritt metadata record for the object.


<b>2) XML files</b><br>
Nuxeo metadata record. For simple objects, there will be a single metadata record. For complex objects, there will be a metadata record for the parent-level component; there will also be metadata records for each child-level component, if applicable.


<b>3) Content files (TIFF, etc.)</b><br>
Main content file and any auxiliary files, imported into Nuxeo.  For complex objects, there will be content files for the parent-level and child-level components.


<b>4) JSON file (media.json)</b><br>
JSON file reflects the structure of the object. For complex objects, the JSON will indicate the association between the  parent-level component metadata record and main content file; and likewise, the association between child-level component metadata record and main content file.


The JSON file can be used to interpret the complex object structure. In the JSON for the example object listed below, the `"id":` indicates the XML file (Nuxeo metadata record) and the `"href":` indicates the main content file (a TIFF).

[json example simplified]
```json
{
  "label": "Here comes the band = Ya viene la banda",
  "href": "https://nuxeo.cdlib.org/nuxeo/nxbigfile/default/5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106/file:content/gen_n7433_4m648h47_001.TIF",
  "id": "5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106",
  "structMap": [
    { 
      "href": "https://nuxeo.cdlib.org/nuxeo/nxbigfile/default/ddd02f15-5fee-44a1-9f99-1bcda6e36436/file:content/gen_n7433_4m648h47_002.TIF",
      "label": "General view",
      "id": "ddd02f15-5fee-44a1-9f99-1bcda6e36436"
    },
    {
      "href": "https://nuxeo.cdlib.org/nuxeo/nxbigfile/default/dda4ca90-c5b7-4ec4-9fcd-3b2e394ef050/file:content/gen_n7433_4m648h47_003.TIF",
      "label": "Detail view",
      "id": "dda4ca90-c5b7-4ec4-9fcd-3b2e394ef050"
    }
  ]
}
```

The individual XML files comprise the complete metadata records.  Note that the XML files also reflect their associated content files, in the `<schema name="file">` and `<schema name="extra_files">` entries.  In the example object, `<schema name="file">` points to the main content file (a TIFF) and `<schema name="extra_files">` points to the auxiliary files (DNG files).   


(Note that the `<picture:views>` entries for JPEG files can be ignored: the `<picture:views>` reference derivative images generated within Nuxeo for display in that context; those files are not deposited into Merritt).



## Example object deposited from Nuxeo to Merritt

<b>Morales, Gloria. Here comes the band = Ya viene la banda (c1999)</b><br>
<b>object primary identifier:</b>  ark:/99999/fk4mk6km9t<br>
<b>permanent link:</b>  http://merritt.cdlib.org/m/ark%3A%2F99999%2Ffk4mk6km8d/1<br>
<b>title:</b>  Here comes the band = Ya viene la banda<br>
<b>creator:</b>  Morales, Gloria<br>
<b>date:</b>  c1999<br>
<b>local id:</b>  5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106<br>
<b>version number:</b>  1<br>
<b>version date:</b>  2016-10-07 02:15 PM UTC<br>
<b>version size:</b>  281.9 MB<br>
<b>version files:</b>  19<br>

### User Files
<b>mrt-erc.txt</b>  text/plain 155 B<br> 
<b>5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106.xml</b>   application/xml 17.9 KB<br>
<b>dda4ca90-c5b7-4ec4-9fcd-3b2e394ef050.xml</b>   application/xml 12 KB<br> 
<b>ddd02f15-5fee-44a1-9f99-1bcda6e36436.xml</b>   application/xml 12 KB<br> 
<b>gen_n7433_4m648h47_001.dng</b>   image/tiff 50.7 MB<br> 
<b>gen_n7433_4m648h47_001.TIF</b>   image/tiff 44 MB<br> 
<b>gen_n7433_4m648h47_003.dng</b>   image/tiff 40.8 MB<br>
<b>gen_n7433_4m648h47_003.TIF</b>   image/tiff 37 MB<br> 
<b>gen_n7433_4m648h47_002.dng</b>   image/tiff 49.4 MB<br> 
<b>gen_n7433_4m648h47_002.TIF</b>   image/tiff 59.9 MB<br> 
<b>5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106-media.json</b>   application/json 1.6 KB<br> 

### System Files
<b>mrt-dc.xml</b>   application/xml 149 B<br> 
<b>mrt-erc.txt</b>   text/plain 158 B<br> 
<b>mrt-ingest.txt</b>   text/plain 1.6 KB<br> 
<b>mrt-membership.txt</b>   text/plain 20 B<br> 
<b>mrt-mom.txt</b>   text/plain 136 B<br> 
<b>mrt-object-map.ttl</b>  plain/turtle 5 KB<br> 
<b>mrt-owner.txt</b>   text/plain 19 B<br> 
<b>mrt-submission-manifest.txt</b>  text/plain 3.1 KB<br> 




