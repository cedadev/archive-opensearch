# Prototype Opensearch Interface for the CEDA Archive

## CEDA Dependencies
![Static Badge](https://img.shields.io/badge/elasticsearch%20v8-3BBEB1)
 - EO Data Bridge: No Error
 - Solr Host: No Error (using Elasticsearch backend) (Outdated with Solr)
 - Thredds: No Error (no direct dependency)
 - Vocab: Dependent

## Example Queries

Here are some example queries to get you started. All urls are appended onto the host name.

### Get top level description

/opensearch/description.xml


### Get description document for collection

/opensearch/description.xml?collectionId=1

### Search CMIP5 collection 

/opensearch/request?collectionId=1&model=CMCC-CM&startRecord=30&httpAccept=application/geo%2Bjson
