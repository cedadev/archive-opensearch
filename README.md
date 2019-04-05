# Prototype Opensearch Interface for the CEDA Archive


## Example Queries

Here are some example queries to get you started. All urls are appended onto the host name.


### Get top level description

/opensearch/description.xml

### Get CMIP5 collection

/opensearch/atom?title=CMIP5

### Get description document for collection

/opensearch/description.xml?collectionId=1

### Search CMIP5 collection 

/opensearch/atom?collectionId=1&model=CMCC-CM&startRecord=30