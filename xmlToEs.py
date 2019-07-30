#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from mw.xml_dump import Iterator
from elasticsearch import  Elasticsearch,helpers
from datetime import datetime
import os

os.system("php /home/xxxxxxx/www/mediawiki/maintenance/dumpBackup.php --current --filter=namespace:0,6 > /tmp/wiki_dump.xml")

#es = Elasticsearch(['ESurl:9200'], http_auth=("username:password"))

es = Elasticsearch(['ESurl:9200'], http_auth=("username:password"))
#time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

index = "cms_1"
mapping = '''
{
    "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas": "0"
    },
    "mappings": {
    "doc": {
      "dynamic": false,
      "properties": {
        "title": {
          "type": "text",
          "analyzer": "ik_max_word"
        },
        "source": {
          "type": "keyword"
        },
        "id": {
          "type": "keyword"
        },
        "url": {
          "type": "keyword"
        },
        "content": {
          "type": "text",
          "analyzer": "ik_max_word"
        },
         "createUser": {
          "type": "keyword"
        },
        "createTime": {
           "type": "date",
           "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ssZZZ||epoch_millis"
        },
        "updateUser": {
          "type": "keyword"
        },
        "updateTime": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ssZZZ||epoch_millis"
        }
      }
    }
  }
}'''
if not es.indices.exists(index):
    es.indices.create(index=index,body=mapping,ignore=400)
else:
    # query source = wiki,delete by query
    queryDoc = {'query': {'match': {'source': 'wiki'}}}
    deleteDoc = es.delete_by_query(index='cms', body=queryDoc)
    print("delete document!!!")

dumpfile = Iterator.from_file(open("/tmp/wiki_dump.xml"))
for page in dumpfile:
    # Iterate through a page's revisions
    for revision in page:
        wikiBody = {
            "id": "revision.id",
            "url": "https://wiki.xxx.cn/"+ page.title,
            "source": "wiki",
            "createUser": revision.contributor.user_text,
            "updateUser": revision.contributor.user_text,
            "content": revision.text,
            "title": page.title,
            "createTime": (datetime.fromtimestamp(revision.timestamp).strftime("%Y-%m-%d %H:%M:%S")),
            "updateTime": (datetime.fromtimestamp(revision.timestamp).strftime("%Y-%m-%d %H:%M:%S"))
         }

        createDoc = es.index(index=index,doc_type='doc', body = wikiBody)
print("done")
