bin/solr restart -c -p 8983   -a "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=20000"
bin/solr zk upconfig -n guess -z localhost:9983 -d  /Users/abhi/Downloads/solrcloud/cores/guessschemacore &&
bin/solr zk upconfig -n schemaless -z localhost:9983 -d schemaless &&
curl -v 'http://localhost:8983/solr/admin/collections?_=1493961385150&action=CREATE&collection.configName=guess&maxShardsPerNode=1&name=coll1&numShards=1&replicationFactor=1&router.name=compositeId&routerName=compositeId&wt=json'
