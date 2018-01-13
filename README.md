# logstore
A distributed log storage database. Logstore is meant as a replacement for [Elasticsearch](https://github.com/elastic/elasticsearch) when storing logs. Elasticsearch is a distributed document store and search engine, and in my opinion really isn't optimized for storing log messages.

# Documentation

### Log Message Placement
Logs are placed on one of the servers using [Jump Consistent Hashing](https://arxiv.org/pdf/1406.2294v1.pdf). Currently rack-awareness is not supported.

### Messages
All log messages are JSON objects, and must be "flat"; they cannot contain nested JSON objects. Arrays as values are supported.

### API

_Coming Soon_