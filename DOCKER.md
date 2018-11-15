# FDNS Indexing Microservice

Foundation Services (FDNS) Indexing Microservice is the navigation layer of the data lake.

## Getting Started

To get started with the FDNS Indexing Microservice you can use either `docker stack deploy` or `docker-compose`:

```yaml
version: '3.1'
services:
  fluentd:
    image: fluent/fluentd
    ports:
      - "24224:24224"
  mongo:
    image: mongo
    ports:
      - "27017:27017"
  fdns-ms-object:
    image: cdcgov/fdns-ms-object
    ports:
      - "8083:8083"
    depends_on:
      - fluentd
      - mongo
    environment:
      OBJECT_PORT: 8083
      OBJECT_MONGO_HOST: mongo
      OBJECT_MONGO_PORT: 27017
      OBJECT_FLUENTD_HOST: fluentd
      OBJECT_FLUENTD_PORT: 24224
  elastic:
    image: docker.elastic.co/elasticsearch/elasticsearch:5.3.0
    ports:
      - "9200:9200"
    environment:
      - http.host=0.0.0.0
      - transport.host=127.0.0.1
      - bootstrap.memory_lock=true
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - xpack.security.enabled=false
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
  fdns-ms-indexing:
    image: cdcgov/fdns-ms-indexing
    ports:
      - "8084:8084"
    depends_on:
      - fluentd
      - elastic
      - fdns-ms-object
    environment:
      INDEXING_PORT: 8084
      INDEXING_ELASTIC_HOST: elastic
      INDEXING_ELASTIC_PORT: 9200
      INDEXING_ELASTIC_PROTOCOL: http
      INDEXING_FLUENTD_HOST: fluentd
      INDEXING_FLUENTD_PORT: 24224
      OBJECT_URL: http://fdns-ms-object:8083
```

[![Try in PWD](https://raw.githubusercontent.com/play-with-docker/stacks/master/assets/images/button.png)](http://play-with-docker.com?stack=https://raw.githubusercontent.com/CDCgov/fdns-ms-indexing/master/stack.yml)

## Source Code

Please see [https://github.com/CDCgov/fdns-ms-indexing](https://github.com/CDCgov/fdns-ms-indexing) for the fdns-ms-indexing source repository.

## Public Domain

This repository constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105. This repository is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/). All contributions to this repository will be released under the CC0 dedication.

## License

The repository utilizes code licensed under the terms of the Apache Software License and therefore is licensed under ASL v2 or later.

The container image in this repository is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the Apache Software License for more details.

## Privacy

This repository contains only non-sensitive, publicly available data and information. All material and community participation is covered by the Surveillance Platform [Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md) and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/privacy.html](http://www.cdc.gov/privacy.html).

## Records

This repository is not a source of government records, but is a copy to increase collaboration and collaborative potential. All government records will be published through the [CDC web site](http://www.cdc.gov).