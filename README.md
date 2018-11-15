[![Build Status](https://travis-ci.org/CDCgov/fdns-ms-indexing.svg?branch=master)](https://travis-ci.org/CDCgov/fdns-ms-indexing)

# FDNS Indexing Microservice

This repository contains the indexing layer for the Data Lake. The indexing layer is also known as the navigation layer.

## Running locally

Carefully read the following instructions for information on how to build, run, and test this microservice in your local environment.

### Before you start

You will need to have the following software installed to run this microservice in your local environment:

- Docker, [Installation guides](https://docs.docker.com/install/)
- Docker Compose, [Installation guides](https://docs.docker.com/compose/install/)
- **Windows Users**: This project uses `Make`, please see [Cygwin](http://www.cygwin.com/) for running commands in this README

### Build

First, you'll need to build the image. You can build the image by running the following command:

```sh
make docker-build
```

### Run

Once the image has been built, you can run it with the following command:

```sh
make docker-run
```

### Test

To check if the microservice is running, open the following URL in your browser:

[http://127.0.0.1:8084/](http://127.0.0.1:8084/)

### Documentation

To access the Swagger documentation, open the following URL in your browser:

[http://127.0.0.1:8084/swagger-ui.html](http://127.0.0.1:8084/swagger-ui.html)

### Elasticsearch Configuration

You can configure the following environment variables using Docker or the Launch Configuration in [Spring Tool Suite](https://spring.io/tools):

- `INDEXING_ELASTIC_HOST`: This is the host for your Elasticsearch server
- `INDEXING_ELASTIC_PORT`: This is the port for your Elasticsearch server
- `INDEXING_ELASTIC_PROTOCOL`: Protocol for communicating with Elasticsearch, by default it's `http`

### Docker Compose

This microservice is designed to be used with other microservices. Please look at the [docker-compose](./docker-compose.yml) file for more information.

- `OBJECT_URL`: This is a configurable environment variable to point to where the Object Microservice is running.

### OAuth 2 Configuration

This microservice is configurable so that it can be secured via an OAuth 2 provider.

__Scopes__: This application uses the following scope: `indexing.*`

Please see the following environment variables for configuring with your OAuth 2 provider:

- `OAUTH2_ACCESS_TOKEN_URI`: This is the introspection URL of your provider, ex: `https://hydra:4444/oauth2/introspect`
- `OAUTH2_PROTECTED_URIS`: This is a path for which routes are to be restricted, ex: `/api/1.0/**`
- `OAUTH2_CLIENT_ID`: This is your OAuth 2 client id with the provider
- `OAUTH2_CLIENT_SECRET`: This is your OAuth 2 client secret with the provider
- `SSL_VERIFYING_DISABLE`: This is an option to disable SSL verification, you can disable this when testing locally but this should be set to `false` for all production systems

### Miscellaneous Configurations

Here are other various configurations and their purposes:

- `INDEXING_PORT`: This is a configurable port the application is set to run on
- `INDEXING_FLUENTD_HOST`: This is the host of your [Fluentd](https://www.fluentd.org/)
- `INDEXING_FLUENTD_PORT`: This is the port of your [Fluentd](https://www.fluentd.org/)
- `INDEXING_PROXY_HOSTNAME`: This is the hostname of your environment for use with Swagger UI, ex: `api.my.org`
  
## Public Domain

This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License

The repository utilizes code licensed under the terms of the Apache Software
License and therefore is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy

This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
Surveillance Platform [Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md)
and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/privacy.html](http://www.cdc.gov/privacy.html).

## Contributing

Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page are subject to the [Presidential Records Act](http://www.archives.gov/about/laws/presidential-records.html)
and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records

This repository is not a source of government records, but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).

## Notices

Please refer to [CDC's Template Repository](https://github.com/CDCgov/template)
for more information about [contributing to this repository](https://github.com/CDCgov/template/blob/master/CONTRIBUTING.md),
[public domain notices and disclaimers](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md),
and [code of conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
