docker-build:
	docker-compose up -d
	docker build \
		-t fdns-ms-indexing \
		--network=fdns-ms-indexing_default \
		--rm \
		--build-arg INDEXING_PORT=8084 \
		--build-arg INDEXING_ELASTIC_HOST=elastic \
		--build-arg INDEXING_ELASTIC_PORT=9200 \
		--build-arg INDEXING_ELASTIC_PROTOCOL=http \
		--build-arg OBJECT_URL=http://fdns-ms-object:8083 \
		--build-arg INDEXING_FLUENTD_HOST=fluentd \
		--build-arg INDEXING_FLUENTD_PORT=24224 \
		--build-arg INDEXING_PROXY_HOSTNAME= \
		--build-arg OAUTH2_ACCESS_TOKEN_URI= \
		--build-arg OAUTH2_PROTECTED_URIS= \
		--build-arg OAUTH2_CLIENT_ID= \
		--build-arg OAUTH2_CLIENT_SECRET= \
		--build-arg SSL_VERIFYING_DISABLE=false \
		.
	docker-compose down

docker-run: docker-start
docker-start:
	docker-compose up -d
	docker run -d \
		-p 8084:8084 \
		--network=fdns-ms-indexing_default  \
		--name=fdns-ms-indexing_main \
		fdns-ms-indexing

docker-stop:
	docker stop fdns-ms-indexing_main || true
	docker rm fdns-ms-indexing_main || true
	docker-compose down

docker-restart:
	make docker-stop 2>/dev/null || true
	make docker-start

sonarqube:
	docker-compose up -d
	docker run -d --name sonarqube -p 9000:9000 -p 9092:9092 sonarqube || true
	mvn -DOBJECT_URL=http://localhost:8083 clean test sonar:sonar
	docker-compose down