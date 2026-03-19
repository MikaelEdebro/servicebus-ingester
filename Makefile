include .env
export

GOBIN := $(shell go env GOPATH)/bin

.PHONY: run build migrate migrate-down sqlc docker-build

run:
	go run ./cmd/ingester

build:
	go build -o bin/ingester ./cmd/ingester

DATABASE_URL := postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_DATABASE)?sslmode=$(DB_SSL_MODE)

migrate:
	$(GOBIN)/goose -dir sql/migrations postgres "$(DATABASE_URL)" up

migrate-down:
	$(GOBIN)/goose -dir sql/migrations postgres "$(DATABASE_URL)" down

sqlc:
	$(GOBIN)/sqlc generate

ACR := acrvcedcsp.azurecr.io
IMAGE := $(ACR)/experiment/ingestion/servicebus-ingester-go
TAG := $(shell date +%Y%m%d.%H%M%S)

docker-build:
	docker build --platform linux/amd64 -t $(IMAGE):$(TAG) .

push: docker-build
	az acr login --name acrvcedcsp
	docker push $(IMAGE):$(TAG)
	@echo "\nPushed: $(IMAGE):$(TAG)"

deploy: push
	sed -i '' 's|image: $(IMAGE):.*|image: $(IMAGE):$(TAG)|' deploy/dev/deployment.yaml
	git add deploy/dev/deployment.yaml
	git commit -m "deploy: $(TAG)"
	git push
