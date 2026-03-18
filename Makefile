include .env
export

GOBIN := $(shell go env GOPATH)/bin

.PHONY: run build migrate migrate-down sqlc docker-build

run:
	go run ./cmd/ingester

build:
	go build -o bin/ingester ./cmd/ingester

migrate:
	$(GOBIN)/goose -dir sql/migrations postgres "$(DATABASE_URL)" up

migrate-down:
	$(GOBIN)/goose -dir sql/migrations postgres "$(DATABASE_URL)" down

sqlc:
	$(GOBIN)/sqlc generate

ACR := acrvcedcsp.azurecr.io
IMAGE := $(ACR)/experiment/ingestion/go-ingester
TAG ?= latest

docker-build:
	docker build --platform linux/amd64 -t $(IMAGE):$(TAG) .

push: docker-build
# 	az acr login --name acrvcedcsp
	docker push $(IMAGE):$(TAG)
