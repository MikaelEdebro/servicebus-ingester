include .env
export

GOBIN := $(shell go env GOPATH)/bin
PATH := $(GOBIN):$(PATH)
SHELL := env PATH=$(PATH) /bin/sh

.PHONY: run build migrate migrate-down sqlc docker-build

run:
	go run ./cmd/ingester

build:
	go build -o bin/ingester ./cmd/ingester

migrate:
	goose -dir sql/migrations postgres "$(DATABASE_URL)" up

migrate-down:
	goose -dir sql/migrations postgres "$(DATABASE_URL)" down

sqlc:
	sqlc generate

ACR := acrvcedcsp.azurecr.io
IMAGE := $(ACR)/experiment/ingestion/go-ingester
TAG ?= latest

docker-build:
	docker build --platform linux/amd64 -t $(IMAGE):$(TAG) .

push: docker-build
	az acr login --name acrvcedcsp
	docker push $(IMAGE):$(TAG)
