FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /ingester ./cmd/ingester

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ingester /ingester
COPY --from=build /src/sql/migrations /sql/migrations
ENTRYPOINT ["/ingester"]
