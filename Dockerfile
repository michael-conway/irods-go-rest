FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
RUN go build -o /out/irods-go-rest ./cmd/irods-go-rest

FROM alpine:3.22 AS runtime

RUN addgroup -S app && adduser -S -G app app \
	&& apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=build /out/irods-go-rest /app/irods-go-rest

ENV IRODS_REST_ADDR=:8080

EXPOSE 8080

USER app

ENTRYPOINT ["/app/irods-go-rest"]
