# Official MinIO images/binaries are no longer published (Docker Hub, quay.io, dl.min.io return 401/410),
# so build MinIO server + mc client from the open-source repos.
FROM golang:1.24-alpine AS build
RUN apk add --no-cache git
ENV CGO_ENABLED=0
RUN go install github.com/minio/minio@latest && go install github.com/minio/mc@latest

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=build /go/bin/minio /usr/local/bin/minio
COPY --from=build /go/bin/mc /usr/local/bin/mc
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
