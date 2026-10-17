# Build the server. Two stages, so what ships is a static binary and nothing
# else - no toolchain, no module cache, no shell scripts that could drift from
# the flags cmd/server actually defines.
# A patch version, not a major line: govulncheck reports standard library
# advisories against the toolchain that built the binary, and the fix for those
# is to build with a newer one. Bump this when it reports something.
# --platform=$BUILDPLATFORM keeps the compiler native and cross-compiles with
# GOARCH instead of emulating an arm64 toolchain under QEMU, which is the
# difference between a two-minute multi-architecture build and a twenty-minute
# one. Go cross-compiles for free; there is no reason to pay for emulation.
FROM --platform=$BUILDPLATFORM golang:1.26.9-alpine AS build
WORKDIR /src

# Dependencies first, so editing Go files does not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

# The build this image contains. Passed in rather than derived, because the
# .git directory is deliberately not in the build context and a binary that
# cannot say which build it is cannot be correlated with an incident.
ARG VERSION=""
ARG TARGETOS
ARG TARGETARCH
# CGO off: pgx and coder/websocket are pure Go, and a static binary runs on
# scratch. TARGETOS/TARGETARCH come from buildx and are what makes one
# `docker build` produce an arm64 image on an amd64 runner.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /ycollab-server ./cmd/server

FROM alpine:3.21
# wget is here for the healthcheck and nothing else; the image would otherwise
# have no way to report readiness to compose.
RUN apk add --no-cache wget ca-certificates
COPY --from=build /ycollab-server /usr/local/bin/ycollab-server

ARG VERSION=""
LABEL org.opencontainers.image.title="ycollab" \
      org.opencontainers.image.description="A self-hostable, Yjs wire-compatible collaboration server" \
      org.opencontainers.image.source="https://github.com/mesutokul/ycollab" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

EXPOSE 8080
# Not root: the process needs no filesystem of its own.
USER nobody
ENTRYPOINT ["/usr/local/bin/ycollab-server"]
