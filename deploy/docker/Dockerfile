# Reusable multi-stage build. One image contains every binary; Compose and
# Kubernetes select which one to run through the container command.
#
# Build from the repository root:
#   docker build -f deploy/docker/Dockerfile -t distributed-task-queue:dev .

# ---- build stage -----------------------------------------------------------
FROM golang:1.23-alpine AS build

# git is needed only if the module graph ever pulls a VCS dependency directly.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Copy the module files first so dependency download is cached independently of
# source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO is disabled so the binaries run on a scratch-like base image.
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags="-s -w" -o /out/scheduler ./cmd/scheduler && \
    go build -trimpath -ldflags="-s -w" -o /out/worker    ./cmd/worker && \
    go build -trimpath -ldflags="-s -w" -o /out/producer  ./cmd/producer && \
    go build -trimpath -ldflags="-s -w" -o /out/benchmark ./cmd/benchmark && \
    go build -trimpath -ldflags="-s -w" -o /out/tqctl     ./cmd/tqctl

# ---- runtime stage ---------------------------------------------------------
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S taskqueue && adduser -S -G taskqueue taskqueue

COPY --from=build /out/scheduler /usr/local/bin/scheduler
COPY --from=build /out/worker    /usr/local/bin/worker
COPY --from=build /out/producer  /usr/local/bin/producer
COPY --from=build /out/benchmark /usr/local/bin/benchmark
COPY --from=build /out/tqctl     /usr/local/bin/tqctl

# Configuration files ship with the image so a container can run without a
# mounted volume. Mount over /etc/taskqueue to override.
COPY configs/ /etc/taskqueue/

USER taskqueue
WORKDIR /home/taskqueue

ENV TQ_CONFIG=/etc/taskqueue/docker.yaml

# No default command: Compose and Kubernetes always name the binary explicitly,
# which keeps it obvious which role a container is playing.
ENTRYPOINT []
CMD ["scheduler"]
