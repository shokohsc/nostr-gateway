# Build stage: full toolchain, throwaway layers.
FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# CGO_ENABLED=0: the module has no cgo dependencies, so the binary is fully
# static and runs on the distroless base below, which has no libc.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway .

# Runtime stage: distroless = CA certs (HTTPS to OpenCode and the relays) and a
# non-root user, no shell and no package manager. The gateway is in-memory only,
# so a read-only root filesystem is enough.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/gateway /gateway
EXPOSE 8080
ENTRYPOINT ["/gateway"]
