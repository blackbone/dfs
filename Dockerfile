# Build stage
FROM golang:1.24.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags='-s -w' -o dfs ./cmd/dfs

# Final minimal image
FROM alpine:3.19
WORKDIR /app
COPY --from=build /src/dfs /usr/local/bin/dfs
EXPOSE 12000 13000
ENTRYPOINT ["/usr/local/bin/dfs"]
