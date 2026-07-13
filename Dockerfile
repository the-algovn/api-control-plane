FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/api-control-plane ./cmd/api-control-plane \
 && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/demo-service ./cmd/demo-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api-control-plane /api-control-plane
COPY --from=build /out/demo-service /demo-service
ENTRYPOINT ["/api-control-plane"]
