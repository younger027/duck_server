FROM golang:1.22.3-bookworm as builder
WORKDIR /build
RUN apt update && apt install -y build-essential
COPY . .
RUN go build -o duckpg .

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /build/duckpg /app/
RUN mkdir /data
VOLUME /app
EXPOSE 5432 8123
CMD ["./duckpg", "--db_path", "/data/duckdb"]
