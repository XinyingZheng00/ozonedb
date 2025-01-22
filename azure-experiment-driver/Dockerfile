# Stage 1: Build Go binary
FROM golang:1.22.1 as go-builder

WORKDIR /app/goairdb
COPY ../goairdb /app/goairdb/

RUN go mod download
RUN apt-get update && apt-get install -y protobuf-compiler
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go
RUN protoc --go_out=. txn.proto
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o goairdb

######################

# Stage 2: Build YCSB
FROM maven:3.8.5-openjdk-11 as maven-builder

WORKDIR /app/ycsb-t
COPY ../ycsb-t /app/ycsb-t

RUN mvn clean -pl com.yahoo.ycsb:pyairdb-binding,com.yahoo.ycsb:jdbc-binding -am install -DskipTests

######################

# Stage 3: Set up the final image
FROM ubuntu:20.04

# Install runtime dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jre-headless wget build-essential software-properties-common python3-pip postgresql-client python-is-python3 protobuf-compiler && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y python3.11 python3.11-venv python3.11-dev && \
    apt-get clean

RUN pip install psycopg2-binary

WORKDIR /app

COPY --from=go-builder /app/goairdb/goairdb /app/goairdb
COPY --from=maven-builder /app/ycsb-t /app/ycsb-t

COPY ./entrypoint.py /app/entrypoint.py