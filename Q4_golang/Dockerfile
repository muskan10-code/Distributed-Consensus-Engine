FROM golang:1.20-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

RUN apk add --no-cache protobuf

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

COPY raft.proto main.go ./

RUN protoc -I. --go_out=. --go-grpc_out=. raft.proto

RUN go build -o raftnode main.go

EXPOSE 50056
CMD ["./raftnode", "6"]
