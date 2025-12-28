FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y curl unzip

RUN GRPCURL_VERSION=$(curl -s https://api.github.com/repos/fullstorydev/grpcurl/releases/latest | grep '"tag_name":' | sed -E 's/.*"v([^"]+)".*/\1/') && \
    curl -LO https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION}_linux_x86_64.tar.gz && \
    tar -xzf grpcurl_${GRPCURL_VERSION}_linux_x86_64.tar.gz && \
    mv grpcurl /usr/local/bin/ && \
    chmod +x /usr/local/bin/grpcurl && \
    rm grpcurl_${GRPCURL_VERSION}_linux_x86_64.tar.gz

COPY raft.proto /app/raft.proto
COPY raft_node.py /app/raft_node.py
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

EXPOSE 50051
CMD ["python", "raft_node.py", "1"]
