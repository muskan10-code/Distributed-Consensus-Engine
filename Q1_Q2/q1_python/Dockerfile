# Dockerfile for Python VotingService (Q1)
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
# Generate gRPC code from proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. twopc.proto
EXPOSE 50051
CMD ["python", "participant.py", "--node_id", "participant1", "--port", "50051", "--decision_address", "localhost:50052"]
