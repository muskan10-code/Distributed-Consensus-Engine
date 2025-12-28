# Dockerfile for Node DecisionService (Q2)
FROM node:14
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 50052
CMD ["node", "decision_participant.js", "--node_id", "node1", "--port", "50052", "--voting_address", "localhost:50051"]
