// decision_participant.js
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const PROTO_PATH = __dirname + "/twopc.proto";

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition).twopc;

const votesStore = {}; // Store votes by transaction_id

// Implements GlobalDecision RPC (external)
function globalDecision(call, callback) {
  const req = call.request;
  console.log(
    `Phase Decision of Node ${nodeId} receives RPC GlobalDecision from Phase Decision of Node ${req.coordinator_id}`
  );
  // Forward the global decision to the local Python voting phase via ApplyDecision.
  const votingAddress = votingServiceAddress; // from argument
  const votingClient = new protoDescriptor.VotingService(
    votingAddress,
    grpc.credentials.createInsecure()
  );
  console.log(
    `Phase Decision of Node ${nodeId} sends RPC ApplyDecision to Phase Voting of Node ${nodeId}`
  );
  votingClient.ApplyDecision(req, (err, response) => {
    if (err) {
      console.error("Error calling ApplyDecision:", err);
      callback(err, null);
    } else {
      callback(null, {
        transaction_id: req.transaction_id,
        participant_id: nodeId,
        status: `Decision ${req.decision} applied`,
      });
    }
  });
}

// Implements ReceiveVote RPC (internal): called by the Python voting phase.
function receiveVote(call, callback) {
  const voteResponse = call.request;
  console.log(
    `Phase Decision of Node ${nodeId} receives RPC ReceiveVote from Phase Voting of Node ${voteResponse.participant_id}`
  );
  votesStore[voteResponse.transaction_id] = voteResponse;
  callback(null, { message: "Vote received and stored" });
}

// Implements GetVote RPC: allows the coordinator to retrieve the stored vote.
function getVote(call, callback) {
  const request = call.request;
  console.log(
    `Phase Decision of Node ${nodeId} receives RPC GetVote for transaction ${request.transaction_id}`
  );
  const voteResponse = votesStore[request.transaction_id];
  if (voteResponse) {
    callback(null, voteResponse);
  } else {
    callback(new Error("Vote not found"), null);
  }
}

const server = new grpc.Server();
server.addService(protoDescriptor.DecisionService.service, {
  GlobalDecision: globalDecision,
  ReceiveVote: receiveVote,
  GetVote: getVote,
});

// Command-line arguments
const argv = require("minimist")(process.argv.slice(2));
const nodeId = argv.node_id || "node1";
const port = argv.port || "50052"; // Port for DecisionService
const votingServiceAddress = argv.voting_address || "localhost:50051"; // Python VotingService address

server.bindAsync(
  `0.0.0.0:${port}`,
  grpc.ServerCredentials.createInsecure(),
  () => {
    server.start();
    console.log(
      `DecisionService server started at port ${port} for Node ${nodeId}`
    );
  }
);
