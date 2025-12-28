// decision_coordinator.js
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

const argv = require("minimist")(process.argv.slice(2));
const coordinatorId = argv.coordinator_id || "coordinatorNode";
const transactionId = argv.transaction_id || "tx123";
const participantList = argv.participants
  ? argv.participants.split(",")
  : ["localhost:50052"];

// Collect votes from all participant decision services.
function collectVotes(transaction_id, participantAddresses, callback) {
  let votes = [];
  let remaining = participantAddresses.length;
  participantAddresses.forEach((addr) => {
    const client = new protoDescriptor.DecisionService(
      addr,
      grpc.credentials.createInsecure()
    );
    const request = { transaction_id: transaction_id, participant_id: addr };
    console.log(
      `Phase Decision of Node ${coordinatorId} sends RPC GetVote to Phase Decision of Node ${addr}`
    );
    client.GetVote(request, (err, response) => {
      remaining--;
      if (err) {
        console.error(`Error getting vote from ${addr}:`, err.message);
        votes.push({ vote: 2 }); // Treat error as ABORT (enum value 2)
      } else {
        console.log(
          `Received vote from ${response.participant_id}: ${response.vote}`
        );
        votes.push(response);
      }
      if (remaining === 0) {
        callback(votes);
      }
    });
  });
}

// Send the global decision (using GlobalDecision RPC) to each participant.
function sendGlobalDecision(transaction_id, decision, participantAddresses) {
  participantAddresses.forEach((addr) => {
    const client = new protoDescriptor.DecisionService(
      addr,
      grpc.credentials.createInsecure()
    );
    const request = {
      transaction_id: transaction_id,
      decision: decision,
      coordinator_id: coordinatorId,
    };
    console.log(
      `Phase Decision of Node ${coordinatorId} sends RPC GlobalDecision to Phase Decision of Node ${addr}`
    );
    client.GlobalDecision(request, (err, response) => {
      if (err) {
        console.error(`Error sending global decision to ${addr}:`, err.message);
      } else {
        console.log(
          `Global decision applied at participant ${response.participant_id}: ${response.status}`
        );
      }
    });
  });
}

// Main coordinator logic: collect votes, decide, then broadcast decision.
collectVotes(transactionId, participantList, (votes) => {
  // Decide: if all votes are COMMIT (enum value 1) then global commit, else abort.
  let globalDecisionValue = 1; // 1 = COMMIT, 2 = ABORT
  for (let voteResp of votes) {
    if (voteResp.vote !== 1) {
      globalDecisionValue = 2;
      break;
    }
  }
  console.log(
    `Global decision is: ${globalDecisionValue === 1 ? "COMMIT" : "ABORT"}`
  );
  sendGlobalDecision(transactionId, globalDecisionValue, participantList);
});
