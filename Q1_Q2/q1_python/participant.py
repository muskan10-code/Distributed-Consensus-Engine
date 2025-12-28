# participant.py
import grpc
from concurrent import futures
import time
import random
import argparse

import twopc_pb2
import twopc_pb2_grpc

class VotingServicer(twopc_pb2_grpc.VotingServiceServicer):
    def __init__(self, node_id, decision_address):
        self.node_id = node_id
        self.decision_address = decision_address

    def Vote(self, request, context):
        # Server-side log for external RPC call
        print(f"Phase Voting of Node {self.node_id} receives RPC Vote from Phase Voting of Node {request.coordinator_id}")
        # Decide vote randomly for demonstration
        vote_decision = random.choice([twopc_pb2.COMMIT, twopc_pb2.ABORT])
        response = twopc_pb2.VoteResponse(transaction_id=request.transaction_id,
                                          vote=vote_decision,
                                          participant_id=self.node_id)
        # Forward vote to the local Node decision phase via the internal RPC.
        try:
            with grpc.insecure_channel(self.decision_address) as channel:
                stub = twopc_pb2_grpc.DecisionServiceStub(channel)
                print(f"Phase Voting of Node {self.node_id} sends RPC ReceiveVote to Phase Decision of Node {self.node_id}")
                ack = stub.ReceiveVote(response)
                print(f"Internal vote forwarding ack: {ack.message}")
        except Exception as e:
            print(f"Error forwarding vote to decision phase: {e}")
        return response

    def ApplyDecision(self, request, context):
        # Server-side log for internal RPC call (from Node decision phase)
        print(f"Phase Voting of Node {self.node_id} receives RPC ApplyDecision from Phase Decision of Node {self.node_id}")
        print(f"Transaction {request.transaction_id}: Final decision applied: " +
              ("COMMIT" if request.decision == twopc_pb2.COMMIT else "ABORT"))
        return twopc_pb2.DecisionResponseAck(message="Decision applied successfully")

def serve(node_id, port, decision_address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    twopc_pb2_grpc.add_VotingServiceServicer_to_server(VotingServicer(node_id, decision_address), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"VotingService server started at port {port} for Node {node_id}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--node_id', type=str, required=True, help="Node ID for this participant")
    parser.add_argument('--port', type=int, default=50051, help="Port to run VotingService")
    parser.add_argument('--decision_address', type=str, default='localhost:50052',
                        help="Address of local DecisionService (Node) for internal communication")
    args = parser.parse_args()
    serve(args.node_id, args.port, args.decision_address)
