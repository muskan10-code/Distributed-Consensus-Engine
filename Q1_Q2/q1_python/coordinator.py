# coordinator.py
import grpc
import argparse
import time

import twopc_pb2
import twopc_pb2_grpc

def run(coordinator_id, participant_addresses, transaction_id):
    responses = []
    for participant in participant_addresses:
        try:
            channel = grpc.insecure_channel(participant)
            stub = twopc_pb2_grpc.VotingServiceStub(channel)
            # Client-side log for external RPC call
            print(f"Phase Voting of Node {coordinator_id} sends RPC Vote to Phase Voting of Node {participant}")
            response = stub.Vote(twopc_pb2.VoteRequest(transaction_id=transaction_id,
                                                        coordinator_id=coordinator_id))
            print(f"Received vote from participant {response.participant_id}: " +
                  ( "COMMIT" if response.vote == twopc_pb2.COMMIT else "ABORT" ))
            responses.append(response)
        except Exception as e:
            print(f"Error contacting participant at {participant}: {e}")
    print("Voting phase complete. Votes collected:")
    for r in responses:
        print(f"Participant {r.participant_id}: " +
              ("COMMIT" if r.vote == twopc_pb2.COMMIT else "ABORT"))
    # (The collected votes can then be forwarded to the decision phase coordinator.)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--coordinator_id', type=str, required=True, help="Coordinator ID")
    parser.add_argument('--participants', type=str, required=True,
                        help="Comma separated list of participant addresses (e.g., localhost:50051,localhost:50061)")
    parser.add_argument('--transaction_id', type=str, default="tx123", help="Transaction ID")
    args = parser.parse_args()
    participant_addresses = args.participants.split(',')
    run(args.coordinator_id, participant_addresses, args.transaction_id)
