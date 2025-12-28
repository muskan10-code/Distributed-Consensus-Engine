import grpc
import raft_pb2
import raft_pb2_grpc
import threading
import random
import time
from concurrent import futures

# Possible states
FOLLOWER, CANDIDATE, LEADER = 0, 1, 2

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, nodes):
        self.node_id = node_id
        self.nodes = nodes  # {node_id: address}
        self.state = FOLLOWER
        self.term = 0
        self.voted_for = None
        self.votes_received = 0

        # Q4-specific: store a list of log entries (term, index, operation)
        self.log_entries = []
        self.commit_index = 0  # track how many have been "committed"

        # Randomized election timeout: [1.5s, 3s]
        self.election_timeout = random.uniform(1.5, 3)
        self.heartbeat_timeout = 1.0  # 1 second
        self.last_heartbeat = time.time()

        print(f"[Node {self.node_id}] Started as FOLLOWER in term {self.term}. "
              f"Election timeout: {self.election_timeout:.2f}s")

        # Start timers in background threads
        threading.Thread(target=self.run_election_timer, daemon=True).start()
        threading.Thread(target=self.run_heartbeat_timer, daemon=True).start()

    # Server-side RPC methods
    def RequestVote(self, request, context):
        print(f"[Node {self.node_id}] runs RPC RequestVote called by Node {request.candidate_id} (Term {request.term})")

        if request.term > self.term:
            print(f"[Node {self.node_id}] Updating term from {self.term} to {request.term}; reverting to FOLLOWER.")
            self.term = request.term
            self.voted_for = None
            self.state = FOLLOWER

        vote_granted = False
        if (self.voted_for in [None, request.candidate_id]) and (request.term >= self.term):
            self.voted_for = request.candidate_id
            vote_granted = True
            self.last_heartbeat = time.time()
            print(f"[Node {self.node_id}] Grants vote to Node {request.candidate_id} for term {request.term}")
        else:
            print(f"[Node {self.node_id}] Rejects vote to Node {request.candidate_id} for term {request.term}")

        return raft_pb2.VoteResponse(term=self.term, vote_granted=vote_granted)

    def AppendEntries(self, request, context):
        print(f"[Node {self.node_id}] runs RPC AppendEntries called by Node {request.leader_id} (Term {request.term})")

        success = False
        if request.term >= self.term:
            if request.term > self.term:
                print(f"[Node {self.node_id}] Updating term from {self.term} to {request.term}; reverting to FOLLOWER.")
            self.state = FOLLOWER
            self.term = request.term
            success = True
            self.last_heartbeat = time.time()

            # Q4: Overwrite our log with the leader's log 
            # If your .proto has repeated LogEntry, we do something like:
            self.log_entries.clear()
            for entry in request.entries:
                # entry has: term, index, operation
                self.log_entries.append( (entry.term, entry.index, entry.operation) )

            # Update commit index
            if request.leader_commit > self.commit_index:
                self.commit_index = request.leader_commit

            print(f"[Node {self.node_id}] Heartbeat/log accepted from leader {request.leader_id} (Term {request.term}). "
                #   f"Log size = {len(self.log_entries)}, commitIndex={self.commit_index}"
                  )
        else:
            print(f"[Node {self.node_id}] Heartbeat rejected; leader's term {request.term} < my term {self.term}.")

        return raft_pb2.AppendResponse(term=self.term, success=success)

    def SendClientRequest(self, request, context):
 
        print(f"[Node {self.node_id}] runs RPC SendClientRequest called by a client. (Operation = {request.operation})")

        if self.state != LEADER:
            # Not leader => forward
            # For simplicity, we guess that 'self.voted_for' might be the leader, or you track a real 'leader_id'
            leader_id = self.voted_for
            if leader_id is None or leader_id == self.node_id:
                # We don't know who the leader is
                return raft_pb2.ClientResponse(success=False, message="No known leader to forward request.")
            leader_addr = self.nodes[leader_id]
            print(f"[Node {self.node_id}] Forwarding client request to leader {leader_id} at {leader_addr}")
            try:
                with grpc.insecure_channel(leader_addr) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    return stub.SendClientRequest(raft_pb2.ClientRequest(operation=request.operation))
            except Exception as e:
                return raft_pb2.ClientResponse(success=False, message=f"Forward failed: {e}")

        # If we are the leader, let's append the operation
        new_index = len(self.log_entries) + 1
        self.log_entries.append( (self.term, new_index, request.operation) )
        print(f"[Node {self.node_id}] (LEADER) appended operation '{request.operation}' to local log at index {new_index} (Term {self.term}).")

        # "replicate" to all other nodes
        self.replicate_log_to_all()

        # For simplicity, pretend it's committed immediately
        self.commit_index = new_index
        return raft_pb2.ClientResponse(success=True, message=f"Operation '{request.operation}' committed at index {new_index}")

    # Client-side calls
    def send_request_vote(self, peer_id, stub):
        print(f"[Node {self.node_id}] sends RPC RequestVote to Node {peer_id} (Term {self.term})")
        try:
            response = stub.RequestVote(
                raft_pb2.VoteRequest(term=self.term, candidate_id=self.node_id)
            )
            if response.vote_granted:
                self.votes_received += 1
                print(f"[Node {self.node_id}] Received VOTE from Node {peer_id}. "
                      f"Total votes: {self.votes_received}")
            else:
                print(f"[Node {self.node_id}] Vote denied by Node {peer_id}.")

            if response.term > self.term:
                print(f"[Node {self.node_id}] Higher term {response.term} from Node {peer_id}; stepping down.")
                self.state = FOLLOWER
                self.term = response.term

        except Exception as e:
            print(f"[Node {self.node_id}] Failed to send RequestVote to Node {peer_id}. Error: {e}")

    def send_append_entries(self, peer_id, stub):
        print(f"[Node {self.node_id}] sends RPC AppendEntries to Node {peer_id} (Term {self.term})")

        # Convert our log to proto entries
        proto_entries = []
        for (entry_term, entry_idx, operation) in self.log_entries:
            e = raft_pb2.LogEntry(term=entry_term, index=entry_idx, operation=operation)
            proto_entries.append(e)

        request = raft_pb2.AppendRequest(
            term=self.term,
            leader_id=self.node_id,
            entries=proto_entries,
            leader_commit=self.commit_index
        )

        try:
            response = stub.AppendEntries(request)
            if response.term > self.term:
                print(f"[Node {self.node_id}] Higher term {response.term} from Node {peer_id}; stepping down.")
                self.state = FOLLOWER
                self.term = response.term
        except Exception as e:
            print(f"[Node {self.node_id}] Failed to send AppendEntries to Node {peer_id}. Error: {e}")

    def replicate_log_to_all(self):

        for peer_id, address in self.nodes.items():
            if peer_id != self.node_id:
                with grpc.insecure_channel(address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    self.send_append_entries(peer_id, stub)

    # Timers
    def run_election_timer(self):
        while True:
            time.sleep(0.1)
            if self.state != LEADER and (time.time() - self.last_heartbeat) > self.election_timeout:
                print(f"[Node {self.node_id}] No heartbeat for {self.election_timeout:.2f}s; starting election.")
                self.start_election()

    def run_heartbeat_timer(self):
        while True:
            time.sleep(self.heartbeat_timeout)
            if self.state == LEADER:
                # send heartbeats (which also includes log)
                self.replicate_log_to_all()

    # Election
    def start_election(self):
        self.state = CANDIDATE
        self.term += 1
        self.voted_for = self.node_id
        self.votes_received = 1
        print(f"[Node {self.node_id}] Became CANDIDATE in term {self.term}, voted for self.")
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(1.5, 3)
        print(f"[Node {self.node_id}] New election timeout: {self.election_timeout:.2f}s")

        for peer_id, address in self.nodes.items():
            if peer_id != self.node_id:
                with grpc.insecure_channel(address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    self.send_request_vote(peer_id, stub)

        if self.votes_received > len(self.nodes) // 2:
            print(f"[Node {self.node_id}] Won election with {self.votes_received} votes; becoming LEADER for term {self.term}.")
            self.state = LEADER
        else:
            print(f"[Node {self.node_id}] Not enough votes ({self.votes_received}); reverting to FOLLOWER.")
            self.state = FOLLOWER


def serve(node_id, port, nodes):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RaftNode(node_id, nodes), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"[Node {node_id}] gRPC server started on port {port}.")
    server.wait_for_termination()


if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])

    # We combine Python (1..5) with Go (6..10) in same cluster
    nodes = {
        1: 'node1:50051',
        2: 'node2:50052',
        3: 'node3:50053',
        4: 'node4:50054',
        5: 'node5:50055',
        6: 'node6:50056',
        7: 'node7:50057',
        8: 'node8:50058',
        9: 'node9:50059',
        10:'node10:50060'
    }

    port = nodes[node_id].split(':')[1]
    serve(node_id, port, nodes)
