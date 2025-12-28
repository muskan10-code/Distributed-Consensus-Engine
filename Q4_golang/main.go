package main

import (
    "context"
    "fmt"
    "log"
    "math/rand"
    "net"
    "os"
    "strconv"
	"strings"
    "sync"
    "time"

    "google.golang.org/grpc"

    pb "github.com/your_github_name/project2/Q4_golang/raft" 
    // ^ Adjust import path so it points to the compiled proto package.
    //   Or place your generated .pb.go files in the same folder. 
)

// ------------------------------------------------------
// We replicate the Python logs style exactly:
//   "Node <X> runs RPC <name> called by Node <Y>"
//   "Node <X> sends RPC <name> to Node <Y>"
// ------------------------------------------------------

// We'll keep the same raft states for simplicity:
const (
    FOLLOWER = iota
    CANDIDATE
    LEADER
)

type LogEntry struct {
    Term      int32
    Index     int32
    Operation string
}

type RaftNode struct {
    pb.UnimplementedRaftServer

    mu           sync.Mutex
    nodeID       int32
    nodes        map[int32]string
    state        int
    currentTerm  int32
    votedFor     *int32
    logEntries   []LogEntry
    commitIndex  int32
    lastApplied  int32
    leaderID     int32

    // heartbeat and election stuff
    electionTimeout  time.Duration
    heartbeatTimeout time.Duration
    lastHeartbeat    time.Time
}

// NewRaftNode constructor
func NewRaftNode(id int32, nodes map[int32]string) *RaftNode {
    rn := &RaftNode{
        nodeID:          id,
        nodes:           nodes,
        state:           FOLLOWER,
        currentTerm:     0,
        votedFor:        nil,
        logEntries:      make([]LogEntry, 0),
        commitIndex:     0,
        lastApplied:     0,
        leaderID:        0,
        electionTimeout: time.Duration(rand.Float64()*1.5+1.5) * time.Second, // [1.5s,3s]
        heartbeatTimeout: 1 * time.Second,
        lastHeartbeat:    time.Now(),
    }

    log.Printf("[Node %d] Started as FOLLOWER in term %d. Election timeout: %.2fs",
        rn.nodeID, rn.currentTerm, rn.electionTimeout.Seconds())

    go rn.runElectionTimer()
    go rn.runHeartbeatTimer()

    return rn
}

// RPC Implementations

func (rn *RaftNode) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
    // Server-side logging
    log.Printf("[Node %d] runs RPC RequestVote called by Node %d (Term %d)", rn.nodeID, req.CandidateId, req.Term)

    rn.mu.Lock()
    defer rn.mu.Unlock()

    if req.Term > rn.currentTerm {
        log.Printf("[Node %d] Updating term from %d to %d; revert to FOLLOWER.",
            rn.nodeID, rn.currentTerm, req.Term)
        rn.currentTerm = req.Term
        rn.votedFor = nil
        rn.state = FOLLOWER
        rn.leaderID = 0
    }

    voteGranted := false
    if (rn.votedFor == nil || *rn.votedFor == req.CandidateId) && req.Term >= rn.currentTerm {
        voteGranted = true
        rn.votedFor = &req.CandidateId
        rn.lastHeartbeat = time.Now() // reset
        log.Printf("[Node %d] Grants vote to Node %d for term %d",
            rn.nodeID, req.CandidateId, req.Term)
    } else {
        log.Printf("[Node %d] Rejects vote to Node %d for term %d",
            rn.nodeID, req.CandidateId, req.Term)
    }

    return &pb.VoteResponse{
        Term:        rn.currentTerm,
        VoteGranted: voteGranted,
    }, nil
}

func (rn *RaftNode) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
    // Server-side logging
    log.Printf("[Node %d] runs RPC AppendEntries called by Node %d (Term %d)", rn.nodeID, req.LeaderId, req.Term)

    rn.mu.Lock()
    defer rn.mu.Unlock()

    success := false
    if req.Term >= rn.currentTerm {
        // Accept leader
        if req.Term > rn.currentTerm {
            log.Printf("[Node %d] Updating term from %d to %d; revert to FOLLOWER.",
                rn.nodeID, rn.currentTerm, req.Term)
            rn.currentTerm = req.Term
            rn.state = FOLLOWER
            rn.votedFor = nil
        }
        rn.leaderID = req.LeaderId
        rn.lastHeartbeat = time.Now()
        success = true

        // Copy entire log from leader (simplified approach)
        rn.logEntries = nil
        for _, entry := range req.Entries {
            rn.logEntries = append(rn.logEntries, LogEntry{
                Term:      entry.Term,
                Index:     entry.Index,
                Operation: entry.Operation,
            })
        }

        // Update commit index
        if req.LeaderCommit > rn.commitIndex {
            rn.commitIndex = req.LeaderCommit
        }

        log.Printf("[Node %d] Heartbeat/log accepted from leader %d (Term %d). Log size = %d, commitIndex=%d",
            rn.nodeID, req.LeaderId, req.Term, len(rn.logEntries), rn.commitIndex)

        // Here we’d also “apply” any newly committed entries up to commitIndex
    } else {
        log.Printf("[Node %d] Heartbeat/log rejected; leader's term %d < my term %d.",
            rn.nodeID, req.Term, rn.currentTerm)
    }

    return &pb.AppendResponse{
        Term:    rn.currentTerm,
        Success: success,
    }, nil
}

func (rn *RaftNode) SendClientRequest(ctx context.Context, req *pb.ClientRequest) (*pb.ClientResponse, error) {
    // Server-side logging
    log.Printf("[Node %d] runs RPC SendClientRequest called by a client. (Operation=%s)",
        rn.nodeID, req.Operation)

    rn.mu.Lock()
    defer rn.mu.Unlock()

    if rn.state != LEADER {
        // If not leader, we forward to the leader. We know the leader ID from
        // the election or from the Python cluster if we discovered it earlier.
        if rn.leaderID == 0 {
            // No known leader
            return &pb.ClientResponse{
                Success: false,
                Message: fmt.Sprintf("Node %d is not leader and no leader known!", rn.nodeID),
            }, nil
        }
        leaderAddr := rn.nodes[rn.leaderID]
        // Client-side log
        log.Printf("[Node %d] sends RPC SendClientRequest to Node %d because that is the leader.",
            rn.nodeID, rn.leaderID)

        // Forward
        conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
        if err != nil {
            return &pb.ClientResponse{
                Success: false,
                Message: fmt.Sprintf("Failed to connect to leader %d at %s: %v", rn.leaderID, leaderAddr, err),
            }, nil
        }
        defer conn.Close()

        c := pb.NewRaftClient(conn)
        // “Called by Node X” must appear on the leader side logs.
        return c.SendClientRequest(ctx, req)
    }

    // If we are the leader, we append to our log
    newIndex := int32(len(rn.logEntries) + 1)
    newEntry := LogEntry{
        Term:      rn.currentTerm,
        Index:     newIndex,
        Operation: req.Operation,
    }
    rn.logEntries = append(rn.logEntries, newEntry)
    log.Printf("[Node %d] (LEADER) appended operation '%s' to local log at index %d",
        rn.nodeID, req.Operation, newIndex)

    // We won't commit immediately. We'll replicate the log to other nodes.
    // Then once majority ACK, we commit. For simplicity, let's replicate
    // the entire log each time. We do that in a heartbeat or directly here.

    rn.replicateLogToAll()

    // In a real raft, we’d wait for majority acks. For now we’ll just pretend it’s immediate.
    rn.commitIndex = newIndex
    // “execute” everything up to commitIndex

    return &pb.ClientResponse{
        Success: true,
        Message: fmt.Sprintf("Operation '%s' committed at index %d", req.Operation, newIndex),
    }, nil
}

// replicateLogToAll is a convenience method
func (rn *RaftNode) replicateLogToAll() {
    for peerID, address := range rn.nodes {
        if peerID == rn.nodeID {
            continue
        }
        go func(pid int32, addr string) {
            // Client-side log
            log.Printf("[Node %d] sends RPC AppendEntries to Node %d (Term %d)",
                rn.nodeID, pid, rn.currentTerm)

            conn, err := grpc.Dial(addr, grpc.WithInsecure())
            if err != nil {
                log.Printf("[Node %d] Failed to dial Node %d: %v", rn.nodeID, pid, err)
                return
            }
            defer conn.Close()

            client := pb.NewRaftClient(conn)

            // Convert local log to proto
            entries := make([]*pb.LogEntry, 0, len(rn.logEntries))
            for _, e := range rn.logEntries {
                entries = append(entries, &pb.LogEntry{
                    Term:      e.Term,
                    Index:     e.Index,
                    Operation: e.Operation,
                })
            }

            req := &pb.AppendRequest{
                Term:        rn.currentTerm,
                LeaderId:    rn.nodeID,
                Entries:     entries,
                LeaderCommit: rn.commitIndex,
            }

            resp, err := client.AppendEntries(context.Background(), req)
            if err != nil {
                log.Printf("[Node %d] AppendEntries to Node %d failed: %v", rn.nodeID, pid, err)
                return
            }
            // If term is higher, step down
            if resp.Term > rn.currentTerm {
                rn.mu.Lock()
                if resp.Term > rn.currentTerm {
                    log.Printf("[Node %d] Detected higher term %d from Node %d; stepping down.",
                        rn.nodeID, resp.Term, pid)
                    rn.currentTerm = resp.Term
                    rn.state = FOLLOWER
                    rn.leaderID = 0
                }
                rn.mu.Unlock()
            }
        }(peerID, address)
    }
}

// Timers
func (rn *RaftNode) runElectionTimer() {
    for {
        time.Sleep(100 * time.Millisecond)
        rn.mu.Lock()
        if rn.state != LEADER && time.Since(rn.lastHeartbeat) > rn.electionTimeout {
            // we will not do full elections here as we rely on the Python cluster.
            log.Printf("[Node %d] No heartbeat for %.2fs; but Q4 might rely on Python's leader. Not re-electing.",
                rn.nodeID, rn.electionTimeout.Seconds())
        }
        rn.mu.Unlock()
    }
}

func (rn *RaftNode) runHeartbeatTimer() {
    for {
        time.Sleep(rn.heartbeatTimeout)
        rn.mu.Lock()
        if rn.state == LEADER {
            // replicate log
            rn.replicateLogToAll()
        }
        rn.mu.Unlock()
    }
}

// main & server
func main() {
    if len(os.Args) < 2 {
        log.Fatal("Usage: ./main <node_id>")
    }
    nodeID, err := strconv.Atoi(os.Args[1])
    if err != nil {
        log.Fatalf("Invalid node ID: %v", err)
    }

    // Example: 5 Go nodes, with distinct ports
    nodes := map[int32]string{
        6: "node6:50056",
        7: "node7:50057",
        8: "node8:50058",
        9: "node9:50059",
        10: "node10:50060",
    }

    port := ""
    if addr, ok := nodes[int32(nodeID)]; ok {
        // parse port from "node6:50056"
        port = addr[strings.Index(addr, ":")+1:]
    } else {
        log.Fatalf("No matching entry in nodes map for node ID %d", nodeID)
    }

    // Initialize gRPC server
    lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
    if err != nil {
        log.Fatalf("Failed to listen on port %s: %v", port, err)
    }

    grpcServer := grpc.NewServer()
    raftNode := NewRaftNode(int32(nodeID), nodes)

    pb.RegisterRaftServer(grpcServer, raftNode)
    log.Printf("[Node %d] gRPC server started. Listening on port %s", nodeID, port)

    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve gRPC server: %v", err)
    }
}
