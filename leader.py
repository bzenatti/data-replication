from concurrent import futures
import grpc
import replication_pb2 as pb
import replication_pb2_grpc as rpc
import json
import os

REPLICA_ADDRS = [
    'localhost:50051',
    'localhost:50052',
    'localhost:50053',
]
QUORUM = (len(REPLICA_ADDRS) // 2) + 1
LEADER_PORT = 50050

class LeaderServicer(rpc.ReplicationServicer):
    def __init__(self):
        self.epoch = 1
        self.log = []
        self.db = {}
        self.committed_offset = -1
        self.replicas = [rpc.ReplicationStub(grpc.insecure_channel(addr)) for addr in REPLICA_ADDRS]

        log_dir = "logs"
        db_dir = "dbs"
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(db_dir, exist_ok=True)

        self.log_file = os.path.join(log_dir, f"log_{LEADER_PORT}.json")
        self.db_file = os.path.join(db_dir, f"db_{LEADER_PORT}.json")
        self._load_state()

        print("Leader initialized")
        print(f"Initial Log:")
        print_log(self.log)
        print(f"Initial Committed Offset: {self.committed_offset}")
        print(f"Initial DB: {self.db}")

    def _load_state(self):
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as f:
                log_data = json.load(f)
                self.log = [pb.LogEntry(**entry) for entry in log_data]
        if os.path.exists(self.db_file):
            with open(self.db_file, 'r') as f:
                self.db = json.load(f)

    def _save_state(self,state):
        log_to_save = [
            {'epoch': entry.epoch, 'offset': entry.offset, 'data': entry.data}
            for entry in self.log
        ]
        if state == "log":
            with open(self.log_file, 'w') as f:
                json.dump(log_to_save, f, indent=4)
        else:
            with open(self.db_file, 'w') as f:
                json.dump(self.db, f, indent=4)

    def Write(self, request, context):
        print("\n--- Leader: Write method called ---")
        entry = pb.LogEntry(
            epoch=self.epoch,
            offset=len(self.log),
            data=request.data
        )
        self.log.append(entry)
        print_log(self.log)
        self._save_state("log") 

        acks = 0
        print("Leader: Pushing log entry to replicas...")
        for i, stub in enumerate(self.replicas):
            try:
                print(f"  - Replicating to replica on {REPLICA_ADDRS[i]}")
                resp = stub.ReplicateLog(pb.ReplicateRequest(
                    leader_epoch=self.epoch,
                    entry=entry,
                    prev_log_offset=entry.offset - 1,
                    prev_log_epoch=self.epoch
                ), timeout=2)
                if resp.ack:
                    acks += 1
                    print(f"  - ACK received from replica on {REPLICA_ADDRS[i]}")
                else:
                    print(f"  - Replica {REPLICA_ADDRS[i]} has inconsistent log. Syncing...")
                    if self.sync_replica(stub, resp.current_offset, i):
                        print(f"  - Sync successful for replica on {REPLICA_ADDRS[i]}")
                        acks += 1
                    else:
                        print(f"  - Sync failed for replica on {REPLICA_ADDRS[i]}")

            except grpc.RpcError as e:
                pass

        print(f"Leader: Total ACKs received: {acks}/{len(self.replicas)}")

        if acks < QUORUM:
            print("Leader: Failed to achieve quorum. Write unsuccessful.")
            return pb.WriteResponse(success=False, message="Failed to replicate to quorum")

        print("Leader: Quorum achieved. Sending commit order to replicas.")

        wantCommit = input("Type anything to commit or 'n' to not commit")

        if(wantCommit.lower() == 'n'):
            print("--- Leader: Didn`t commit the message ---")
            self.log.pop()
            return pb.WriteResponse(success=True, message=f"Not commited on replicas")

        self.committed_offset = entry.offset

        for log_entry in self.log: 
            key = f"{log_entry.epoch}:{log_entry.offset}"
            if key not in self.db and log_entry.offset <= self.committed_offset:
                self.db[key] = log_entry.data
        
        self._save_state("db") 

        for i, stub in enumerate(self.replicas):
            try:
                print(f"  - Sending commit to replica on {REPLICA_ADDRS[i]}")
                stub.CommitLog(pb.CommitRequest(
                    leader_epoch=self.epoch,
                    commit_offset=entry.offset
                ), timeout=20)
            except grpc.RpcError as e:
                print(f"  - RPC error during commit with replica on {REPLICA_ADDRS[i]}: {e.details()}")
                pass

        print("--- Leader: Write method finished ---")
        return pb.WriteResponse(success=True, message=f"Committed at offset {entry.offset}")

    def Read(self, request, context):
        print("\n--- Leader: Read method called ---")
        print(f"Leader: Returning committed data: {self.db}")
        print("--- Leader: Read method finished ---")

        return pb.ReadResponse(data=self.db)
    
    def sync_replica(self, stub, current_offset, replica_index):
        start_offset = current_offset + 1
        print(f"Leader: Syncing replica {REPLICA_ADDRS[replica_index]} from offset {start_offset}")

        for entry in self.log[start_offset:]:
            try:
                resp = stub.ReplicateLog(pb.ReplicateRequest(
                    leader_epoch=self.epoch,
                    entry=entry,
                    prev_log_offset=entry.offset - 1,
                    prev_log_epoch=self.epoch
                ), timeout=2)

                if not resp.ack:
                    print(f"  - Replica {REPLICA_ADDRS[replica_index]} still inconsistent at offset {resp.current_offset}")
                    return self.sync_replica(stub, resp.current_offset, replica_index)

            except grpc.RpcError as e:
                print(f"  - RPC error during sync with {REPLICA_ADDRS[replica_index]}: {e.details()}")
                return False

        return True

def print_log(log):
    if not log:
        print("Log: [empty]")
        return
    print("Log:")
    for entry in log:
        print(f"  - [Epoch: {entry.epoch} | Offset: {entry.offset} | Data: \"{entry.data}\"]")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpc.add_ReplicationServicer_to_server(LeaderServicer(), server)
    server.add_insecure_port(f'[::]:{LEADER_PORT}')
    server.start()
    print(f"Leader running on :{LEADER_PORT}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nLeader shutting down...")
        server.stop(0)

if __name__ == '__main__':
    serve()