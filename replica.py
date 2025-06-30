import threading
from concurrent import futures
import grpc
import replication_pb2 as pb
import replication_pb2_grpc as rpc
import json
import os

class ReplicaServicer(rpc.ReplicationServicer):
    def __init__(self, port):
        self.epoch = 1
        self.log = []
        self.db = {}
        self.lock = threading.Lock()
        
        self.port = port
        log_dir = "logs"
        db_dir = "dbs"
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(db_dir, exist_ok=True)

        self.log_file = os.path.join(log_dir, f"log_{self.port}.json")
        self.db_file = os.path.join(db_dir, f"db_{self.port}.json")
        self._load_state()
        
        print(f"Replica on port {self.port} initialized")
        print(f"Initial Log: {self.log}")
        print(f"Initial DB: {self.db}")

    def _load_state(self):
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as f:
                log_data = json.load(f)
                self.log = [pb.LogEntry(**entry) for entry in log_data]
        if os.path.exists(self.db_file):
            with open(self.db_file, 'r') as f:
                self.db = json.load(f)

    def _save_state(self):
        log_to_save = [
            {'epoch': entry.epoch, 'offset': entry.offset, 'data': entry.data}
            for entry in self.log
        ]
        with open(self.log_file, 'w') as f:
            json.dump(log_to_save, f, indent=4)
        with open(self.db_file, 'w') as f:
            json.dump(self.db, f, indent=4)

    def ReplicateLog(self, request, context):
        with self.lock:
            print("\n--- Replica: ReplicateLog method called ---")
            print(f"Replica Log (before): ")
            print_log(self.log)
            print(f"Replica DB (before): {self.db}")
            
            prev_ok = (request.prev_log_offset == len(self.log)-1 and request.prev_log_epoch  == self.epoch)

            if not prev_ok:
                print("Replica: Inconsistent log detected. Truncating...")
                self.log = self.log[:request.prev_log_offset+1]
                print(f"Replica Log (after truncation): {self.log}")
                self._save_state() 
                return pb.ReplicateResponse(
                    ack=False,
                    current_offset=len(self.log)-1
                )
            print("Replica: Consistent log. Appending entry.")
            self.log.append(request.entry)
            self._save_state() 

            print(f"Replica Log (after append):")
            print_log(self.log)
            print(f"Replica DB (after append): {self.db}")
            print("--- Replica: ReplicateLog method finished ---")

            return pb.ReplicateResponse(
                ack=True,
                current_offset=request.entry.offset
            )

    def CommitLog(self, request, context):
        with self.lock:
            print("\n--- Replica: CommitLog method called ---")
            print(f"Replica Log (before commit):")
            print_log(self.log)
            print(f"Replica DB (before commit): {self.db}")

            wantCommit = input("Type anything to commit or 'n' to not commit")

            if(wantCommit.lower() == 'n'):
                print("--- Leader: Didn`t commit the message ---")
                
                return pb.CommitResponse(success=True)
                
            for entry in self.log:
                key = f"{entry.epoch}:{entry.offset}"
                if key not in self.db and entry.offset <= request.commit_offset:
                    self.db[key] = entry.data

            self._save_state() 

            print(f"Replica Log (after commit):")
            print_log(self.log)
            print(f"Replica DB (after commit): {self.db}")
            print("--- Replica: CommitLog method finished ---")
            return pb.CommitResponse(success=True)

def print_log(log):
    if not log:
        print("Log: [empty]")
        return
    print("Log:")
    for entry in log:
        print(f"  - [Epoch: {entry.epoch} | Offset: {entry.offset} | Data: \"{entry.data}\"]")


def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpc.add_ReplicationServicer_to_server(ReplicaServicer(port), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Replica running on :{port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nReplica shutting down...")
        server.stop(0)


if __name__ == '__main__':
    import sys
    port = sys.argv[1] if len(sys.argv) > 1 else '50051'
    serve(port)