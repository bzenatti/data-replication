import threading
from concurrent import futures

import grpc
import replication_pb2 as pb
import replication_pb2_grpc as rpc

class ReplicaServicer(rpc.ReplicationServicer):
    def __init__(self):
        self.epoch = 1
        self.log = []           
        self.db = {}            
        self.lock = threading.Lock()

    # Leader → Replica
    def ReplicateLog(self, request, context):
        with self.lock:
            prev_ok = (
                request.prev_log_offset == len(self.log)-1 and
                request.prev_log_epoch  == self.epoch
            )
            if not prev_ok:
                # conflict: truncate and report current state
                self.log = self.log[:request.prev_log_offset+1]
                return pb.ReplicateResponse(
                    ack=False,
                    current_offset=len(self.log)-1
                )
            # append intermediate
            self.log.append(request.entry)
            return pb.ReplicateResponse(
                ack=True,
                current_offset=request.entry.offset
            )

    def CommitLog(self, request, context):
        with self.lock:
            # commit up to commit_offset
            for entry in self.log:
                key = f"{entry.epoch}:{entry.offset}"
                if key not in self.db and entry.offset <= request.commit_offset:
                    self.db[key] = entry.data
            return pb.CommitResponse(success=True)    

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpc.add_ReplicationServicer_to_server(ReplicaServicer(), server)
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
    serve(sys.argv[1] if len(sys.argv)>1 else '50051')