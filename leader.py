from concurrent import futures

import grpc
import replication_pb2 as pb
import replication_pb2_grpc as rpc

REPLICA_ADDRS = [
    'localhost:50051',
    'localhost:50052',
    'localhost:50053',
]
QUORUM = (len(REPLICA_ADDRS) // 2) + 1

class LeaderServicer(rpc.ReplicationServicer):
    def __init__(self):
        self.epoch = 1
        self.log = []  
        self.committed_offset = -1
        self.replicas = [rpc.ReplicationStub(grpc.insecure_channel(addr)) for addr in REPLICA_ADDRS]

    def Write(self, request, context):
        entry = pb.LogEntry(
            epoch=self.epoch,
            offset=len(self.log),
            data=request.data
        )
        self.log.append(entry)
        # Push to replicas
        acks = 0
        responses = []
        for stub in self.replicas:
            try:
                resp = stub.ReplicateLog(pb.ReplicateRequest(
                    leader_epoch=self.epoch,
                    entry=entry,
                    prev_log_offset=entry.offset - 1,
                    prev_log_epoch=self.epoch
                ), timeout=2)
                if resp.ack:
                    acks += 1
            except grpc.RpcError:
                pass

        # Wait for quorum
        if acks < QUORUM:
            return pb.WriteResponse(success=False, message="Failed to replicate to quorum")
        
        # Commit
        self.committed_offset = entry.offset
        for stub in self.replicas:
            try:
                stub.CommitLog(pb.CommitRequest(
                    leader_epoch=self.epoch,
                    commit_offset=entry.offset
                ), timeout=2)
            except grpc.RpcError:
                pass
        return pb.WriteResponse(success=True, message=f"Committed at offset {entry.offset}")

    def Read(self, request, context):
        out = {}
        for e in self.log[:self.committed_offset+1]:
            out[f"{e.epoch}:{e.offset}"] = e.data
        return pb.ReadResponse(data=out)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpc.add_ReplicationServicer_to_server(LeaderServicer(), server)
    server.add_insecure_port('[::]:50050')
    server.start()
    print("Leader running on :50050")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nLeader shutting down...")
        server.stop(0)

if __name__ == '__main__':
    serve()
