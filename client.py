import grpc
import replication_pb2 as pb
import replication_pb2_grpc as rpc
import sys

def main():
    ch = grpc.insecure_channel('localhost:50050')
    stub = rpc.ReplicationStub(ch)

    if sys.argv[1] == 'write':
        data = sys.argv[2]
        resp = stub.Write(pb.WriteRequest(data=data))
        print("Write:", resp.success, resp.message)

    elif sys.argv[1] == 'read':
        resp = stub.Read(pb.ReadRequest())
        for k,v in resp.data.items():
            print(k, "→", v)

    else:
        print("Usage: client.py write <msg> | read")

if __name__ == '__main__':
    main()
