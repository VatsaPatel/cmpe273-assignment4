import socket
from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT, serialize_DELETE

from CH_node_ring import CHRing

BUFFER_SIZE = 1024


class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()

    def put(self, key, payload):
        return self.send(payload)

    def get_request(self, key, payload):
        return self.send(payload)

    def delete(self, key, payload):
        return self.send(payload)


def process(udp_clients):
    ch_ring = CHRing(NODES, VirtualNodeFactor=4)
    hash_codes = set()

    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        server_index = ch_ring.get_node(key)
        response = udp_clients[server_index].put(key, data_bytes)
        hash_codes.add(response.decode())
        print(response)

    # GET all users.
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        server_index = ch_ring.get_node(key)
        response = udp_clients[server_index].get_request(hc, data_bytes)
        print(response)

    # Delete all Users
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_DELETE(hc)
        server_index = ch_ring.get_node(key)
        response = udp_clients[server_index].delete(key, data_bytes)
        print(response)

    # PUT all users with Replication
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        for server_index in ch_ring.get_node_with_replications(key, ReplicationFactor=2):
            response = udp_clients[server_index].put(key, data_bytes)
            hash_codes.add(response.decode())
            print(response)


if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    process(clients)
