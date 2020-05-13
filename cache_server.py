import sys
import socket

from server_config import NODES
from pickle_hash import deserialize, hash_code_hex, serialize_GET, serialize

BUFFER_SIZE = 1024

class MyDict(dict):
    def __init__(self): 
        self = dict() 
          
    def put(self, key, value):
        self[key] = value
        return key

    def get(self, key):
        if key in self:
            value = self[key]
            return value
        else:
            return 'not found'

    def delete(self, key):
        if key in self:
            if self.pop(key):
                return 'success'
        else:
            return 'error'

class UDPServer():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self.db = MyDict()


    def extract_request(self, data_bytes):
        request = deserialize(data_bytes)
        operation = request['operation']
        key = request['id']
        payload = None
        if 'payload' in request: 
            payload = request['payload']
        print(f'operation={operation}\nid={key}\npayload={payload}')
        response = self.handle_operation(operation, key, payload)
        return response


    def handle_operation(self, operation, key, value):
        if operation == 'GET':
            temp_data = self.db.get(key)
            yolo = serialize(temp_data)
            return yolo
        elif operation == 'PUT':
            return self.db.put(key, value)
        elif operation == 'DELETE':
            return self.db.delete(key)
        else:
            print(f'Error: Invalid operation={operation}')
            return 'Not supported operation={}'.format(operation)


    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((self.host, self.port))

        while True:
            data, ip = s.recvfrom(BUFFER_SIZE)
            print("{}:size={}".format(ip, len(data)))
            response = self.extract_request(data)
            # reply back to the client
            if isinstance(response, str):
                response = response.encode("utf-8")

            s.sendto(response, ip)


if __name__ == "__main__":
    # if len(sys.argv) < 2:
    #     print('Missing server index as argument. Usage: python3 client_CH_ConsistentHashing.py [0-{}]'.format(len(NODES)-1))
    #     sys.exit(2)

    node_index = int(sys.argv[1])
    # node_index = 0
    node = NODES[node_index]
    udpServer = UDPServer(node['host'], node['port'])
    print('Cache Server[{}] started at {}:{}'.format(node_index, node['host'], node['port']))
    udpServer.run()
