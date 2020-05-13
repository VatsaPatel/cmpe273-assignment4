from pickle_hash import hash_code_hex

class RHRing(object):
    def __init__(self, nodes):
        self.nodes = nodes

    #Return the node with highest computed weight
    def get_node(self, key):
        assert len(self.nodes) > 0
        lookup = {}
        for node in self.nodes:
            weight = self.compute_weight(node, key)
            lookup[weight]=node
        # print(weights)
        node = max(lookup, key=int) #select node of highest weight
        return self.nodes.index(lookup[node])

    #Return the weight
    def compute_weight(self, node, key):
        # a b values from research paper
        a = 1103515245
        b = 12345

        node = int(hash_code_hex(str(node).encode()), base=16) # Hashing both host & port
        hash_val = int(hash_code_hex(key.encode()), base=16) # Hashing key

        return (a * ((a * node + b) ^ hash_val) + b) % 4294967296 # 2^32
