from server_config import NODES
from pickle_hash import hash_code_hex
from bisect import bisect_right

class CHRing():

    def __init__(self, nodes, VirtualNodeFactor=4):
        assert len(nodes) > 0
        self.nodes = nodes
        self.virtualNodeFactor = VirtualNodeFactor
        self.totalNodes = len(self.nodes) * VirtualNodeFactor # Total No. of Nodes
        self.max = 4294967296  # 2^32 value
        self.chunk = int(self.max / self.totalNodes) # Size of node within the whole ring

        self.ring = []   # Array containing division points
        self.lookup = {}  # Contain mapping of virtual node to physical node

        self._createRing()

    def _createRing(self):
        for i in range(0, self.max, self.chunk):
            self.ring.append(i) # Division points for each point in ring

        # Map virtual nodes to their physical counterpart
        z = 0
        for i in range(self.virtualNodeFactor):
            for j in range(len(self.nodes)):
                # print(f"server:{j} is {self.ring[z]}")
                self.lookup[self.ring[z]] = j  # Add in lookup dict
                z += 1



    def get_node(self, key_hex):
        keyInRing = int(hash_code_hex(key_hex.encode()), 16) % self.max

        # Bisect the Ring. If keyInRing value higher than n-1 point then return node 0.
        # Hence map the node in clockwise direction.
        d = 0
        if self.ring[self.totalNodes - 1] > keyInRing:
            d = bisect_right(self.ring, keyInRing)

        print(self.lookup)
        print(keyInRing)
        return self.lookup[self.ring[d]]

    def get_node_with_replications(self, key_hex, ReplicationFactor=1):
        keyInRing = int(key_hex.encode(), 16) % self.max
        # Bisect the Ring. If keyInRing value higher than n-1 point then return node 0.
        # Hence map the node in clockwise direction.
        d = 0
        if self.ring[self.totalNodes - 1] > keyInRing:
            d = bisect_right(self.ring, keyInRing)

        if ReplicationFactor< len(self.nodes):
            server = self.lookup[self.ring[d]]
            for i in range(0,ReplicationFactor+1):
                yield (server+i)%len(self.nodes)
        else:
            print("Replication factor more than No. of nodes")


if __name__ == '__main__':
    ring = CHRing(nodes=NODES,VirtualNodeFactor=2)
    node = ring.get_node('e621944d55b827774fa6f9813ddeacd9')
    print(node)


    # for i in ring.get_node_with_replications('ed9440c442632621b608521b3f2650b8'):
    #     print(i)
