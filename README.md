# cmpe273-assignment4

## Consistent Hashing
- run client_CH_ConsistentHashing.py
- Support for replications
- Defauld VirtualNodeFactor=4, ReplicationFactor=2. Can be changed.

## Rendezvous Hashing
- run client_RH_RendezvousHashing.py
- Computing highest weight using formula: (a * ((a * node + b) ^ hash_val) + b) % 4294967296
