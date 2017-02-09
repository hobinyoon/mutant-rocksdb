## Mutant: Multi-Temperature LSM Tree-Based Database Storage

The cost of serving LSM-tree based databases can be enormous due to the sheer volume of data. We propose Mutant that takes the best of fast, expensive storage devices and slow, less-expensive storage devices. By continuously monitoring accesses to SSTables migrating them into proper storage devices, Mutant reduces cost up to 78.89% without sacrificing latency.

This is a RocksDB-Mutant implementation.
