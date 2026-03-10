# LithicDB

A high-performance, distributed Log-Structured Merge (LSM) Tree Key-Value store written in pure Go.

## Architecture
* **Storage Engine:** Memtable (Red-Black Tree), Write-Ahead Log (WAL), and SSTables.
* **Distribution:** Sharded via Consistent Hashing ring.
* **Communication:** gRPC