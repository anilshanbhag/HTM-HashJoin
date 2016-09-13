HTM Hash Join
=====================

Implements an adaptive multi-core hash join based on hardware transactional memory (HTM-Adaptive).

The join does > 2x better than a radix join (PRJ) on data with locality and with low-overhead switches to radix join in the absence of locality. 

![Hash Join Performance](/figs/perf.png)

For details, see paper [link](http://anilshanbhag.in/static/papers/HTM_imdm16.pdf)

Also contains implementations of:

* hash join using a global c++ 11 atomics-based hash table
* parallel radix join based hash join (PRJ)
* hash join using a global hash table which uses locks (NPJ)

Installation
----------

Note that running HTM hash join requires hardware that supports Hardware Transactional Memory via Intel TSX. Use this [tool](https://github.com/andikleen/tsx-tools) to check if your hardware has TSX.

Make sure you have Intel TBB downloaded.

```
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_MODULE_PATH=/path/to/FindTBB
make 
```

A binary `main` created is used to run all the different algorithms.

```
./main --algo <algo>
       --rSize <size of build side>
       --transactionSize <number of hash table inserts per transaction>
       --dataDistr <data distribution>
       --shuffleRange <shuffle window (only used if dataDistr == local_shuffle)>
       --numPartitions <number of partitions>

algo = htm, nocc, atomic
dataDistr = uniform, sorted, shuffle, local_shuffle
```

Contact
-------

In case of issues, file an issue or email: anils [at] mit [dot] edu
