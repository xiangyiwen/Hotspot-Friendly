Hotspot-Friendly
==============
Hotspot-Friendly is a multi-version optimistic concurrency control protocol, which delivers both high throughput and low abort rate. This protocol is especially designed for workloads that contain hotspots. 

The repository is built on an extension of DBx1000: https://github.com/ScarletGuo/Bamboo-Public. We have fixed several bugs inherited from the original DBx1000 implementation.


Test Environment
------------
We compare the performance of six supported protocols across diverse workloads in a server equipped with an Intel Xeon Gold 5218R CPU (20 physical cores @2.10GHz and 27.5 MiB LLC) and 96 GiB DDR4 DRAM, running Ubuntu 20.04.3 LTS. Each core supports two hardware threads, resulting in a total of 40 threads. Each experiment was repeated five times. The final results are the average of five experimental results. It is important to note that no other background processes should be running during the experiments. 

Test Results
------------
The experimental results are attached here (including the configurations for each experiments).


Build
------------

To build the system.

    cmake -DTBB_DIR=~/tbb/cmake
    cmake -DCMAKE_BUILD_TYPE=Release
    make -j
    
    
    
Run
------------

To run the system.

    ./rundb
    
    

Configuration
---------------

DBMS configurations can be changed in the config.h file. For the meaning of each configuration inherited from the original DBx1000, please refer to README file. 
Extra configuration parameters specific to Hotspot-Friendly include: 
```
    ABORT_OPTIMIZATION        : By defaul, it is true. If set false, it will disable the optimization for minimizing unnecessary 𝑙𝑎𝑡𝑐ℎ occupancy during a transaction’s abort procedure.
    VERSION_CHAIN_CONTROL     : By defaul, it is true. If set false, it will disable the optimization for reducing the probability of cascading aborts.
    DEADLOCK_DETECTION        : By defaul, it is true. If set false, it will disable the dead-dependency detection.
    HOTSPOT_FRIENDLY_TIMEOUT  : By defaul, it is false. If set true, it will enable the timeout mechanism. Specifically, a transaction will abort if it waits for depended transactions to commit during the validation phase and exceeds the ABORT_WAIT_TIME.
    ABORT_WAIT_TIME           : The waiting threshold for the timeout mechanism. (1 means 1ms)
    CC_ALG                    : Concurrency control protocols. Six protocols are supported (NO_WAIT WAIT_DIE TICTOC SILO HOTSPOT_FRIENDLY).
```

