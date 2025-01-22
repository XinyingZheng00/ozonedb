# cd /users/Xinying/ozonedb/build && make clean-db && cd /users/Xinying/YCSB
# python3 /users/Xinying/YCSB/bin/ycsb load ozonedb -s -P /users/Xinying/ozonedb/bench/ycsb/workload/workloada/1KB/workloada_1000000 > /users/Xinying/ozonedb/bench/ycsb/ycsb2graph/ozonedb$j-workloadA-$i.result

python3 /users/Xinying/YCSB/bin/ycsb run ozonedb -s -P /users/Xinying/ozonedb/bench/ycsb/workload/workloada/1KB/workloada_10000 >> /users/Xinying/ozonedb/bench/ycsb/ycsb2graph/ozonedb1KB.result &
# python3 /users/Xinying/YCSB/bin/ycsb load ozonedb -s -P /users/Xinying/ozonedb/bench/ycsb/workload/workloada/1KB/workloada_10000 >> /users/Xinying/ozonedb/bench/ycsb/ycsb2graph/ozonedb1KB.result &
# python3 /users/Xinying/YCSB/bin/ycsb run ozonedb -s -P /users/Xinying/ozonedb/bench/ycsb/workload/workloada/1KB/workloada_10000 >> /users/Xinying/ozonedb/bench/ycsb/ycsb2graph/ozonedb1KB.result &
# python3 /users/Xinying/YCSB/bin/ycsb load ozonedb -s -P /users/Xinying/ozonedb/bench/ycsb/workload/workloada/1KB/workloada_10000 >> /users/Xinying/ozonedb/bench/ycsb/ycsb2graph/ozonedb1KB.result

