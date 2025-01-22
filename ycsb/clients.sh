REPEATS=3
for i in 10000 20000 30000 40000 50000
do
    cd /users/Xinying/ozonedb/build
    make clean-db
    cd /users/Xinying/YCSB
    for ((j=1; j<=REPEATS; j++))
    do
    rm -rf /users/Xinying/ozonedb/bench/ycsb/ycsb2graph/no_ozonedb100KB_$j-workloadF-$i.result
    done
    for ((j=1; j<=REPEATS; j++))
    do
        python3 /users/Xinying/YCSB/bin/ycsb load ozonedb -s -P /users/Xinying/ozonedb/bench/ycsb/workload/workloadf/100KB/workloadf_$i >> /users/Xinying/ozonedb/bench/ycsb/ycsb2graph/no_ozonedb100KB_$j-workloadF-$i.result &
    done
    wait

    for ((j=1; j<=REPEATS; j++))
    do
        python3 /users/Xinying/YCSB/bin/ycsb run ozonedb -s -P /users/Xinying/ozonedb/bench/ycsb/workload/workloadf/100KB/workloadf_$i >> /users/Xinying/ozonedb/bench/ycsb/ycsb2graph/no_ozonedb100KB_$j-workloadF-$i.result &    
    done
    wait
done