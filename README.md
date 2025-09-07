
# Set up and Running Lazylog system

 ## Prepare machines
 1. Reserve 6 nodes in cloudlab, the type should be either **c6525-25g** or **xl170**
	 1 for client node, running ycsb clients
	 3 for durability nodes and consensus nodes
	 2 for data shards
 2. Generate ssh keys and add keys to github
```
ssh-keygen -t rsa
curl -H "Authorization: token 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" \
-H "Accept: application/vnd.github+json" \
--data "{\"title\":\"key:$(hostname)\",\"key\":\"$(cat ~/.ssh/id_rsa.pub)\"}" \
https://api.github.com/user/keys
```
 3. install dependency for lazy log
 ```
 For all node:
sudo mkdir -p /sharedfs && cd /sharedfs && git clone git@github.com:XinyingZheng00/LazyLog-Artifact.git --recursive && cd LazyLog-Artifact/scripts && bash install_mlnx.sh
cd /sharedfs/LazyLog-Artifact/scripts
bash ./deps.sh && sudo xargs -a benchmark/requirements.txt apt install -y
```
4. Reboot all nodes in cloudlab dashboard
5. 
```
echo 2048 | sudo tee /proc/sys/vm/nr_hugepages
sudo mkdir -p /data && sudo chown -R Xinying /data
```
## Prepare code and compile
 1. Now we need to prepare all necessary codes, we will have /sharedfs as a shared nfd directory so that we can just prepare in one node and others can access it.
```
Set up nfs in server side:
sudo apt install nfs-kernel-server
sudo chown -R Xinying /sharedfs
echo "/sharedfs *(rw,sync,no_subtree_check)" | sudo tee -a /etc/exports
sudo exportfs -ra # reload exports
sudo exportfs -v # verify what’s being exported
```
```
Client side:
sudo apt install nfs-common && sudo mkdir -p /sharedfs && sudo mount 10.10.1.1:/sharedfs /sharedfs
Optional:
add this line to /etc/fstab:
10.10.1.1:/sharedfs /sharedfs nfs defaults 0 0
  ```
  
2. Now we start to prepare all necessary code directory just **in one node**:
```
cd /sharedfs
git clone https://github.com/erpc-io/eRPC && cd eRPC && git checkout 793b2a93591d372519983fe23ea4e438199f2462 && cmake . -DPERF=ON -DTRANSPORT=infiniband -DROCE=ON -DLOG_LEVEL=info -DCMAKE_POSITION_INDEPENDENT_CODE=ON && make -j$(nproc)
cd /sharedfs
git clone git@github.com:XinyingZheng00/LazyLog-Artifact.git --recursive
```
3. Change the index in the **LazyLog-Artifact/RDMA/src/infinity/core/Context.h** file constructor.
run the following command to get the index:
```
sudo /opt/mellanox/iproute2/sbin/rdma link
```
```
link mlx5_0/1 state ACTIVE physical_state LINK_UP netdev eno33
link mlx5_1/1 state DOWN physical_state DISABLED netdev eno34
link mlx5_2/1 state ACTIVE physical_state LINK_UP netdev enp65s0f0
link mlx5_3/1 state DOWN physical_state DISABLED netdev enp65s0f1
```
Here, for instance, we used mlx5_2/1 for our tests, which corresponds to index 2 in the entire list. (This is 0 indexed)
Therefore, the device id in Context would be 2 here.

4. We now start to compile lazylog system
```
cd LazyLog-Artifact/
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cmake --build build -j$(nproc)
```

## Run lazylog system
1. prepare configure files
```
cp cfg/rdma.prop cfg_datalog/ && cp cfg/rdma.prop cfg_metadatalog/ && cp cfg/rdma.prop cfg_tasklog/
```
2. add ssh keys for all machines  

3. run it!!
```
cd LazyLog-Artifact/scripts 
#run for datalog
./run.sh cfg_datalog 4
#run for metadatalog
./run.sh cfg_metadatalog 4
#run for tasklog
./run.sh cfg_tasklog 4
```
4. check whether all processes are running
```
sudo lsof -t -i:31856 -i:31857 -i:31858 -i:31862 -i:31853 -i:31854 -i:31855 -i:31861 -i:31850 -i:31851 -i:31852 -i:31860
#node 0 should have 0 process
#node 1,2,3 should have 4 processes
#node 4,5 should have 2 processes
```
  
## Test in client side

Inside ozonedb-tmp/lazylog directory, we have basic client to test the basic functionality of lazylog system.

```
#prepare the lib
git clone git@github.com:XinyingZheng00/ozonedb-tmp.git # if not already
cp /sharedfs/LazyLog-Artifact/build/src/liblazylogcli.a /sharedfs/ozonedb-tmp/lazylog/lib/ && cp /sharedfs/LazyLog-Artifact/build/src/cons_log/storage/libbackendcli.a /sharedfs/ozonedb-tmp/lazylog/lib/
cp /sharedfs/eRPC/build/liberpc.a /sharedfs/ozonedb-tmp/lazylog/lib/

#start testing 
cd /sharedfs/ozonedb-tmp/lazylog && mkdir -p build && cd build && cmake .. && make -j($nproc)
sudo GLOG_minloglevel=1 ./basic_client -P /sharedfs/LazyLog-Artifact/cfg_metadatalog/be.prop -P /sharedfs/LazyLog-Artifact/cfg_metadatalog/dl_client.prop -P /sharedfs/LazyLog-Artifact/cfg_metadatalog/rdma.prop -p mode=w

sudo GLOG_minloglevel=1 ./basic_client -P /sharedfs/LazyLog-Artifact/cfg_datalog/be.prop -P /sharedfs/LazyLog-Artifact/cfg_datalog/dl_client.prop -P /sharedfs/LazyLog-Artifact/cfg_datalog/rdma.prop -p mode=w

sudo GLOG_minloglevel=1 ./basic_client -P /sharedfs/LazyLog-Artifact/cfg_tasklog/be.prop -P /sharedfs/LazyLog-Artifact/cfg_tasklog/dl_client.prop -P /sharedfs/LazyLog-Artifact/cfg_tasklog/rdma.prop -p mode=w
```

# Compile Ozonedb system

```
cd /sharedfs
git clone git@github.com:XinyingZheng00/ozonedb-tmp.git # if not already
# set up nodes
cd ozonedb-tmp && bash bench/scripts/setup_node.sh
# compile the project
bash bench/scripts/update_jni.sh
```

# Run Ozonedb experiments in local machine
we are using zfs filesystem, and utilize the idea "filesystem as a shared log"
The config file is  ozonedb-tmp/bench/scripts/config/ycsb.yaml

**note: the threads parameter only works for ozonedb**
```
# run the experiment
cd /sharedfs/ozonedb-tmp/bench/scripts/local
python3 load_local_ycsb.py
python3 run_local_ycsb.py
# the result will be placed in /sharedfs/ozonedb-tmp/bench/results/

# plot the experiment
cd /sharedfs/ozonedb-tmp/bench/scripts/plot
# for latency and throughput comparison: python3 latency.py [local/cloud] [regular expression]
python3 latency.py local 1KB-10000*
# runtime throughput: python3 throughput_over_time.py [local/cloud] [0/1 indicate to add event or not]
python3 throughput_over_time.py local 0 1KB-10000-insert*

```
# Compile and Run Lazykv System (which is a baseline)

```
cd /sharedfs/ozonedb-tmp/lazykv-jni-maven
sudo mvn clean package
sudo cp ${OZONEDB_HOME}/lazykv-jni-maven/jni/target/classes/liblazykv.so /usr/lib
sudo mvn install:install-file -Dfile=${OZONEDB_HOME}/lazykv-jni-maven/jni/target/demoproc-jni-1.0-jar-with-dependencies.jar -DgroupId=lazykvjnimaven   -DartifactId=demoproc-jni   -Dversion=1.0   -Dpackaging=jar

cd ${OZONEDB_HOME}/bench/scripts/lazylog
python3 load_lazy_ycsb.py
python3 run_lazy_ycsb.py 
```