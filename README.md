### Step1: build ozonedb shared library

```bash
git clone ozonedb
git submodule update
echo "export OZONEDB_HOME=/users/Xinying/ozonedb" >> ~/.bashrc && source ~/.bashrc
# install build tools: 
sudo apt update
sudo apt install cmake maven python3-pip -y
# install necessary dependency for vcpkg: 
sudo apt-get install zip pkg-config -y
mkdir build && cd build && cmake .. && make -j$(nproc)
```

### Step2: build/update ozonedb jni
```bash 
sudo apt install openjdk-8-jdk -y
sudo update-alternatives --config java
echo "
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
" >> ~/.bashrc && source ~/.bashrc

# This needs to be run each time you update ozonedb code
./$OZONEDB_HOME/bench/scripts/update_jni.sh
```

### Step3: run ycsb using local SSD
```bash
# To run using local SSD, we first need to set up zfs file system
sudo apt install zfsutils-linux
sudo fdisk -l
sudo zpool create tank /dev/sda2
sudo zfs list
ls /tank
df /tank
# Change the configuration in bench/scripts/ycsb.config
# The followings are all under bench/scripts
# load first
python3 load_local_ycsb.py --config ycsb.config
# run
python3 run_local_ycsb.py --config ycsb.config
# get graph for local results [the argument is the pattern to match all the result file under bench/results]
python3 ycsb2graph.py {"ozonedb*","rocksdb*"}
```