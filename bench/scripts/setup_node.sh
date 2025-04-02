# set up local host
# in the directory of this ozonedb repo
# must run with ". bench/scripts/setup_node.sh"
    
git submodule update --init --recursive
echo "export OZONEDB_HOME=$(pwd)" >> ~/.bashrc
source ~/.bashrc
echo $OZONEDB_HOME
sudo apt update
sudo apt install -y cmake maven python3-pip zip pkg-config sqlite3
sudo apt install openjdk-8-jdk -y
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
echo "export PATH=$JAVA_HOME/bin:$PATH" >> ~/.bashrc
source ~/.bashrc
java -version

# for local experiments, also set up zfs
sudo apt install -y zfsutils-linux
sudo fdisk /dev/sda
sudo fdisk -l
sudo zpool create tank /dev/sda6
sudo zfs list
df /tank
sudo chmod +777 /tank
    
# compile ozonedb
bash $OZONEDB_HOME/bench/scripts/update_jni.sh
    