# set up local host
# in the directory of this ozonedb repo
# must run with ". bench/scripts/setup_node.sh"
    
git submodule update --init --recursive
echo "export OZONEDB_HOME=$(pwd)" >> ~/.bashrc
echo "export OZONEDB_HOME=$(pwd)" >> ~/.profile # for cloud use
source ~/.bashrc
source ~/.profile
echo $OZONEDB_HOME
sudo apt update
sudo apt install -y cmake maven python3-pip zip pkg-config sqlite3 build-essential
sudo apt install openjdk-8-jdk -y
ARCH=$(dpkg --print-architecture)
if [ "$ARCH" = "amd64" ]; then
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.profile
elif [ "$ARCH" = "arm64" ]; then
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64" >> ~/.bashrc
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64" >> ~/.profile
else
    echo "Unsupported architecture: $ARCH"
    echo $ARCH
fi
echo "export PATH=$JAVA_HOME/bin:$PATH" >> ~/.bashrc
echo "export PATH=$JAVA_HOME/bin:$PATH" >> ~/.profile # for cloud use
source ~/.bashrc
source ~/.profile
java -version

# for local experiments, also set up zfs
# sudo apt install -y zfsutils-linux
# sudo fdisk /dev/sda
# sudo fdisk -l
# sudo zpool create tank /dev/sda6
# sudo zfs list
# df /tank
# sudo chmod +777 /tank
    
# compile ozonedb
bash $OZONEDB_HOME/bench/scripts/update_jni.sh
    