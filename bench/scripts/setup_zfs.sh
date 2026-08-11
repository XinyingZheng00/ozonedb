# for local experiments, also set up zfs
sudo apt install -y zfsutils-linux
sudo fdisk /dev/sda
sudo fdisk -l
sudo zpool create tank /dev/sda6
sudo zfs list
df /tank
sudo chmod +777 /tank
