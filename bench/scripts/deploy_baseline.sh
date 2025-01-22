sudo apt-get install libgflags-dev libsnappy-dev zlib1g-dev  libbz2-dev  liblz4-dev  libzstd-dev -y
mkdir $OZONEDB_HOME/bench/baseline && cd $OZONEDB_HOME/bench/baseline
git clone https://github.com/facebook/rocksdb.git
cd rocksdb && mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DUSE_RTTI=true
make -j$(nproc) rocksdb
sudo make install -j$(nproc)
sudo ldconfig