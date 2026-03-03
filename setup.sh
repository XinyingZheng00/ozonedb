#!/bin/bash

# OzoneDB Setup Script
# This script automates the setup process for OzoneDB

set -e  # Exit on error

echo "=========================================="
echo "OzoneDB Setup Script"
echo "=========================================="

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Step 1: Set OZONEDB_HOME
echo ""
echo "Step 1: Setting OZONEDB_HOME environment variable..."
export OZONEDB_HOME="$SCRIPT_DIR"
if ! grep -q "OZONEDB_HOME" ~/.bashrc; then
    echo "export OZONEDB_HOME=$OZONEDB_HOME" >> ~/.bashrc
    echo "Added OZONEDB_HOME to ~/.bashrc"
else
    echo "OZONEDB_HOME already in ~/.bashrc"
fi
source ~/.bashrc 2>/dev/null || true
echo "OZONEDB_HOME=$OZONEDB_HOME"

# Step 2: Initialize git submodules
echo ""
echo "Step 2: Initializing git submodules..."
git submodule update --init --recursive
echo "Git submodules initialized"

# Step 3: Install system dependencies
echo ""
echo "Step 3: Installing system dependencies..."
echo "This may require sudo access..."
sudo apt update
sudo apt install -y cmake maven python3-pip zip pkg-config sqlite3 build-essential
sudo apt install -y openjdk-8-jdk

# Step 4: Set JAVA_HOME
echo ""
echo "Step 4: Configuring JAVA_HOME..."
ARCH=$(dpkg --print-architecture)
if [ "$ARCH" = "amd64" ]; then
    JAVA_HOME_PATH="/usr/lib/jvm/java-8-openjdk-amd64"
elif [ "$ARCH" = "arm64" ]; then
    JAVA_HOME_PATH="/usr/lib/jvm/java-8-openjdk-arm64"
else
    echo "Warning: Unsupported architecture: $ARCH"
    echo "Please set JAVA_HOME manually"
    JAVA_HOME_PATH=""
fi

if [ -n "$JAVA_HOME_PATH" ]; then
    export JAVA_HOME="$JAVA_HOME_PATH"
    if ! grep -q "JAVA_HOME" ~/.bashrc; then
        echo "export JAVA_HOME=$JAVA_HOME_PATH" >> ~/.bashrc
        echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
        echo "Added JAVA_HOME to ~/.bashrc"
    fi
    source ~/.bashrc 2>/dev/null || true
    echo "JAVA_HOME=$JAVA_HOME"
    java -version
fi

# Step 5: Install Python dependencies
echo ""
echo "Step 5: Installing Python dependencies..."
pip3 install --user pyyaml matplotlib numpy
echo "Python dependencies installed"

# Step 6: Create results directory
echo ""
echo "Step 6: Creating results directory..."
mkdir -p "$OZONEDB_HOME/bench/results/local"
echo "Results directory created"

# Step 7: Check/create ycsb_data_path
echo ""
echo "Step 7: Checking YCSB data path..."
YCSB_DATA_PATH="/tank/ycsb_data"
if [ ! -d "$YCSB_DATA_PATH" ]; then
    echo "Creating YCSB data directory at $YCSB_DATA_PATH..."
    echo "Note: This requires sudo. If you prefer a different location, edit bench/scripts/config/ycsb.yaml"
    read -p "Create $YCSB_DATA_PATH? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        sudo mkdir -p "$YCSB_DATA_PATH"
        sudo chmod 777 "$YCSB_DATA_PATH"
        echo "YCSB data directory created"
    else
        echo "Skipping. Please create the directory manually or update ycsb.yaml"
    fi
else
    echo "YCSB data directory already exists: $YCSB_DATA_PATH"
fi

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Build the project:"
echo "   cd $OZONEDB_HOME"
echo "   bash bench/scripts/update_jni.sh"
echo ""
echo "2. Build YCSB:"
echo "   cd $OZONEDB_HOME/ycsb"
echo "   mvn clean package -DskipTests"
echo ""
echo "3. Run tests:"
echo "   cd $OZONEDB_HOME/bench/scripts/local"
echo "   python3 load_local_ycsb.py"
echo "   python3 run_local_ycsb.py"
echo ""
echo "4. Generate plots:"
echo "   cd $OZONEDB_HOME/bench/scripts/plot"
echo "   python3 latency.py local \"1KB-100000*\""
echo "   python3 throughput_over_time.py local 0 \"1KB-100000-workloada*\""
echo ""
echo "For detailed instructions, see SETUP_GUIDE.md"
