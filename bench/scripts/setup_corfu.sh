#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/CorfuDB/CorfuDB.git"
CLONE_DIR="${CLONE_DIR:-$HOME/CorfuDB}"

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
sudo apt-get install -y openjdk-25-jdk maven

if [[ ! -d "$CLONE_DIR/.git" ]]; then
  git clone "$REPO_URL" "$CLONE_DIR"
fi

cd "$CLONE_DIR"
mvn clean install -DskipTests

# Follow CorfuDB README.md for different setups for running corfu
