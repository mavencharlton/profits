#!/bin/bash
set -e

REPO_URL="https://github.com/YOUR_USERNAME/tradeos.git"
PROJECT_DIR="tradeos"

echo "=== TradeOS Colab Bootstrap ==="

# install system deps
apt-get install -y python3-pip > /dev/null 2>&1

# clone or pull
if [ -d "$PROJECT_DIR" ]; then
    echo "Repo exists — pulling latest..."
    cd $PROJECT_DIR && git pull && cd ..
else
    echo "Cloning repo..."
    git clone $REPO_URL $PROJECT_DIR
fi

cd $PROJECT_DIR

# install python deps
echo "Installing dependencies..."
pip install -r requirements.txt --quiet

# env check
if [ ! -f ".env" ]; then
    echo "⚠ No .env file found. Copy .env.example and fill in values."
    exit 1
fi

# run
echo "Starting TradeOS..."
python app.py