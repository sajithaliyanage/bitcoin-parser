#!/bin/bash

bitcoind -daemon -rpcuser=user -rpcpassword=password -rpcport=8332 -server=1 -rpcallowip=0.0.0.0/0 -rpcbind=0.0.0.0 -blocksonly -txindex -maxconnections=10 -rpcthreads=10 -blocksdir=/mnt/data/bitcoin --datadir=/mnt/data/bitcoin