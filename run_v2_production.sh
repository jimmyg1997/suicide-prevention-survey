#!/bin/bash
PORT=8600
PID=$(lsof -ti :$PORT) 

if [ ! -z "$PID" ]; then
  echo "Killing process on port $PORT..."
  kill -9 $PID
fi

python3 main_v2.py
