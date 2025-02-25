#!/bin/bash
PORT=8600
PID=$(lsof -ti :$PORT) 

if [ ! -z "$PID" ]; then
  echo "Killing process on port $PORT..."
  kill -9 $PID
fi

echo "Starting Flask on port $PORT..."
export FLASK_APP=main_v2.py
export FLASK_ENV=development  # Enables debug mode
flask run --host=0.0.0.0 --port=$PORT &

# Wait for 2 seconds before opening the browser
sleep 2

# Open the default browser to the Flask app
open http://localhost:$PORT
