#!/bin/bash

while true; do
    echo "Starting script..."
    python3 query_from_rcm.py

    echo "Script crashed at $(date)"
    echo "Restarting in 5 seconds..."
    sleep 5
done