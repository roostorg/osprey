#!/bin/sh

mc alias set myminio http://localhost:9000 minioadmin minioadmin123
mc mb --ignore-existing myminio/execution-output
echo "Bucket 'execution-output' ready"
