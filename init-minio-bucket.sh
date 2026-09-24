#!/bin/sh
set -e

rc alias set myminio http://minio:9000 minioadmin minioadmin123
rc mb --ignore-existing myminio/execution-output
echo "Bucket 'execution-output' ready"
