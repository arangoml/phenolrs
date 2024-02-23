#!/bin/bash
DATABASE_PORT=${DATABASE_PORT:=8529}
STARTER_PORT=$((DATABASE_PORT-1))

docker exec adb /app/arangodb stop --starter.port=${STARTER_PORT}
sleep 1
docker rm -f \
  adb \
docker rm -f configs
