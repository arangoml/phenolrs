#!/bin/bash

# Configuration environment variables:
#   STARTER_MODE:             (single|cluster|activefailover), default single
#   DOCKER_IMAGE:             ArangoDB docker image, default docker.io/arangodb/arangodb:latest
#   SSL:                      (true|false), default false
#   DATABASE_EXTENDED_NAMES:  (true|false), default false
#   ARANGO_LICENSE_KEY:       only required for ArangoDB Enterprise

# EXAMPLE:
# STARTER_MODE=cluster SSL=true ./start_db.sh

STARTER_MODE=${STARTER_MODE:=cluster}
DOCKER_IMAGE=${DOCKER_IMAGE:=docker.io/arangodb/arangodb:latest}
SSL=${SSL:=false}
DATABASE_EXTENDED_NAMES=${DATABASE_EXTENDED_NAMES:=false}
DATABASE_PORT=${DATABASE_PORT:=8529}

# Ports for the other database nodes in the cluster, and the starter port
DB_PORT2=$((DATABASE_PORT+1))
DB_PORT3=$((DATABASE_PORT+2))
STARTER_PORT=$((DATABASE_PORT-1))

STARTER_DOCKER_IMAGE=docker.io/arangodb/arangodb-starter:latest
GW=172.28.0.1
LOCALGW=localhost
docker network create arangodb --subnet 172.28.0.0/16

# exit when any command fails
set -e

docker pull $STARTER_DOCKER_IMAGE
docker pull $DOCKER_IMAGE

LOCATION=$(pwd)/$(dirname "$0")

echo "Averysecretword" > "$LOCATION"/jwtSecret
docker run --rm -v "$LOCATION"/jwtSecret:/jwtSecret "$STARTER_DOCKER_IMAGE" auth header --auth.jwt-secret /jwtSecret > "$LOCATION"/jwtHeader
AUTHORIZATION_HEADER=$(cat "$LOCATION"/jwtHeader)

STARTER_ARGS=
SCHEME=http
ARANGOSH_SCHEME=http+tcp
COORDINATORS=("$LOCALGW:$DATABASE_PORT" "$LOCALGW:$DB_PORT2" "$LOCALGW:$DB_PORT3")
COORDINATORSINTERNAL=("$GW:$DATABASE_PORT" "$GW:$DB_PORT2" "$GW:$DB_PORT3")

if [ "$STARTER_MODE" == "single" ]; then
  COORDINATORS=("$LOCALGW:$DATABASE_PORT")
  COORDINATORSINTERNAL=("$GW:$DATABASE_PORT")
fi

if [ "$SSL" == "true" ]; then
    STARTER_ARGS="$STARTER_ARGS --ssl.keyfile=server.pem"
    SCHEME=https
    ARANGOSH_SCHEME=http+ssl
fi

if [ "$DATABASE_EXTENDED_NAMES" == "true" ]; then
    STARTER_ARGS="${STARTER_ARGS} --all.database.extended-names-databases=true"
fi

if [ "$USE_MOUNTED_DATA" == "true" ]; then
    STARTER_ARGS="${STARTER_ARGS} --starter.data-dir=/data"
    MOUNT_DATA="-v $LOCATION/data:/data"
fi

docker run -d \
    --name=adb \
    -p $STARTER_PORT:$STARTER_PORT  \
    -v "$LOCATION"/server.pem:/server.pem \
    -v "$LOCATION"/jwtSecret:/jwtSecret \
    $MOUNT_DATA \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e ARANGO_LICENSE_KEY="$ARANGO_LICENSE_KEY" \
    $STARTER_DOCKER_IMAGE \
    $STARTER_ARGS \
    --docker.container=adb \
    --auth.jwt-secret=/jwtSecret \
    --starter.address="${GW}" \
    --starter.port=${STARTER_PORT} \
    --docker.image="${DOCKER_IMAGE}" \
    --args.all.query.require-with="true" \
    --starter.local --starter.mode=${STARTER_MODE} --all.log.level=debug --all.log.output=+ --log.verbose


wait_server() {
    # shellcheck disable=SC2091
    until $(curl --output /dev/null --insecure --fail --silent --head -i -H "$AUTHORIZATION_HEADER" "$SCHEME://$1/_api/version"); do
        printf '.'
        sleep 1
    done
}

echo "Waiting..."

for a in ${COORDINATORS[*]} ; do
    wait_server "$a"
done

set +e
ITER=0
for a in ${COORDINATORS[*]} ; do
    echo ""
    echo "Setting username and password..."
    docker run --rm ${DOCKER_IMAGE} arangosh --server.endpoint="$ARANGOSH_SCHEME://${COORDINATORSINTERNAL[ITER]}" --server.authentication=false --javascript.execute-string='require("org/arangodb/users").update("root", "test")'
    ITER=$(expr $ITER + 1)
done
set -e

for a in ${COORDINATORS[*]} ; do
    echo ""
    echo "Requesting endpoint version..."
    curl -u root:test --insecure --fail "$SCHEME://$a/_api/version"
done

echo ""
echo ""
echo "Done, your deployment is reachable at: "
for a in ${COORDINATORS[*]} ; do
    echo "$SCHEME://$a"
    echo ""
done

if [ "$STARTER_MODE" == "activefailover" ]; then
  LEADER=$("$LOCATION"/find_active_endpoint.sh)
  echo "Leader: $SCHEME://$LEADER"
  echo ""
fi
