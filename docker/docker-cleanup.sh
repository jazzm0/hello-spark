#!/usr/bin/env bash

# Delete all containers
docker rm -f $(docker ps -a -q)
# Delete all images
docker rmi -f $(docker images -q)
# remove all dangling volumes
for volume in `docker volume ls -qf dangling=true`
do
    docker volume rm ${volume}
done