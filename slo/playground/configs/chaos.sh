#!/bin/sh -e

get_random_container() {
    # Get a list of all containers starting with ydb-database-*
    containers=$(docker ps --format '{{.Names}}' | grep '^ydb-database-')

    # Convert the list to a newline-separated string
    containers=$(echo "$containers" | tr ' ' '\n')

    # Count the number of containers
    containersCount=$(echo "$containers" | wc -l)

    # Generate a random number between 0 and containersCount - 1
    randomIndex=$(shuf -i 0-$(($containersCount - 1)) -n 1)

    # Get the container name at the random index
    nodeForChaos=$(echo "$containers" | sed -n "$(($randomIndex + 1))p")
}


sleep 60

echo "Start CHAOS YDB cluster!"

for i in $(seq 1 5)
do
  echo "[$(date)]: docker stop/start iteration $i"
  
  get_random_container
  
  sh -c "docker stop ${nodeForChaos} -t 10"
  sh -c "docker start ${nodeForChaos}"
  
  sleep 60
done

for i in $(seq 1 3)
do
  echo "[$(date)]: docker restart iteration $i"
  
  get_random_container
  
  sh -c "docker restart ${nodeForChaos} -t 0"
  
  sleep 60
done

get_random_container

echo "[$(date)]: docker kill -s SIGKILL ${nodeForChaos}"

sh -c "docker kill -s SIGKILL ${nodeForChaos}"
