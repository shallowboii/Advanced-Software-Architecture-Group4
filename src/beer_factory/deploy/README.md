# Beer factory

Deploying requires:
- Docker
- kubectl
- k3s OR minikube

All commands must be run from `src/beer_factory`.

## Minikube - Setup

```sh
minikube start -p beer-factory --cpus=4 --memory=6144
kubectl config use-context beer-factory

sh deploy/scripts/images.sh build
sh deploy/scripts/images.sh load-minikube
```

## K3s - Setup

Build Docker images for the cluster:

```sh
PLATFORM=linux/amd64 sh deploy/scripts/images.sh build
sh deploy/scripts/images.sh save /tmp/beer-factory-images.tar
```

Import the archive on your cluster's nodes:

```sh
sudo k3s ctr images import /tmp/beer-factory-images.tar
```

## Deploy

Use the same manifests for minikube and k3s:

```sh
kubectl apply -k deploy
kubectl -n beer-factory wait --for=condition=complete job/kafka-topics --timeout=600s
kubectl -n beer-factory wait --for=condition=Available deployment \
  -l app.kubernetes.io/component=machine --timeout=600s
```

## Access services

If you need to port forward a service outside of the cluster's network:

```sh
kubectl -n beer-factory port-forward service/rabbitmq 15672:15672 1883:1883 5672:5672
kubectl -n beer-factory port-forward service/kafka 9092:9092
kubectl -n beer-factory port-forward service/redis 6379:6379
```

## Check

```sh
kubectl -n beer-factory logs job/kafka-topics
kubectl -n beer-factory logs deployment/mashing -c machine
kubectl -n beer-factory exec redis-0 -- redis-cli ping
kubectl -n beer-factory get events --sort-by=.lastTimestamp
```

## Rebuild

For minikube:

```sh
sh deploy/scripts/images.sh build
sh deploy/scripts/images.sh load-minikube
kubectl -n beer-factory rollout restart deployment -l app.kubernetes.io/component=machine
```

For k3s:
Re-run all the steps from k3s' Setup section.

If you changed topic configuration or bootstrap scripts:

```sh
kubectl -n beer-factory delete job kafka-topics --ignore-not-found
kubectl apply -k deploy
```

## Stop or remove

To stop everythng but keep data:

```sh
kubectl -n beer-factory delete deployments,statefulsets,jobs --all
```

You can then run `kubectl apply -k deploy` to resume the nodes.

To remove **everything**:

```sh
kubectl delete namespace beer-factory
```

## USEFUL COMMANDS

# Replace POD_NAME with a name from get pods
kubectl -n beer-factory describe pod POD_NAME

# Follow the mashing worker's output
kubectl -n beer-factory logs -f deployment/mashing -c machine

# Read initialization results
kubectl -n beer-factory logs job/kafka-topics

# Run a command inside Redis
kubectl -n beer-factory exec redis-0 -- redis-cli pin
