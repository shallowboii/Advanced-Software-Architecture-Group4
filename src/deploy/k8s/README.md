# Kubernetes factory

One Pod per physical-machine instance; execution state is held only in RAM. Six StatefulSets represent the six machine types. The unitank has fermentation and maturation phases in the same vessel. Machine sets use `OnDelete` updates so an image change cannot automatically replace occupied tanks.

Render a profile with `kubectl kustomize src/deploy/k8s/overlays/20`. Profiles are `smoke`, `20`, `40` and `mock-cluster`. The mock overlay exposes frontend NodePort 30088 and relies on the cluster's default StorageClass. Replace exposure, resource sizing and storage for the actual cluster. Development credentials in the generated files are placeholders; replace them with cluster-specific secret references when configuring the real cluster.

Before applying, set the four application images to the built immutable tags or load the local `beerfactory-{machine,backend,inventory,frontend}:dev` images into a local cluster. Use `kubectl apply -k ...` only against the intended kube context. The generator `src/tools/generate_deployment.py` is the source for generated manifests; regenerate after changing defaults.

The backend has two replicas with one active coordinator. A frontend or backend rollout must preserve state. For a controller upgrade, stop admission to the selected machine, wait until it is empty with no reservation or transfer, then delete only that Pod. Never force-scale down an occupied tank. Machines have no data claims. Restarting a busy controller invalidates the run and stops the line. Inventory also resets on restart.

PostgreSQL, RabbitMQ, Kafka and inventory are single-instance in these classroom profiles. This does not demonstrate infrastructure-wide high availability. Inventory runs as one Deployment with Recreate updates to avoid two independent stock copies. Do not update it during an experiment.

Local physical-button simulation: `python3 src/tools/operator.py stop --kubernetes --machine unitank-0`. This calls the controller's local socket via `kubectl exec`; it bypasses application buses but still depends on the Kubernetes control plane. A real hard-wired global stop circuit remains outside the software simulation.

The previous implementation added machine PVCs and an inventory StatefulSet. Removing them changes immutable StatefulSet fields: validate this revision in a fresh factory namespace/cluster. Do not apply it as an in-place upgrade of an occupied factory or delete existing volumes to force the update. Archive previous run evidence first.
