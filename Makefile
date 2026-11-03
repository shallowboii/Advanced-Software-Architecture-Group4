PYTHON ?= python3
PROFILE ?= smoke
PORT ?= 8088
ROOT := $(CURDIR)
COMPOSE = docker compose --project-directory "$(ROOT)" -f "$(ROOT)/src/deploy/compose/$(PROFILE).json"

.PHONY: generate up down logs smoke test manifests formal
generate:
	$(PYTHON) src/tools/generate_deployment.py
up:
	FACTORY_PORT=$(PORT) $(COMPOSE) up --build -d
down:
	$(COMPOSE) down
logs:
	$(COMPOSE) logs -f --tail=100
smoke:
	$(PYTHON) src/tools/smoke.py --base-url http://localhost:$(PORT)
test:
	$(PYTHON) -m unittest discover -s src/inventory -p 'test*.py'
	$(MAKE) -C src/machine check
	$(PYTHON) -m unittest discover -s src/tools -p 'test*.py'
	$(PYTHON) -m unittest discover -s src/experiment -p 'test*.py'
manifests:
	kubectl kustomize src/deploy/k8s/overlays/mock-cluster >/dev/null
formal:
	$(PYTHON) src/models/generate.py
	$(PYTHON) src/models/verify.py
