# corpus-build

In order to make new corpus from a database of material from the web archive,
this repo contains functionality to extract the full text from specific domains that have a responsible editor.

`responsible-editor-filter.yaml` contains the domains that should be filtered upon.
A postgreSQL database is required with the following tables:
- `warcinfo` - contains the metadata about the full text entry
- `fulltext` - contains the actual text

Both of these tables are linked together using the field `fulltext_hash`

# Local setup

## Build

To build the container image locally, simply run
```shell
docker build . --tag corpus-build
```

## Run

> [!CAUTION]
> Note that the password is passed in as plain text, which is a security vulnerability.

To run the built container, run:

```shell
docker run --interactive --tty --rm corpus-build
# Inside the container run:
source /virtual_environment/bin/activate
python main.py \
    --filter-yaml-file=responsible-editor-filter.yaml \
    --hostname=<host-or-ip-of-database-server> \
    --port=<target-port> \
    --database=<name-of-database-to-connect> \
    --user=<database-username> \
    --password=<plain-text-password-for-user> \
    --output-dir="/build/output"
```

If the setup of the server is as `main.py` expect, then files will appear in
`/build/output/` containing the relevant full text for the specified domains
listed in `responsible-editor-filter.yaml`.

# Kubernetes

## Configuration

Take a look `kubernetes/` for a template of how to deploy it in your cluster.

You need to configure the `namespace` field in `kubernetes/kustomization.yaml`,
and also the proxy fields in `kubernetes/secrets.yaml` (or delete the entire
file if you do not need it)

## Deploy

To deploy the pod, run:

```shell
kubectl apply --kustomize kubernetes/
```

## Run

When the pod has been created successfully, run:

```shell
kubectl exec --stdin --tty corpus-build -- bash
# Inside the container run:
source /virtual_environment/bin/activate
python main.py \
    --filter-yaml-file=responsible-editor-filter.yaml \
    --hostname=<host-or-ip-of-database-server> \
    --port=<target-port> \
    --database=<name-of-database-to-connect> \
    --user=<database-username> \
    --password=<plain-text-password-for-user> \
    --output-dir="/build/output"
```

To export the output to your local machine, run:

```shell
kubectl exec corpus-build -- tar cf - /build/output | tar xf - -C .
```
