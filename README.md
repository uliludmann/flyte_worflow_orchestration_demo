# Install

1. Clone the repo
2. Install python deps

```sh
poetry install
```

3. Install Bin Deps

```sh
brew install flyteorg/homebrew-tap/flytectl
```

4. Bootstrap flyte-demo environment
   Docker must be running. Couldnt get it to work with Podman quickly. :(
   run the demo

```sh
flytectl demo start
```

open http://localhost:30080/console to see the flyte Dashboard.

## How to use

Open a `poetry shell`

Run our Small ETL Workflow:

```sh
pyflyte run --remote flyte_benchmarks/titanic_etl.py \
    extract_transform_load_wf --uri \
    amazon-sagemaker-data-wrangler-documentation-artifacts/walkthrough_titanic.csv
```

See it in action using the output url.

## Teardown

Run:

```sh
flytectl demo teardown
```
