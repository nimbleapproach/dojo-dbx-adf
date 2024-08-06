# Medallion Architecture

![Medallion Architecture](/documentation/assets/images/medallion.png "Medallion Architecture")

## Testing

**Note:** all commands should be run in the root of the repository

### Running Unit Tests

1. Create a virtual environment:

   ```shell
   python -m venv .venv
   ```

1. Activate virtual environment:

   ```shell
   source ./.venv/scripts/activate
   ```

1. Install required Python libraries:

   ```shell
   pip install --requirement ./databricks/requirements/test.txt
   ```

1. Running the tests is then as simple as:

   ```shell
   pytest
   ```

### Running Spark Tests

Tests that require \[Py\]Spark are not strictly unit-tests (as they require you to be running a Spark cluster, after all!).

Easiest way to run a Spark cluster? Docker!

1. Build the Docker container:

    ```shell
    docker build --tag pyspark-test .
    ```

1. Run the tests inside the container:

    ```shell
    MSYS_NO_PATHCONV=1 \
        docker run \
            --rm \
            --volume $(pwd):/app/ \
            --workdir /app/ \
            pyspark-test \
                /databricks/python3/bin/python -m pytest
    ```

_Aside: if you're running Docker in WSL then you need to change `$(pwd):/app/` to `/mnt/$(pwd):/app/`._

---

## Change History

- v0.01 14.09.2023 Initialize
