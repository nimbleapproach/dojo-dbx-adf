import os

import pytest
from pyspark.sql import SparkSession

# Names of fixtures that require Spark to be available
_SPARKY_FIXTURES = ["spark", "spark_fixture", "dbutils"]


@pytest.fixture(scope="module")
def spark_fixture():
    spark = (
        SparkSession.builder.appName("Integration Test PySpark")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark


def _spark_is_available() -> bool:
    """
    Helper method to determine if we're running in an environment that contains a Spark
    instance that we could run our non-unit tests against.

    :return: whether Spark has been detected.
    """
    if os.getenv("PYSPARK_PYTHON"):  # not a perfect check, but it should do for now.
        return True
    return False


def _mark_tests_using_spark_fixture(tests: list[pytest.Function]) -> None:
    """
    Adds the `requires_spark` marker to tests that are using fixtures that require a
    Spark instance.

    :param tests: list of tests collected by `pytest`
    """
    for test in tests:
        # If the test uses _any_ of our Spark fixtures, add a marker!
        if any(
            sparky_fixture in test.fixturenames for sparky_fixture in _SPARKY_FIXTURES
        ):
            test.add_marker(pytest.mark.requires_spark)


def _skip_spark_tests_if_spark_not_available(test: pytest.Function) -> None:
    """
    If Spark is not available, we will tell `pytest` to skip tests that require a
    SparkSession.

    If Spark is available, no action will be taken by default.

    :param test: test collected by `pytest`
    """
    if _spark_is_available():
        return

    requires_spark_markers = list(test.iter_markers(name="requires_spark"))

    if requires_spark_markers:
        pytest.skip("Skipped tests that require a SparkSession")


def pytest_collection_modifyitems(items):
    _mark_tests_using_spark_fixture(tests=items)


def pytest_runtest_setup(item):
    _skip_spark_tests_if_spark_not_available(test=item)
