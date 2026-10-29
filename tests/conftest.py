import pytest
from retailiq.runtime import create_spark


@pytest.fixture(scope="session")
def spark():
    session = create_spark("RetailIQ-tests", partitions=2)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
