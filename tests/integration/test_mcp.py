import json
import os
import subprocess
import time
import pytest
import psycopg
import pytest_asyncio
from docker.errors import DockerException
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer

from materialize_mcp_server.mz_client import MzClient


@pytest_asyncio.fixture(scope="session")
async def materialize_pool():
    try:
        context = subprocess.check_output(
            ["docker", "context", "show"], text=True
        ).strip()
        output = subprocess.check_output(
            ["docker", "context", "inspect", context], text=True
        )
        context_info = json.loads(output)[0]
        host = context_info["Endpoints"]["docker"]["Host"]

        os.environ["DOCKER_HOST"] = host
        print(f"Configured DOCKER_HOST = {host}")

    except Exception as e:
        pytest.skip(f"Failed to configure DOCKER_HOST: {e}")

    try:
        # Configure the container with explicit settings
        container = (
            DockerContainer("materialize/materialized:latest")
            .with_exposed_ports(6875)
            .with_env("MZ_LOG_FILTER", "info")
        )

        print("Starting Materialize container...")
        container.start()
        print("Materialize container started")

        # Get container info using the container object directly
        print(f"Container ID: {container._container.id}")
        print(f"Container status: {container._container.status}")

    except DockerException as e:
        pytest.skip(
            f"Failed to start Materialize container; skipping integration tests: {e}"
        )

    host = container.get_container_host_ip()
    port = container.get_exposed_port(6875)
    print(f"Materialize running at {host}:{port}")

    dsn = f"postgresql://materialize@{host}:{port}/materialize"

    # Wait for the container to be ready
    pool = AsyncConnectionPool(conninfo=dsn, min_size=1, max_size=10, open=False)

    for attempt in range(30):
        try:
            print(f"Connection attempt {attempt + 1}...")

            await pool.open()
            yield pool

            break
        except psycopg.OperationalError as e:
            if attempt == 29:
                container.stop()
                pytest.skip(
                    f"Materialize did not become ready in time; skipping integration tests. Last error: {e}"
                )
            time.sleep(10)
            print(f"Connection attempt {attempt + 1} failed, retrying...")

    container.stop()


@pytest.mark.asyncio
async def test_list_tools(materialize_pool):
    client = MzClient(pool=materialize_pool)
    tools = await client.list_tools()
    assert len(tools) == 0
