import json
import os
import requests
import subprocess
import time
import pytest
import psycopg
import pytest_asyncio
from docker.errors import DockerException
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

from materialize_mcp_server.mz_client import MzClient


def wait_for_readyz(host: str, port: int, timeout: int = 120, interval: int = 1):
    url = f"http://{host}:{port}/api/readyz"
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        print("Waiting for materialized to become ready...")
        time.sleep(interval)

    raise TimeoutError(f"Materialized did not become ready within {timeout} seconds.")


@pytest_asyncio.fixture(scope="function")
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
            .with_exposed_ports(6875, 6878)
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
    sql_port = int(container.get_exposed_port(6875))
    http_port = int(container.get_exposed_port(6878))
    wait_for_readyz(host, http_port)
    print(f"Materialize running at {host}:{sql_port}")

    conn = f"postgres://materialize@{host}:{sql_port}/materialize"
    pool = AsyncConnectionPool(conninfo=conn, min_size=1, max_size=10, open=False)
    await pool.open()
    yield pool
    container.stop()


@pytest.mark.asyncio
async def test_list_tools(materialize_pool):
    client = MzClient(pool=materialize_pool)
    tools = await client.list_tools()
    assert len(tools) == 0
