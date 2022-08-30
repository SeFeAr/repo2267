import asyncio
import math
import urllib.request

import pytest

from function import bench


class MockUrlOpen:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def read():
        return (
            b'[{"cm": "acme.cloudapp.net", "pk": 7799916, "fqdn": '
            b'"instance.domain.com", "guid": "q0280893-3352-41-cefa4bbf7278", '
            b'"username": "user@domain.com", "password": "Secret Password", '
            b'"version": 7, "ayuda_bms_api_v2_auth_headers": '
            b'"amx d=:240-401s-1e8r-b10b84:1118"}]'
        )

    @staticmethod
    def add_header(key, val):
        pass


class MockSession:
    def __init__(self, *args, **kwargs):
        self.reason = "OK"
        self.status = 200
        self.cookies = {"Domain": "domain.name.com"}

    async def request(self, *args, **kwargs):
        await asyncio.sleep(0)

    async def __aenter__(self, *args, **kwargs):
        return self

    async def __aexit__(self, *args, **kwargs):
        await asyncio.sleep(0)


MAMBA_JSON_RESPONSE = [
    {
        "cm": "acme.cloudapp.net",
        "pk": 7799916,
        "fqdn": "instance.domain.com",
        "guid": "q0280893-3352-41-cefa4bbf7278",
        "username": "user@domain.com",
        "password": "Secret Password",
        "version": 7,
        "ayuda_bms_api_v2_auth_headers": "amx d=:240-401s-1e8r-b10b84:1118",
    }
]


@pytest.mark.asyncio
async def test_timer():
    timer = bench.Timer()
    with timer:
        await asyncio.sleep(0.5)

    assert math.isclose(timer.elapsed, 0.5, abs_tol=0.05)


def test_get_instances_from_mamba(monkeypatch):
    monkeypatch.setattr(urllib.request, "Request", MockUrlOpen)
    monkeypatch.setattr(urllib.request, "urlopen", MockUrlOpen)

    assert bench.get_instances_from_mamba("sdf", "sdf") == MAMBA_JSON_RESPONSE


def test_post_instances_to_mamba(monkeypatch):
    monkeypatch.setattr(urllib.request, "Request", MockUrlOpen)
    monkeypatch.setattr(urllib.request, "urlopen", MockUrlOpen)

    assert bench.post_instances_to_mamba("sdf", "sdf", {"test": "data"}) is None
