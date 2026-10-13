import pytest
import requests

from market_data_collector import rest_client as rc


def test_network_error_becomes_snapshot_error(monkeypatch):
    def fake_get(*args, **kwargs):
        raise requests.ConnectionError("no network")

    monkeypatch.setattr(rc.requests, "get", fake_get)

    with pytest.raises(rc.SnapshotError):
        rc.get_snapshot("BTCUSDT", 1000)


def test_http_error_status_becomes_snapshot_error(monkeypatch):
    response = requests.Response()
    response.status_code = 429

    monkeypatch.setattr(rc.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(rc.SnapshotError):
        rc.get_snapshot("BTCUSDT", 1000)


def test_bad_json_becomes_snapshot_error(monkeypatch):
    response = requests.Response()
    response.status_code = 200
    response._content = b"not json"

    monkeypatch.setattr(rc.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(rc.SnapshotError):
        rc.get_snapshot("BTCUSDT", 1000)


def test_request_has_timeout(monkeypatch):
    seen = {}

    def fake_get(url, params, timeout=None):
        seen["timeout"] = timeout
        raise requests.Timeout("too slow")

    monkeypatch.setattr(rc.requests, "get", fake_get)

    with pytest.raises(rc.SnapshotError):
        rc.get_snapshot("BTCUSDT", 1000)
    assert seen["timeout"] == rc.REST_TIMEOUT
