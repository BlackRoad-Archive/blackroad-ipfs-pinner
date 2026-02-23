"""Tests for IPFS Pinner."""
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import sys, os
sys.path.insert(0, "/tmp")
from ipfs_pinner import IPFSPinner, PinningJob, Gateway, STATUS_PINNED, STATUS_FAILED, STATUS_UNPINNED, POLICY_PERMANENT, POLICY_TTL, GW_PUBLIC


@pytest.fixture
def pinner(tmp_path):
    db = tmp_path / "test.db"
    return IPFSPinner(db_path=db)


@pytest.fixture
def pinner_with_gw(pinner):
    pinner.add_gateway("ipfs.io", "https://ipfs.io", GW_PUBLIC)
    pinner.add_gateway("dweb.link", "https://dweb.link", GW_PUBLIC)
    return pinner


def test_init_creates_db(tmp_path):
    db = tmp_path / "test.db"
    IPFSPinner(db_path=db)
    assert db.exists()


def test_add_gateway(pinner):
    gw = pinner.add_gateway("test-gw", "https://example.com", GW_PUBLIC)
    assert gw.name == "test-gw"
    assert gw.url == "https://example.com"
    assert gw.type == GW_PUBLIC
    assert gw.id


def test_list_gateways(pinner_with_gw):
    gws = pinner_with_gw.list_gateways()
    assert len(gws) == 2
    names = {g.name for g in gws}
    assert "ipfs.io" in names
    assert "dweb.link" in names


def test_get_gateway_by_name(pinner_with_gw):
    gw = pinner_with_gw.get_gateway_by_name("ipfs.io")
    assert gw is not None
    assert gw.url == "https://ipfs.io"


def test_get_gateway_by_name_missing(pinner):
    gw = pinner.get_gateway_by_name("nonexistent")
    assert gw is None


@patch("ipfs_pinner.IPFSPinner._pin_on_gateway", return_value=True)
def test_pin_success(mock_pin, pinner_with_gw):
    job_id = pinner_with_gw.pin("QmTest123", "my-file", gateways=["ipfs.io"])
    assert job_id
    mock_pin.assert_called_once()


@patch("ipfs_pinner.IPFSPinner._pin_on_gateway", return_value=False)
def test_pin_all_fail(mock_pin, pinner_with_gw):
    job_id = pinner_with_gw.pin("QmFailCID", "fail-file", gateways=["ipfs.io"])
    assert job_id
    jobs = pinner_with_gw.list_jobs(status_filter=STATUS_FAILED)
    assert any(j["name"] == "fail-file" for j in jobs)


def test_pin_no_gateways_raises(pinner):
    with pytest.raises(ValueError, match="No gateways"):
        pinner.pin("QmTest", "test")


@patch("ipfs_pinner.IPFSPinner._pin_on_gateway", return_value=True)
def test_verify_pin(mock_pin, pinner_with_gw):
    job_id = pinner_with_gw.pin("QmVerify", "verify-test", gateways=["ipfs.io"])
    result = pinner_with_gw.verify_pin(job_id)
    assert result["cid"] == "QmVerify"
    assert result["status"] in (STATUS_PINNED, STATUS_FAILED)


def test_verify_pin_missing_job(pinner):
    with pytest.raises(ValueError):
        pinner.verify_pin("nonexistent-id")


@patch("ipfs_pinner.IPFSPinner._pin_on_gateway", return_value=True)
def test_verify_all_pins(mock_pin, pinner_with_gw):
    pinner_with_gw.pin("QmA", "a", gateways=["ipfs.io"])
    pinner_with_gw.pin("QmB", "b", gateways=["ipfs.io"])
    results = pinner_with_gw.verify_all_pins()
    assert len(results) == 2


@patch("ipfs_pinner.IPFSPinner._pin_on_gateway", return_value=True)
def test_unpin(mock_pin, pinner_with_gw):
    pinner_with_gw.pin("QmUnpin", "unpin-test", gateways=["ipfs.io"])
    count = pinner_with_gw.unpin("QmUnpin")
    assert count >= 1
    jobs = pinner_with_gw.list_jobs(status_filter=STATUS_UNPINNED)
    assert any(j["name"] == "unpin-test" for j in jobs)


@patch("ipfs_pinner.IPFSPinner._pin_on_gateway", side_effect=[False, True])
def test_repin_failed(mock_pin, pinner_with_gw):
    pinner_with_gw.pin("QmFailed", "failed-file", gateways=["ipfs.io"])
    results = pinner_with_gw.repin_failed()
    assert isinstance(results, list)


@patch("ipfs_pinner.IPFSPinner._pin_on_gateway", return_value=True)
def test_redundancy_report(mock_pin, pinner_with_gw):
    pinner_with_gw.pin("QmReport", "report-test", gateways=["ipfs.io"])
    report = pinner_with_gw.get_redundancy_report()
    assert "total_jobs" in report
    assert "by_status" in report
    assert "avg_replicas" in report
    assert "low_redundancy" in report


@patch("ipfs_pinner.IPFSPinner._pin_on_gateway", return_value=True)
def test_export_manifest(mock_pin, pinner_with_gw, tmp_path):
    pinner_with_gw.pin("QmExport", "export-test", gateways=["ipfs.io"])
    out = tmp_path / "manifest.json"
    pinner_with_gw.export_manifest(output_path=str(out))
    assert out.exists()
    data = json.loads(out.read_text())
    assert "jobs" in data
    assert "gateways" in data
    assert data["version"] == "1.0"


def test_ttl_expiry():
    job = PinningJob(
        id="x", cid="Qm", name="t", size_estimate=0, gateways=[],
        pin_policy=POLICY_TTL, ttl_hours=1,
        created_at="2000-01-01T00:00:00",
    )
    assert job.is_expired()


def test_permanent_never_expires():
    job = PinningJob(
        id="x", cid="Qm", name="t", size_estimate=0, gateways=[],
        pin_policy=POLICY_PERMANENT, ttl_hours=0,
        created_at="2000-01-01T00:00:00",
    )
    assert not job.is_expired()
