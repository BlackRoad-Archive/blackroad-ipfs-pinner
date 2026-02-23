"""
IPFS Content Pinning Service with Redundancy
blackroad-ipfs-pinner: Pin CIDs across multiple IPFS gateways for redundancy.
"""

import argparse
import json
import logging
import os
import sqlite3
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from uuid import uuid4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ipfs_pinner")

DB_PATH = Path(os.environ.get("IPFS_PINNER_DB", Path.home() / ".blackroad" / "ipfs_pinner.db"))

STATUS_PENDING = "pending"
STATUS_PINNING = "pinning"
STATUS_PINNED = "pinned"
STATUS_FAILED = "failed"
STATUS_UNPINNED = "unpinned"

POLICY_PERMANENT = "permanent"
POLICY_TEMPORARY = "temporary"
POLICY_TTL = "ttl"

GW_PUBLIC = "public"
GW_PRIVATE = "private"
GW_INFURA = "infura"
GW_PINATA = "pinata"
GW_W3S = "w3s"


@dataclass
class Gateway:
    """Represents an IPFS gateway endpoint."""
    id: str
    name: str
    url: str
    type: str  # public/private/infura/pinata/w3s
    api_key_hint: str = ""
    success_rate: float = 1.0
    avg_latency_ms: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_row(cls, row: tuple) -> "Gateway":
        return cls(
            id=row[0],
            name=row[1],
            url=row[2],
            type=row[3],
            api_key_hint=row[4],
            success_rate=row[5],
            avg_latency_ms=row[6],
        )


@dataclass
class PinningJob:
    """Represents a single CID pinning job."""
    id: str
    cid: str
    name: str
    size_estimate: int
    gateways: list
    status: str = STATUS_PENDING
    replicas: int = 0
    last_verified: Optional[str] = None
    pin_policy: str = POLICY_PERMANENT
    ttl_hours: int = 0
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    error_msg: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["gateways"] = json.dumps(self.gateways)
        return d

    @classmethod
    def from_row(cls, row: tuple) -> "PinningJob":
        return cls(
            id=row[0],
            cid=row[1],
            name=row[2],
            size_estimate=row[3],
            gateways=json.loads(row[4]) if row[4] else [],
            status=row[5],
            replicas=row[6],
            last_verified=row[7],
            pin_policy=row[8],
            ttl_hours=row[9],
            created_at=row[10],
            error_msg=row[11] if len(row) > 11 else "",
        )

    def is_expired(self) -> bool:
        """Check if a TTL-based pin has expired."""
        if self.pin_policy != POLICY_TTL or self.ttl_hours <= 0:
            return False
        if not self.created_at:
            return False
        created = datetime.fromisoformat(self.created_at)
        expiry = created + timedelta(hours=self.ttl_hours)
        return datetime.utcnow() > expiry


class IPFSPinner:
    """
    IPFS Content Pinning Service.

    Manages pinning of IPFS CIDs across multiple gateways for redundancy.
    Stores state in a local SQLite database.

    Usage::

        pinner = IPFSPinner()
        pinner.add_gateway("ipfs.io", "https://ipfs.io", GW_PUBLIC)
        job_id = pinner.pin("QmXxx...", "my-file", ["ipfs.io"])
        pinner.verify_pin(job_id)
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Initialize the SQLite schema."""
        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS gateways (
                    id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    url TEXT NOT NULL,
                    type TEXT NOT NULL,
                    api_key_hint TEXT DEFAULT '',
                    success_rate REAL DEFAULT 1.0,
                    avg_latency_ms REAL DEFAULT 0.0
                );

                CREATE TABLE IF NOT EXISTS pinning_jobs (
                    id TEXT PRIMARY KEY,
                    cid TEXT NOT NULL,
                    name TEXT NOT NULL,
                    size_estimate INTEGER DEFAULT 0,
                    gateways TEXT DEFAULT '[]',
                    status TEXT DEFAULT 'pending',
                    replicas INTEGER DEFAULT 0,
                    last_verified TEXT,
                    pin_policy TEXT DEFAULT 'permanent',
                    ttl_hours INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    error_msg TEXT DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_cid ON pinning_jobs(cid);
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON pinning_jobs(status);
            """)
        logger.debug("Database initialized at %s", self.db_path)

    # ---------- Gateway Management ----------

    def add_gateway(
        self,
        name: str,
        url: str,
        gw_type: str = GW_PUBLIC,
        api_key_hint: str = "",
    ) -> Gateway:
        """Register a new IPFS gateway.

        Args:
            name: Human-readable gateway name.
            url: Base URL of the gateway.
            gw_type: Gateway type (public/private/infura/pinata/w3s).
            api_key_hint: Partial API key hint for identification (never full key).

        Returns:
            The created Gateway dataclass instance.
        """
        gw = Gateway(
            id=str(uuid4()),
            name=name,
            url=url.rstrip("/"),
            type=gw_type,
            api_key_hint=api_key_hint,
        )
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO gateways
                   (id, name, url, type, api_key_hint, success_rate, avg_latency_ms)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (gw.id, gw.name, gw.url, gw.type, gw.api_key_hint, gw.success_rate, gw.avg_latency_ms),
            )
        logger.info("Gateway registered: %s (%s)", name, url)
        return gw

    def list_gateways(self) -> list:
        """Return all registered gateways."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM gateways ORDER BY name").fetchall()
        return [Gateway(
            id=r["id"], name=r["name"], url=r["url"], type=r["type"],
            api_key_hint=r["api_key_hint"], success_rate=r["success_rate"],
            avg_latency_ms=r["avg_latency_ms"]
        ) for r in rows]

    def get_gateway_by_name(self, name: str) -> Optional[Gateway]:
        """Fetch a gateway by name."""
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM gateways WHERE name = ?", (name,)).fetchone()
        if not row:
            return None
        return Gateway(
            id=row["id"], name=row["name"], url=row["url"], type=row["type"],
            api_key_hint=row["api_key_hint"], success_rate=row["success_rate"],
            avg_latency_ms=row["avg_latency_ms"]
        )

    # ---------- Pinning ----------

    def pin(
        self,
        cid: str,
        name: str,
        gateways: Optional[list] = None,
        pin_policy: str = POLICY_PERMANENT,
        ttl_hours: int = 0,
        size_estimate: int = 0,
    ) -> str:
        """Pin a CID across one or more gateways.

        Args:
            cid: The IPFS content identifier to pin.
            name: Human-readable name for this pin job.
            gateways: List of gateway names to pin on. Uses all if None.
            pin_policy: permanent/temporary/ttl.
            ttl_hours: Hours until expiry (only for ttl policy).
            size_estimate: Estimated size in bytes.

        Returns:
            Job ID (UUID string).
        """
        if gateways is None:
            gateways = [gw.name for gw in self.list_gateways()]

        if not gateways:
            raise ValueError("No gateways specified and none registered.")

        job = PinningJob(
            id=str(uuid4()),
            cid=cid,
            name=name,
            size_estimate=size_estimate,
            gateways=gateways,
            status=STATUS_PINNING,
            pin_policy=pin_policy,
            ttl_hours=ttl_hours,
        )

        with self._get_conn() as conn:
            conn.execute(
                """INSERT INTO pinning_jobs
                   (id, cid, name, size_estimate, gateways, status, replicas,
                    last_verified, pin_policy, ttl_hours, created_at, error_msg)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    job.id, job.cid, job.name, job.size_estimate,
                    json.dumps(job.gateways), job.status, 0,
                    None, job.pin_policy, job.ttl_hours, job.created_at, "",
                ),
            )

        logger.info("Pinning job created: %s for CID %s", job.id, cid)
        successes = self._attempt_pin(job)

        status = STATUS_PINNED if successes > 0 else STATUS_FAILED
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE pinning_jobs SET status=?, replicas=?, last_verified=? WHERE id=?",
                (status, successes, datetime.utcnow().isoformat(), job.id),
            )
        logger.info("Pin job %s finished: status=%s replicas=%d", job.id, status, successes)
        return job.id

    def _attempt_pin(self, job: PinningJob) -> int:
        """Try to pin on each gateway, return number of successes."""
        successes = 0
        for gw_name in job.gateways:
            gw = self.get_gateway_by_name(gw_name)
            if gw is None:
                logger.warning("Gateway %s not found, skipping.", gw_name)
                continue
            ok = self._pin_on_gateway(job.cid, gw)
            if ok:
                successes += 1
                self._update_gateway_stats(gw.id, True, 0)
            else:
                self._update_gateway_stats(gw.id, False, 0)
        return successes

    def _pin_on_gateway(self, cid: str, gw: Gateway) -> bool:
        """Attempt to pin a CID on a specific gateway."""
        url = f"{gw.url}/ipfs/{cid}"
        start = time.time()
        try:
            req = urllib.request.Request(url, method="HEAD")
            req.add_header("User-Agent", "blackroad-ipfs-pinner/1.0")
            with urllib.request.urlopen(req, timeout=15) as resp:
                elapsed = (time.time() - start) * 1000
                logger.debug("HEAD %s -> %d (%.0fms)", url, resp.status, elapsed)
                self._update_gateway_latency(gw.id, elapsed)
                return resp.status < 400
        except urllib.error.HTTPError as e:
            logger.warning("HTTP error pinning %s on %s: %s", cid, gw.name, e)
            return e.code < 500
        except Exception as e:
            logger.warning("Failed to pin %s on %s: %s", cid, gw.name, e)
            return False

    def _update_gateway_stats(self, gw_id: str, success: bool, latency_ms: float) -> None:
        """Update running success rate for a gateway."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT success_rate FROM gateways WHERE id=?", (gw_id,)
            ).fetchone()
            if not row:
                return
            old_rate = row["success_rate"]
            new_rate = old_rate * 0.9 + (1.0 if success else 0.0) * 0.1
            conn.execute(
                "UPDATE gateways SET success_rate=? WHERE id=?",
                (round(new_rate, 4), gw_id),
            )

    def _update_gateway_latency(self, gw_id: str, latency_ms: float) -> None:
        """Update EMA of gateway latency."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT avg_latency_ms FROM gateways WHERE id=?", (gw_id,)
            ).fetchone()
            if not row:
                return
            old = row["avg_latency_ms"]
            new = old * 0.8 + latency_ms * 0.2 if old > 0 else latency_ms
            conn.execute(
                "UPDATE gateways SET avg_latency_ms=? WHERE id=?",
                (round(new, 2), gw_id),
            )

    # ---------- Verification ----------

    def verify_pin(self, job_id: str) -> dict:
        """Verify that a pinning job is still accessible on its gateways.

        Args:
            job_id: The job UUID to verify.

        Returns:
            Dict with job_id, cid, replicas, verified_gateways, failed_gateways, status.
        """
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM pinning_jobs WHERE id=?", (job_id,)
            ).fetchone()
        if not row:
            raise ValueError(f"Job {job_id} not found.")

        job = PinningJob(
            id=row["id"], cid=row["cid"], name=row["name"],
            size_estimate=row["size_estimate"],
            gateways=json.loads(row["gateways"]),
            status=row["status"], replicas=row["replicas"],
            last_verified=row["last_verified"],
            pin_policy=row["pin_policy"], ttl_hours=row["ttl_hours"],
            created_at=row["created_at"],
        )

        if job.is_expired():
            logger.info("Job %s has expired TTL, marking unpinned.", job_id)
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE pinning_jobs SET status=? WHERE id=?",
                    (STATUS_UNPINNED, job_id),
                )
            return {"job_id": job_id, "status": STATUS_UNPINNED, "expired": True}

        verified = []
        failed = []
        for gw_name in job.gateways:
            gw = self.get_gateway_by_name(gw_name)
            if gw and self._pin_on_gateway(job.cid, gw):
                verified.append(gw_name)
            else:
                failed.append(gw_name)

        replicas = len(verified)
        status = STATUS_PINNED if replicas > 0 else STATUS_FAILED
        now = datetime.utcnow().isoformat()

        with self._get_conn() as conn:
            conn.execute(
                "UPDATE pinning_jobs SET replicas=?, last_verified=?, status=? WHERE id=?",
                (replicas, now, status, job_id),
            )

        result = {
            "job_id": job_id,
            "cid": job.cid,
            "replicas": replicas,
            "verified_gateways": verified,
            "failed_gateways": failed,
            "status": status,
            "last_verified": now,
        }
        logger.info("Verified job %s: %d replicas", job_id, replicas)
        return result

    def verify_all_pins(self) -> list:
        """Verify all active pinning jobs.

        Returns:
            List of verification result dicts.
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT id FROM pinning_jobs WHERE status IN (?, ?)",
                (STATUS_PINNED, STATUS_PINNING),
            ).fetchall()

        results = []
        for row in rows:
            try:
                result = self.verify_pin(row["id"])
                results.append(result)
            except Exception as e:
                logger.error("Error verifying job %s: %s", row["id"], e)
                results.append({"job_id": row["id"], "error": str(e)})
        logger.info("Verified %d jobs.", len(results))
        return results

    # ---------- Unpin / Repin ----------

    def unpin(self, cid: str) -> int:
        """Mark all jobs for a CID as unpinned.

        Args:
            cid: The CID to unpin.

        Returns:
            Number of jobs updated.
        """
        with self._get_conn() as conn:
            cursor = conn.execute(
                "UPDATE pinning_jobs SET status=? WHERE cid=? AND status != ?",
                (STATUS_UNPINNED, cid, STATUS_UNPINNED),
            )
            count = cursor.rowcount
        logger.info("Unpinned %d jobs for CID %s", count, cid)
        return count

    def repin_failed(self) -> list:
        """Attempt to repin all failed jobs.

        Returns:
            List of job IDs that were retried.
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pinning_jobs WHERE status=?", (STATUS_FAILED,)
            ).fetchall()

        retried = []
        for row in rows:
            job = PinningJob(
                id=row["id"], cid=row["cid"], name=row["name"],
                size_estimate=row["size_estimate"],
                gateways=json.loads(row["gateways"]),
                status=row["status"], replicas=row["replicas"],
                last_verified=row["last_verified"],
                pin_policy=row["pin_policy"], ttl_hours=row["ttl_hours"],
                created_at=row["created_at"],
            )
            successes = self._attempt_pin(job)
            status = STATUS_PINNED if successes > 0 else STATUS_FAILED
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE pinning_jobs SET status=?, replicas=?, last_verified=? WHERE id=?",
                    (status, successes, datetime.utcnow().isoformat(), job.id),
                )
            retried.append({"job_id": job.id, "status": status, "replicas": successes})

        logger.info("Retried %d failed jobs.", len(retried))
        return retried

    # ---------- Reporting ----------

    def get_redundancy_report(self) -> dict:
        """Generate a redundancy report across all jobs.

        Returns:
            Dict with totals, by_status, by_policy, low_redundancy list.
        """
        with self._get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM pinning_jobs").fetchone()[0]
            by_status = {
                row[0]: row[1]
                for row in conn.execute(
                    "SELECT status, COUNT(*) FROM pinning_jobs GROUP BY status"
                ).fetchall()
            }
            avg_replicas = conn.execute(
                "SELECT AVG(replicas) FROM pinning_jobs WHERE status=?", (STATUS_PINNED,)
            ).fetchone()[0] or 0
            low_redundancy = conn.execute(
                "SELECT id, cid, name, replicas FROM pinning_jobs WHERE replicas < 2 AND status=?",
                (STATUS_PINNED,),
            ).fetchall()
            by_policy = {
                row[0]: row[1]
                for row in conn.execute(
                    "SELECT pin_policy, COUNT(*) FROM pinning_jobs GROUP BY pin_policy"
                ).fetchall()
            }

        report = {
            "total_jobs": total,
            "by_status": by_status,
            "by_policy": by_policy,
            "avg_replicas": round(avg_replicas, 2),
            "low_redundancy": [
                {"id": r[0], "cid": r[1], "name": r[2], "replicas": r[3]}
                for r in low_redundancy
            ],
            "generated_at": datetime.utcnow().isoformat(),
        }
        return report

    def export_manifest(self, output_path: Optional[str] = None) -> str:
        """Export all pinning jobs as a JSON manifest.

        Args:
            output_path: File path to write to. Prints to stdout if None.

        Returns:
            JSON string of the manifest.
        """
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM pinning_jobs ORDER BY created_at").fetchall()
            gw_rows = conn.execute("SELECT * FROM gateways ORDER BY name").fetchall()

        jobs = []
        for r in rows:
            jobs.append({
                "id": r["id"], "cid": r["cid"], "name": r["name"],
                "size_estimate": r["size_estimate"],
                "gateways": json.loads(r["gateways"]),
                "status": r["status"], "replicas": r["replicas"],
                "last_verified": r["last_verified"],
                "pin_policy": r["pin_policy"], "ttl_hours": r["ttl_hours"],
                "created_at": r["created_at"],
            })

        gws = []
        for r in gw_rows:
            gws.append({
                "id": r["id"], "name": r["name"], "url": r["url"],
                "type": r["type"], "success_rate": r["success_rate"],
                "avg_latency_ms": r["avg_latency_ms"],
            })

        manifest = {
            "version": "1.0",
            "exported_at": datetime.utcnow().isoformat(),
            "gateways": gws,
            "jobs": jobs,
        }
        output = json.dumps(manifest, indent=2)

        if output_path:
            with open(output_path, "w") as f:
                f.write(output)
            logger.info("Manifest exported to %s", output_path)
        else:
            print(output)

        return output

    # ---------- CLI Helpers ----------

    def list_jobs(self, status_filter: Optional[str] = None) -> list:
        """List all jobs, optionally filtered by status."""
        with self._get_conn() as conn:
            if status_filter:
                rows = conn.execute(
                    "SELECT * FROM pinning_jobs WHERE status=? ORDER BY created_at DESC",
                    (status_filter,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM pinning_jobs ORDER BY created_at DESC"
                ).fetchall()
        result = []
        for r in rows:
            result.append({
                "id": r["id"][:8] + "...",
                "cid": r["cid"][:16] + "...",
                "name": r["name"],
                "status": r["status"],
                "replicas": r["replicas"],
                "policy": r["pin_policy"],
                "created_at": r["created_at"][:10],
            })
        return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_table(rows: list, headers: list) -> None:
    if not rows:
        print("  (no results)")
        return
    widths = [len(h) for h in headers]
    for row in rows:
        for i, key in enumerate(headers):
            widths[i] = max(widths[i], len(str(row.get(key, ""))))
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  " + "  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*[str(row.get(k, "")) for k in headers]))


def cmd_add_gateway(args, pinner: IPFSPinner) -> None:
    gw = pinner.add_gateway(args.name, args.url, args.type, args.api_key_hint or "")
    print(f"✓ Gateway registered: {gw.name} [{gw.type}] → {gw.url}")


def cmd_pin(args, pinner: IPFSPinner) -> None:
    gateways = args.gateways.split(",") if args.gateways else None
    job_id = pinner.pin(
        args.cid, args.name,
        gateways=gateways,
        pin_policy=args.policy,
        ttl_hours=args.ttl,
        size_estimate=args.size,
    )
    print(f"✓ Pin job created: {job_id}")


def cmd_verify(args, pinner: IPFSPinner) -> None:
    result = pinner.verify_pin(args.job_id)
    print(json.dumps(result, indent=2))


def cmd_verify_all(args, pinner: IPFSPinner) -> None:
    results = pinner.verify_all_pins()
    print(f"Verified {len(results)} jobs:")
    for r in results:
        status = r.get("status", "error")
        replicas = r.get("replicas", "?")
        print(f"  {r['job_id'][:8]}... status={status} replicas={replicas}")


def cmd_unpin(args, pinner: IPFSPinner) -> None:
    count = pinner.unpin(args.cid)
    print(f"✓ Unpinned {count} job(s) for CID {args.cid}")


def cmd_repin_failed(args, pinner: IPFSPinner) -> None:
    results = pinner.repin_failed()
    if not results:
        print("No failed jobs found.")
        return
    for r in results:
        print(f"  {r['job_id'][:8]}... → {r['status']} ({r['replicas']} replicas)")


def cmd_report(args, pinner: IPFSPinner) -> None:
    report = pinner.get_redundancy_report()
    print(f"=== IPFS Redundancy Report ({report['generated_at'][:10]}) ===")
    print(f"Total jobs: {report['total_jobs']}")
    print(f"Avg replicas (pinned): {report['avg_replicas']}")
    print("\nBy Status:")
    for status, count in report["by_status"].items():
        print(f"  {status:<12} {count}")
    print("\nBy Policy:")
    for policy, count in report["by_policy"].items():
        print(f"  {policy:<12} {count}")
    if report["low_redundancy"]:
        print(f"\nLow Redundancy ({len(report['low_redundancy'])} jobs):")
        for job in report["low_redundancy"]:
            print(f"  {job['name']} [{job['cid'][:12]}...] replicas={job['replicas']}")


def cmd_export(args, pinner: IPFSPinner) -> None:
    pinner.export_manifest(output_path=args.output)
    if args.output:
        print(f"✓ Manifest exported to {args.output}")


def cmd_list_gateways(args, pinner: IPFSPinner) -> None:
    gateways = pinner.list_gateways()
    if not gateways:
        print("No gateways registered.")
        return
    headers = ["name", "url", "type", "success_rate", "avg_latency_ms"]
    rows = [{
        "name": gw.name, "url": gw.url, "type": gw.type,
        "success_rate": f"{gw.success_rate:.2%}",
        "avg_latency_ms": f"{gw.avg_latency_ms:.0f}ms",
    } for gw in gateways]
    _print_table(rows, headers)


def cmd_list_jobs(args, pinner: IPFSPinner) -> None:
    jobs = pinner.list_jobs(status_filter=getattr(args, "status", None))
    if not jobs:
        print("No jobs found.")
        return
    _print_table(jobs, ["id", "name", "cid", "status", "replicas", "policy", "created_at"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IPFS Content Pinning Service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", help="Override database path")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # add-gateway
    p = sub.add_parser("add-gateway", help="Register a new IPFS gateway")
    p.add_argument("name", help="Gateway name")
    p.add_argument("url", help="Gateway base URL")
    p.add_argument("--type", default=GW_PUBLIC, choices=[GW_PUBLIC, GW_PRIVATE, GW_INFURA, GW_PINATA, GW_W3S])
    p.add_argument("--api-key-hint", default="", help="Partial API key hint")
    p.set_defaults(func=cmd_add_gateway)

    # list-gateways
    p = sub.add_parser("list-gateways", help="List registered gateways")
    p.set_defaults(func=cmd_list_gateways)

    # pin
    p = sub.add_parser("pin", help="Pin a CID")
    p.add_argument("cid", help="IPFS CID to pin")
    p.add_argument("name", help="Human-readable name")
    p.add_argument("--gateways", help="Comma-separated gateway names")
    p.add_argument("--policy", default=POLICY_PERMANENT, choices=[POLICY_PERMANENT, POLICY_TEMPORARY, POLICY_TTL])
    p.add_argument("--ttl", type=int, default=0, help="TTL in hours (policy=ttl)")
    p.add_argument("--size", type=int, default=0, help="Estimated size in bytes")
    p.set_defaults(func=cmd_pin)

    # verify
    p = sub.add_parser("verify", help="Verify a single pin job")
    p.add_argument("job_id", help="Job UUID")
    p.set_defaults(func=cmd_verify)

    # verify-all
    p = sub.add_parser("verify-all", help="Verify all active pin jobs")
    p.set_defaults(func=cmd_verify_all)

    # unpin
    p = sub.add_parser("unpin", help="Unpin all jobs for a CID")
    p.add_argument("cid", help="IPFS CID to unpin")
    p.set_defaults(func=cmd_unpin)

    # repin-failed
    p = sub.add_parser("repin-failed", help="Retry all failed pin jobs")
    p.set_defaults(func=cmd_repin_failed)

    # report
    p = sub.add_parser("report", help="Show redundancy report")
    p.set_defaults(func=cmd_report)

    # export
    p = sub.add_parser("export", help="Export pinning manifest as JSON")
    p.add_argument("--output", "-o", help="Output file path (default: stdout)")
    p.set_defaults(func=cmd_export)

    # list
    p = sub.add_parser("list", help="List all pinning jobs")
    p.add_argument("--status", help="Filter by status")
    p.set_defaults(func=cmd_list_jobs)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    db_path = Path(args.db) if getattr(args, "db", None) else None
    pinner = IPFSPinner(db_path=db_path)

    args.func(args, pinner)


if __name__ == "__main__":
    main()
