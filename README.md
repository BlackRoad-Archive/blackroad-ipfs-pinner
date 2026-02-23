# blackroad-ipfs-pinner

> IPFS content pinning service with multi-gateway redundancy

Pin IPFS CIDs across multiple gateways (IPFS.io, Pinata, Infura, web3.storage) for redundancy. Tracks all pinning jobs in a local SQLite database with verification, retry, and reporting capabilities.

## Features

- 🔁 **Multi-gateway pinning** — Pin to multiple IPFS gateways simultaneously
- ✅ **Verification** — HEAD-check all gateways and track replica count
- 🔄 **Repin failed jobs** — Retry all failed pins with one command
- 📊 **Redundancy report** — Identify low-replica content
- 📦 **Manifest export** — Export full state as JSON
- ⏳ **Pin policies** — Permanent, temporary, or TTL-based

## Installation

```bash
git clone https://github.com/BlackRoad-Archive/blackroad-ipfs-pinner
cd blackroad-ipfs-pinner
pip install -r requirements.txt  # only stdlib + pytest for tests
```

## Usage

### Register gateways

```bash
python ipfs_pinner.py add-gateway ipfs.io https://ipfs.io --type public
python ipfs_pinner.py add-gateway pinata https://gateway.pinata.cloud --type pinata
python ipfs_pinner.py list-gateways
```

### Pin a CID

```bash
python ipfs_pinner.py pin QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco "my-content" \
  --gateways "ipfs.io,pinata" \
  --policy permanent
```

### Pin with TTL

```bash
python ipfs_pinner.py pin QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG "temp-content" \
  --policy ttl --ttl 24
```

### Verify pins

```bash
python ipfs_pinner.py verify <job-id>
python ipfs_pinner.py verify-all
```

### Repin failures

```bash
python ipfs_pinner.py repin-failed
```

### Redundancy report

```bash
python ipfs_pinner.py report
```

### Export manifest

```bash
python ipfs_pinner.py export --output manifest.json
```

### Unpin

```bash
python ipfs_pinner.py unpin QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco
```

## Architecture

```
IPFSPinner
├── SQLite DB (~/.blackroad/ipfs_pinner.db)
│   ├── gateways      — Registered gateway endpoints + stats
│   └── pinning_jobs  — CID → gateway mapping + status
├── Gateway verification via HTTP HEAD requests
└── EMA-based success rate and latency tracking
```

## Gateway Types

| Type | Description |
|------|-------------|
| `public` | Public IPFS gateways (ipfs.io, dweb.link) |
| `private` | Self-hosted gateways |
| `infura` | Infura IPFS service |
| `pinata` | Pinata dedicated gateway |
| `w3s` | web3.storage |

## Pin Policies

| Policy | Description |
|--------|-------------|
| `permanent` | Pin indefinitely |
| `temporary` | Pin until manually removed |
| `ttl` | Pin for a fixed number of hours |

## Tests

```bash
pytest tests/ -v
```
