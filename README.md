# Neotec Dual Sync v2.4.0

**Production-oriented configurable dual-instance synchronisation framework for Frappe/ERPNext v15.**

Neotec Dual Sync lets two Frappe instances exchange documents in real time or via scheduled batch jobs, with field-level mapping, conflict resolution, idempotency, loop prevention, and HMAC-signed transport.

---

## Features

- **Bidirectional sync** — Source, Target, or Both role per instance
- **Flexible triggers** — On Submit, After Insert, On Update, Batch, or Manual
- **Field mapping engine** — Direct copy, Static values, Python transform scripts, type coercion
- **Child table mapping** — Sync linked child tables with their own mapping profiles
- **Loop prevention** — Route-trace + hop-count blocking prevents infinite sync cycles
- **Idempotency** — Deduplicates by transaction ID and source document identity
- **HMAC-SHA256 request signing** — Verifies every inbound request is from a trusted source
- **IP allow-list** — Per-instance IP restriction enforcement
- **Conflict resolution UI** — Accept incoming / Keep existing / Ignore with one click
- **Automatic retry** — Exponential back-off retry for failed outbound syncs
- **Condition scripts** — Python expressions to conditionally skip sync per document
- **Dry-run mode** — Test your configuration without sending data
- **Audit snapshots** — Capture before/after payload for every sync operation
- **Secret masking** — Never logs api_secret or shared_secret in plaintext
- **Log housekeeping** — Daily cleanup of old Success/Skipped records
- **Live dashboard** — Real-time counters on the Settings form

---

## Installation

```bash
bench get-app neotec_dual_sync
bench --site your-site install-app neotec_dual_sync
bench --site your-site migrate
```

---

## Quick Setup

### Step 1 — Configure this instance

Open **Neotec Sync Settings** and set:

| Field | Value |
|---|---|
| Instance Role | `Source` (this pushes), `Target` (this receives), or `Both` |
| Local Instance ID | Auto-generated on install — copy for the other side |
| Remote Base URL | `https://your-other-frappe.example.com` |
| API Key / API Secret | Frappe API credentials for the remote user |
| Shared Secret | A random string — must match on both instances |
| Enabled | ✅ (only after everything else is configured) |

Click **Test Connection** to verify before enabling.

### Step 2 — Add sync rules

In the **Rules** table, add one row per DocType to sync:

- **Source DocType** — e.g. `Sales Order`
- **Target DocType** — usually the same, unless you're mapping across types
- **Trigger Mode** — `On Submit` for submitted documents, `After Insert` for drafts
- **Mapping Profile** — optional; leave blank for pass-through
- **Duplicate Policy** — `Skip If Unchanged` (recommended default)

### Step 3 — Configure the remote instance

Repeat Step 1 on the other Frappe instance with the roles reversed (or `Both`).

---

## Field Mappings

Create a **Neotec Sync Mapping** document to transform fields during sync:

| Mapping Type | Behaviour |
|---|---|
| Direct | Copy source field value as-is |
| Static Value | Always write a fixed value |
| Scripted Transform | Python code: `result = value.upper()` |
| Ignore | Skip this field entirely |

The `transform_script` runs inside Frappe's `safe_exec` sandbox. Available variables: `value` (source field value), `source` (full source document dict), `result` (write your output here).

---

## Conflict Resolution

When a document arrives and already exists locally, the **Duplicate Policy** determines the action:

- `Skip If Unchanged` — skips if content hash matches
- `Update Existing` — overwrites local doc with incoming data
- `Reject Duplicate` — rejects with an error
- `Create Conflict Record` — creates a **Neotec Sync Conflict** for manual review

Open any conflict record to see a side-by-side diff and click **Accept Incoming**, **Keep Existing**, or **Ignore**.

---

## Security

- All outbound requests are signed with `HMAC-SHA256(shared_secret, body)` via the `X-Neotec-Signature` header.
- The inbound endpoint verifies this signature before any processing.
- API credentials use standard Frappe token auth.
- Secrets are never stored in log payloads when **Mask Secrets in Logs** is enabled.
- Per-instance IP allow-lists restrict which hosts may send inbound sync requests.

---

## Architecture

```
[Source Instance]                    [Target Instance]
  doc_events hook                      /api/method/neotec_dual_sync.api.receive_document
       │                                          │
  create Sync Log (Queued)           verify HMAC signature
       │                             check IP allowlist
  Scheduler (every 10m)              loop detection
       │                             idempotency check
  process_batch_queue()              apply_inbound_document()
       │                             field mapping
  push_document_to_remote()          insert / update / conflict
       │                             create Sync Log (Success/Failed)
  update Sync Log (Success/Failed)
       │
  retry_failed_syncs() [10m]
  cleanup_old_logs()  [daily 2 AM]
```

---

## Roles

| Role | Access |
|---|---|
| `Neotec Dual Sync Manager` | Full read/write on all sync DocTypes |
| `Neotec Dual Sync User` | Read-only access to logs and settings |
| `Neotec Dual Sync API` | No desk access — for API-level integrations |

---

## License

MIT © Neotec
