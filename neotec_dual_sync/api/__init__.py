"""
Neotec Dual Sync — Public API endpoints (whitelisted Frappe methods).
"""
import json
import traceback

import frappe
from frappe import _
from neotec_dual_sync.api.services import (
    get_settings,
    payload_hash,
    verify_hmac_signature,
    register_idempotency,
    should_block_loop,
    create_sync_log,
    update_sync_log,
    apply_inbound_document,
    check_ip_allowlist,
    test_remote_connection,
)


# ---------------------------------------------------------------------------
# Inbound sync endpoint
# ---------------------------------------------------------------------------

@frappe.whitelist(allow_guest=False)
def receive_document():
    """
    Entry point for inbound document sync from a remote Frappe instance.
    Performs: auth check → HMAC verify → IP check → loop detection
               → idempotency → document apply → log.
    """
    settings = get_settings()

    if not settings.enabled:
        frappe.throw(_("Sync is disabled on this instance."), frappe.PermissionError)
    if settings.instance_role not in ("Target", "Both") or not settings.accept_inbound_sync:
        frappe.throw(_("Inbound sync is not allowed on this instance."), frappe.PermissionError)

    # Parse raw request body
    raw_body = frappe.request.get_data() or b""
    try:
        payload = json.loads(raw_body)
    except Exception:
        frappe.throw(_("Invalid JSON payload."), frappe.ValidationError)

    source_instance_id = payload.get("source_instance_id") or ""
    source_doctype = payload.get("source_doctype") or ""
    source_name = payload.get("source_docname") or ""
    tx = payload.get("sync_transaction_id") or frappe.generate_hash(length=20)
    sync_meta = payload.get("sync_meta") or {}

    # HMAC signature verification
    if settings.signature_required:
        provided_sig = frappe.request.headers.get("X-Neotec-Signature", "")
        shared_secret = settings.get_password("shared_secret") or ""
        if not shared_secret:
            _reject(source_doctype, source_name, tx, payload,
                    "HMAC_CONFIG_ERROR", "shared_secret not set but signature_required=1")
        if not verify_hmac_signature(raw_body, shared_secret, provided_sig):
            _reject(source_doctype, source_name, tx, payload,
                    "INVALID_SIGNATURE", "HMAC signature mismatch — request rejected")

    # IP allow-list check (look up instance record)
    instance_doc = None
    if source_instance_id:
        try:
            instance_doc = frappe.get_doc("Neotec Sync Instance", {"instance_id": source_instance_id})
            check_ip_allowlist(instance_doc)
        except frappe.DoesNotExistError:
            pass  # Unknown instance — we allow but don't enforce IP

    # Loop detection
    blocked, reason = should_block_loop(sync_meta, settings.local_instance_id)
    if blocked:
        create_sync_log(
            reference_doctype=source_doctype, reference_name=source_name,
            status="Loop Prevented", direction="Inbound",
            request_payload=payload, response_payload={"reason": reason},
            sync_transaction_id=tx, source_instance_id=source_instance_id,
        )
        return {"ok": False, "error_code": "LOOP_BLOCKED", "message": reason}

    # Idempotency check
    h = payload_hash(payload)
    existing, is_dup = register_idempotency(
        source_instance_id, source_doctype, source_name, tx, h
    )
    if is_dup:
        return {"ok": False, "error_code": "DUPLICATE_DETECTED",
                "message": "Transaction already processed", "idempotency_log": existing}

    # Apply the document to this instance
    result = apply_inbound_document(payload, settings)

    status = "Success" if result.get("ok") else "Failed"
    create_sync_log(
        reference_doctype=source_doctype, reference_name=source_name,
        status=status, direction="Inbound",
        request_payload=payload,
        response_payload=result,
        error_message=result.get("error"),
        sync_transaction_id=tx,
        source_instance_id=source_instance_id,
    )

    return {"ok": result.get("ok"), "message": result}


def _reject(doctype, name, tx, payload, code, message):
    create_sync_log(
        reference_doctype=doctype, reference_name=name,
        status="Failed", direction="Inbound",
        request_payload=payload,
        error_message=message,
        sync_transaction_id=tx,
    )
    frappe.throw(_(message), frappe.PermissionError)


# ---------------------------------------------------------------------------
# Manual trigger endpoint
# ---------------------------------------------------------------------------

@frappe.whitelist()
def manual_sync(doctype: str, docname: str):
    """
    Manually trigger an outbound sync for a single document.
    Called from the document toolbar or list view.
    """
    from neotec_dual_sync.api.events import _queue_if_matched

    settings = get_settings()
    if not settings.enabled:
        frappe.throw(_("Sync is disabled."))
    if settings.instance_role == "Target":
        frappe.throw(_("This instance is configured as Target-only and cannot push documents."))

    doc = frappe.get_doc(doctype, docname)
    _queue_if_matched(doc, "manual")
    return {"ok": True, "message": f"Document {doctype}/{docname} queued for sync."}


# ---------------------------------------------------------------------------
# Connection test (called from Settings form button)
# ---------------------------------------------------------------------------

@frappe.whitelist()
def validate_connection():
    """Test connectivity and auth to the remote instance."""
    settings = get_settings()
    result = test_remote_connection(settings)
    return result


# ---------------------------------------------------------------------------
# Dashboard stats (called from Dashboard doctype)
# ---------------------------------------------------------------------------

@frappe.whitelist()
def get_dashboard_stats():
    """Return summary statistics for the sync dashboard."""
    stats = {}

    for status in ("Queued", "Processing", "Success", "Failed", "Skipped",
                   "Loop Prevented", "Duplicate", "Received"):
        stats[status.lower().replace(" ", "_")] = frappe.db.count(
            "Neotec Sync Log", {"status": status}
        )

    stats["open_conflicts"] = frappe.db.count("Neotec Sync Conflict", {"status": "Open"})
    stats["idempotency_records"] = frappe.db.count("Neotec Sync Idempotency Log")

    # Last 24h throughput
    from frappe.utils import add_days, today
    yesterday = add_days(today(), -1)
    stats["synced_last_24h"] = frappe.db.count(
        "Neotec Sync Log",
        {"status": "Success", "creation": (">", yesterday)},
    )
    stats["failed_last_24h"] = frappe.db.count(
        "Neotec Sync Log",
        {"status": "Failed", "creation": (">", yesterday)},
    )

    return stats


# ---------------------------------------------------------------------------
# Conflict resolution actions
# ---------------------------------------------------------------------------

@frappe.whitelist()
def resolve_conflict(conflict_name: str, action: str):
    """
    action: 'accept_incoming' | 'keep_existing' | 'ignore'
    """
    conflict = frappe.get_doc("Neotec Sync Conflict", conflict_name)

    if action == "accept_incoming":
        try:
            incoming = json.loads(conflict.payload_after or "{}")
            if incoming:
                target_dt = conflict.reference_doctype
                target_name = conflict.reference_name
                doc = frappe.get_doc(target_dt, target_name)
                for k, v in incoming.items():
                    if k not in ("doctype", "name", "creation", "modified"):
                        setattr(doc, k, v)
                doc.save(ignore_permissions=True)
                frappe.db.commit()
        except Exception:
            frappe.log_error(title="Neotec Sync: Conflict Accept Error",
                             message=traceback.format_exc())
            frappe.throw(_("Failed to apply incoming changes. Check error log."))
        conflict.status = "Resolved"

    elif action == "keep_existing":
        conflict.status = "Resolved"

    elif action == "ignore":
        conflict.status = "Ignored"

    else:
        frappe.throw(_(f"Unknown conflict action: {action}"))

    conflict.save(ignore_permissions=True)
    frappe.db.commit()
    return {"ok": True, "status": conflict.status}
