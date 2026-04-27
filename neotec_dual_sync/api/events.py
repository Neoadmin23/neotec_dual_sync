"""
Neotec Dual Sync — Document Event Handlers
Hooks into Frappe doc_events to queue outbound sync log entries.
"""
import frappe
from neotec_dual_sync.api.services import get_settings, create_sync_log

# DocTypes that should never trigger sync (would cause infinite loops or noise)
_EXCLUDED_DOCTYPES = frozenset({
    "Neotec Sync Log",
    "Neotec Sync Batch",
    "Neotec Sync Settings",
    "Neotec Sync Idempotency Log",
    "Neotec Sync Conflict",
    "Neotec Sync Rule",
    "Neotec Sync Mapping",
    "Neotec Sync Field Map",
    "Neotec Sync Mapping Row",
    "Neotec Sync Instance",
    "Neotec Sync Route Policy",
    "Neotec Sync API Key",
    "Neotec Sync Dashboard",
    "Error Log",
    "Activity Log",
    "Access Log",
    "Version",
})


def _queue_if_matched(doc, event_name: str):
    """
    Core dispatcher: checks settings and rules, then creates an outbound log entry.
    Returns the log doc if queued, else None.
    """
    if doc.doctype in _EXCLUDED_DOCTYPES:
        return None

    # Skip documents that arrived from remote (prevents loopback)
    if getattr(doc, "nxd_received_from_remote", 0):
        return None

    settings = get_settings()
    if not settings.enabled or settings.instance_role == "Target":
        return None
    if not settings.allow_outbound_sync:
        return None

    for row in (settings.rules or []):
        if not getattr(row, "enabled", 1):
            continue
        if row.source_doctype != doc.doctype:
            continue

        trigger = row.trigger_mode or "On Submit"

        # Manual trigger bypasses trigger_mode check
        if event_name == "manual":
            pass
        elif trigger == "On Submit" and event_name not in ("on_submit", "on_update_after_submit"):
            continue
        elif trigger == "Batch" and event_name not in ("after_insert", "on_update"):
            continue
        elif trigger == "Both" and event_name not in (
            "on_submit", "on_update_after_submit", "after_insert", "on_update", "manual"
        ):
            continue
        elif trigger == "Manual" and event_name != "manual":
            continue

        # Evaluate condition script if present
        if getattr(row, "condition_script", None):
            if not _eval_condition(row.condition_script, doc):
                continue

        tx_id = frappe.generate_hash(length=20)
        log = create_sync_log(
            reference_doctype=doc.doctype,
            reference_name=doc.name,
            event_name=event_name,
            status="Queued",
            direction="Outbound",
            sync_transaction_id=tx_id,
            rule_name=getattr(row, "name", None),
        )
        return log

    return None


def _eval_condition(script: str, doc) -> bool:
    """Evaluate a Python condition script using Frappe's safe_exec. Returns bool."""
    try:
        local_vars = {"doc": doc, "result": True}
        frappe.safe_exec(script, _locals=local_vars)
        return bool(local_vars.get("result", True))
    except Exception:
        frappe.log_error(
            title="Neotec Sync: Condition Script Error",
            message=frappe.get_traceback(),
        )
        return False  # Fail closed: if condition errors, don't sync


# ---------------------------------------------------------------------------
# Frappe doc_event hooks
# ---------------------------------------------------------------------------

def handle_on_submit(doc, method=None):
    _queue_if_matched(doc, "on_submit")


def handle_update_after_submit(doc, method=None):
    _queue_if_matched(doc, "on_update_after_submit")


def handle_after_insert(doc, method=None):
    _queue_if_matched(doc, "after_insert")


def handle_on_update(doc, method=None):
    _queue_if_matched(doc, "on_update")
