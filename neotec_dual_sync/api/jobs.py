"""
Neotec Dual Sync — Scheduled Jobs
Runs every 10 minutes via Frappe scheduler.
  - process_batch_queue: dispatch queued outbound sync logs
  - retry_failed_syncs:  retry failed logs within retry window
  - cleanup_old_logs:    purge old processed records (daily)
"""
import json
import traceback
from datetime import timedelta

import frappe
from frappe.utils import now_datetime, add_to_date

from neotec_dual_sync.api.services import (
    get_settings,
    push_document_to_remote,
    update_sync_log,
    create_sync_log,
)


def process_batch_queue():
    """
    Pick up all Queued outbound sync log entries and dispatch them
    to the remote instance. Respects batch_size setting.
    """
    settings = get_settings()
    if not settings.enabled or settings.instance_role == "Target":
        return

    if not settings.allow_outbound_sync:
        return

    batch_size = int(settings.batch_size or 50)

    queued = frappe.get_all(
        "Neotec Sync Log",
        filters={"status": "Queued", "direction": "Outbound"},
        fields=["name", "reference_doctype", "reference_name",
                "sync_transaction_id", "rule_name", "retry_count"],
        order_by="creation asc",
        limit=batch_size,
    )

    if not queued:
        return

    success_count = 0
    fail_count = 0

    for log_entry in queued:
        try:
            _dispatch_log_entry(log_entry, settings)
            success_count += 1
        except Exception:
            fail_count += 1
            frappe.log_error(
                title=f"Neotec Sync: Dispatch error for log {log_entry.name}",
                message=traceback.format_exc(),
            )
            update_sync_log(log_entry.name, status="Failed",
                            error_message=traceback.format_exc()[-500:])

    if settings.log_level == "DEBUG":
        frappe.log_error(
            title="Neotec Sync: Batch complete",
            message=f"Dispatched {success_count} OK, {fail_count} failed",
        )


def _dispatch_log_entry(log_entry: dict, settings):
    """Dispatch a single queued log entry to the remote instance."""
    doctype = log_entry.reference_doctype
    docname = log_entry.reference_name
    tx_id = log_entry.sync_transaction_id

    if not doctype or not docname:
        update_sync_log(log_entry.name, status="Skipped",
                        error_message="Missing reference_doctype or reference_name")
        return

    # Mark as Processing
    update_sync_log(log_entry.name, status="Processing")

    # Load the document
    try:
        doc = frappe.get_doc(doctype, docname)
    except frappe.DoesNotExistError:
        update_sync_log(log_entry.name, status="Skipped",
                        error_message=f"Document {doctype}/{docname} no longer exists")
        return

    # Find the matching rule
    rule = _find_rule_for_log(log_entry, settings)
    if not rule:
        update_sync_log(log_entry.name, status="Skipped",
                        error_message=f"No matching enabled rule for {doctype}")
        return

    # Build sync_meta for outbound
    sync_meta = {"route_trace": [], "hop_count": 0}

    result = push_document_to_remote(doc, rule, settings, tx_id, sync_meta)

    if result.get("dry_run"):
        update_sync_log(
            log_entry.name, status="Success",
            response_payload=result.get("payload"),
            error_message="[Dry Run — not actually sent]",
        )
        return

    if result.get("ok"):
        update_sync_log(
            log_entry.name, status="Success",
            response_payload=result.get("response"),
        )
    else:
        retry_count = int(log_entry.retry_count or 0) + 1
        max_retries = int(settings.max_retries or 3)
        new_status = "Failed" if retry_count > max_retries else "Queued"
        update_sync_log(
            log_entry.name,
            status="Failed",
            error_message=result.get("error", "Unknown error"),
            response_payload=result.get("response"),
            retry_count=retry_count,
        )


def _find_rule_for_log(log_entry: dict, settings):
    """Return the Sync Rule doc-row matching this log entry."""
    rule_name = log_entry.get("rule_name")
    doctype = log_entry.reference_doctype

    for row in (settings.rules or []):
        if not getattr(row, "enabled", 1):
            continue
        if rule_name and getattr(row, "name", None) == rule_name:
            return row
        if not rule_name and row.source_doctype == doctype:
            return row
    return None


def retry_failed_syncs():
    """
    Re-queue Failed sync log entries that are still within the retry window.
    Uses exponential-ish back-off based on retry_count * retry_interval_minutes.
    """
    settings = get_settings()
    if not settings.enabled or settings.instance_role == "Target":
        return

    max_retries = int(settings.max_retries or 3)
    interval_mins = int(settings.retry_interval_minutes or 10)

    failed_logs = frappe.get_all(
        "Neotec Sync Log",
        filters={
            "status": "Failed",
            "direction": "Outbound",
            "retry_count": ("<", max_retries),
        },
        fields=["name", "retry_count", "modified"],
        order_by="modified asc",
        limit=200,
    )

    now = now_datetime()
    requeued = 0

    for log in failed_logs:
        retry_count = int(log.retry_count or 0)
        # Back-off: each retry waits longer
        wait_minutes = interval_mins * (2 ** retry_count)
        retry_after = add_to_date(log.modified, minutes=wait_minutes)

        if now >= retry_after:
            update_sync_log(log.name, status="Queued")
            requeued += 1

    if requeued and settings.log_level == "DEBUG":
        frappe.log_error(
            title="Neotec Sync: Retry job",
            message=f"Re-queued {requeued} failed sync logs",
        )


def cleanup_old_logs():
    """
    Daily housekeeping: purge old Success/Skipped/Duplicate log entries
    older than 30 days to prevent unbounded table growth.
    Also purge Idempotency Logs older than 60 days.
    """
    settings = get_settings()

    cutoff_30 = add_to_date(now_datetime(), days=-30)
    cutoff_60 = add_to_date(now_datetime(), days=-60)

    # Purge non-critical sync logs
    frappe.db.delete(
        "Neotec Sync Log",
        {
            "status": ("in", ("Success", "Skipped", "Duplicate", "Loop Prevented")),
            "creation": ("<", cutoff_30),
        },
    )

    # Purge old idempotency records
    frappe.db.delete(
        "Neotec Sync Idempotency Log",
        {"creation": ("<", cutoff_60)},
    )

    frappe.db.commit()
