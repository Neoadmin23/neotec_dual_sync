"""
Neotec Dual Sync — Core Services
Shared utilities: settings, hashing, logging, mapping engine, HTTP push, inbound apply.
"""

import hashlib
import hmac
import json
import traceback

import frappe
import requests
from frappe import _
from frappe.utils import now_datetime, cstr


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------

def get_settings():
    return frappe.get_single("Neotec Sync Settings")


# ---------------------------------------------------------------------------
# Payload hashing & HMAC
# ---------------------------------------------------------------------------

def payload_hash(payload: dict) -> str:
    serialised = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


def build_hmac_signature(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def verify_hmac_signature(body: bytes, secret: str, provided_sig: str) -> bool:
    expected = build_hmac_signature(body, secret)
    return hmac.compare_digest(expected, provided_sig or "")


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _mask_payload(payload_str: str, settings) -> str:
    if not getattr(settings, "mask_secrets_in_logs", 1):
        return payload_str
    import re
    for field in ("api_secret", "shared_secret", "password", "token"):
        payload_str = re.sub(
            r'("' + field + r'"\s*:\s*)"[^"]*"',
            r'\1"***"',
            payload_str,
            flags=re.IGNORECASE,
        )
    return payload_str


def create_sync_log(
    reference_doctype=None,
    reference_name=None,
    event_name=None,
    direction="Outbound",
    status="Queued",
    sync_transaction_id=None,
    request_payload=None,
    response_payload=None,
    error_message=None,
    retry_count=0,
    rule_name=None,
    source_instance_id=None,
):
    settings = get_settings()
    if isinstance(request_payload, dict):
        request_payload = json.dumps(request_payload, indent=2, default=str)
        request_payload = _mask_payload(request_payload, settings)
    if isinstance(response_payload, dict):
        response_payload = json.dumps(response_payload, indent=2, default=str)

    doc = frappe.get_doc({
        "doctype": "Neotec Sync Log",
        "reference_doctype": reference_doctype,
        "reference_name": reference_name,
        "event_name": event_name,
        "direction": direction,
        "status": status,
        "sync_transaction_id": sync_transaction_id or frappe.generate_hash(length=20),
        "request_payload": request_payload,
        "response_payload": response_payload,
        "error_message": error_message,
        "retry_count": retry_count,
        "rule_name": rule_name,
        "source_instance_id": source_instance_id,
    })
    doc.insert(ignore_permissions=True)
    frappe.db.commit()
    return doc


def update_sync_log(log_name: str, **kwargs):
    if not log_name:
        return
    doc = frappe.get_doc("Neotec Sync Log", log_name)
    for k, v in kwargs.items():
        if isinstance(v, dict):
            v = json.dumps(v, indent=2, default=str)
        setattr(doc, k, v)
    doc.save(ignore_permissions=True)
    frappe.db.commit()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def register_idempotency(
    source_instance, source_doctype, source_name,
    transaction_id, hash_value, target_name=None, status="Processed"
):
    existing = (
        frappe.db.get_value("Neotec Sync Idempotency Log",
                            {"sync_transaction_id": transaction_id}, "name")
        or frappe.db.get_value("Neotec Sync Idempotency Log",
                               {"source_instance_id": source_instance,
                                "source_doctype": source_doctype,
                                "source_docname": source_name}, "name")
    )
    if existing:
        return existing, True

    doc = frappe.get_doc({
        "doctype": "Neotec Sync Idempotency Log",
        "source_instance_id": source_instance,
        "source_doctype": source_doctype,
        "source_docname": source_name,
        "sync_transaction_id": transaction_id,
        "payload_hash": hash_value,
        "target_docname": target_name,
        "status": status,
        "processed_on": now_datetime(),
    })
    doc.insert(ignore_permissions=True)
    frappe.db.commit()
    return doc.name, False


# ---------------------------------------------------------------------------
# Loop / hop detection
# ---------------------------------------------------------------------------

def should_block_loop(sync_meta: dict, local_instance_id: str):
    if not sync_meta:
        return False, ""
    settings = get_settings()
    max_hops = int(getattr(settings, "max_hop_count", 5) or 5)
    route = sync_meta.get("route_trace") or []
    hop_count = int(sync_meta.get("hop_count") or 0)
    if local_instance_id and local_instance_id in route:
        return True, f"Loop: local instance already in route trace {route}"
    if hop_count >= max_hops:
        return True, f"Hop count {hop_count} reached max {max_hops}"
    return False, ""


def append_route_trace(sync_meta: dict, local_instance_id: str) -> dict:
    meta = dict(sync_meta or {})
    trace = list(meta.get("route_trace") or [])
    trace.append(local_instance_id)
    meta["route_trace"] = trace
    meta["hop_count"] = int(meta.get("hop_count") or 0) + 1
    return meta


# ---------------------------------------------------------------------------
# IP allow-list
# ---------------------------------------------------------------------------

def check_ip_allowlist(instance_doc):
    if not instance_doc or not getattr(instance_doc, "allowed_ip_list", None):
        return
    allowed = [ip.strip() for ip in instance_doc.allowed_ip_list.splitlines() if ip.strip()]
    if not allowed:
        return
    remote_ip = (
        frappe.local.request.environ.get("HTTP_X_FORWARDED_FOR", "").split(",")[0].strip()
        or frappe.local.request.environ.get("REMOTE_ADDR", "")
    )
    if remote_ip not in allowed:
        frappe.throw(_(f"Request from IP {remote_ip} is not allowed."), frappe.PermissionError)


# ---------------------------------------------------------------------------
# Field mapping engine
# ---------------------------------------------------------------------------

def apply_mapping(source_doc_dict: dict, mapping_doc) -> dict:
    target = {}
    if not mapping_doc or not mapping_doc.field_mappings:
        return dict(source_doc_dict)

    for row in mapping_doc.field_mappings:
        mtype = row.mapping_type or "Direct"
        if mtype == "Ignore":
            continue
        source_val = source_doc_dict.get(row.source_field)
        if mtype == "Static Value":
            val = row.default_value
        elif mtype == "Scripted Transform":
            val = _run_transform_script(row.transform_script, source_val, source_doc_dict)
        else:
            val = source_val
            if val is None and row.default_value not in (None, ""):
                val = row.default_value
        if val is not None and row.target_data_type:
            val = _coerce_type(val, row.target_data_type)
        if row.required_in_target and (val is None or val == ""):
            frappe.throw(_(f"Required field '{row.target_field}' is empty after mapping."))
        target[row.target_field] = val

    for child_row in (mapping_doc.child_table_mappings or []):
        source_table = source_doc_dict.get(child_row.source_table_field) or []
        child_mapping = None
        if getattr(child_row, "row_mappings", None):
            try:
                child_mapping = frappe.get_doc("Neotec Sync Mapping", child_row.row_mappings)
            except Exception:
                pass
        mapped_children = []
        for item in source_table:
            mapped_children.append(apply_mapping(item, child_mapping) if child_mapping else dict(item))
        target[child_row.target_table_field] = mapped_children

    return target


def _run_transform_script(script: str, value, source_doc: dict):
    if not script:
        return value
    try:
        local_vars = {"value": value, "source": source_doc, "result": value}
        frappe.safe_exec(script, _locals=local_vars)
        return local_vars.get("result", value)
    except Exception:
        frappe.log_error(title="Neotec Sync: Transform Error", message=traceback.format_exc())
        return value


def _coerce_type(val, target_type: str):
    try:
        if target_type == "Int":
            return int(float(cstr(val)))
        if target_type in ("Float", "Currency"):
            return float(cstr(val))
        if target_type == "Check":
            return 1 if cstr(val).lower() in ("1", "true", "yes") else 0
        if target_type == "JSON":
            return json.loads(val) if isinstance(val, str) else val
        return val
    except Exception:
        return val


# ---------------------------------------------------------------------------
# Audit snapshot
# ---------------------------------------------------------------------------

def capture_audit_snapshot(doc_dict: dict, settings) -> str:
    if not getattr(settings, "enable_audit_snapshot", 0):
        return None
    return json.dumps(doc_dict, indent=2, default=str)


# ---------------------------------------------------------------------------
# Outbound HTTP push
# ---------------------------------------------------------------------------

def push_document_to_remote(doc, rule, settings, sync_transaction_id: str, sync_meta: dict = None) -> dict:
    if not settings.remote_base_url:
        return {"ok": False, "error": "remote_base_url is not configured"}

    doc_dict = doc.as_dict() if hasattr(doc, "as_dict") else dict(doc)

    mapping_doc = None
    if getattr(rule, "mapping_profile", None):
        try:
            mapping_doc = frappe.get_doc("Neotec Sync Mapping", rule.mapping_profile)
        except Exception:
            pass

    mapped_dict = apply_mapping(doc_dict, mapping_doc) if (mapping_doc and mapping_doc.active) else dict(doc_dict)

    for key in ("__islocal", "__unsaved", "__last_sync_on"):
        mapped_dict.pop(key, None)
    mapped_dict["nxd_received_from_remote"] = 1

    updated_meta = append_route_trace(sync_meta or {}, settings.local_instance_id)

    payload = {
        "source_instance_id": settings.local_instance_id,
        "source_doctype": doc_dict.get("doctype"),
        "source_docname": doc_dict.get("name"),
        "target_doctype": rule.target_doctype,
        "sync_transaction_id": sync_transaction_id,
        "sync_meta": updated_meta,
        "document": mapped_dict,
    }

    snapshot = capture_audit_snapshot(doc_dict, settings)

    url = settings.remote_base_url.rstrip("/") + "/api/method/neotec_dual_sync.api.receive_document"
    body_bytes = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "X-Neotec-Source": settings.local_instance_id or "",
        "X-Neotec-Transaction": sync_transaction_id,
        "Authorization": "token {}:{}".format(
            settings.api_key or "",
            settings.get_password("api_secret") or "",
        ),
    }

    if settings.signature_required and settings.shared_secret:
        sig = build_hmac_signature(body_bytes, settings.get_password("shared_secret"))
        headers["X-Neotec-Signature"] = sig

    if settings.dry_run_mode:
        return {"ok": True, "dry_run": True, "payload": payload, "snapshot": snapshot}

    try:
        resp = requests.post(
            url, data=body_bytes, headers=headers,
            verify=bool(settings.verify_ssl),
            timeout=int(settings.timeout_seconds or 30),
        )
        resp_json = {}
        try:
            resp_json = resp.json()
        except Exception:
            pass

        # Frappe wraps responses in {"message": {...}}
        inner = resp_json.get("message") or resp_json
        ok = resp.status_code in (200, 201) and inner.get("ok", True) is not False
        return {
            "ok": ok,
            "status_code": resp.status_code,
            "response": resp_json,
            "snapshot": snapshot,
            "error": None if ok else inner.get("message", f"HTTP {resp.status_code}"),
        }
    except requests.exceptions.SSLError as e:
        return {"ok": False, "error": f"SSL Error: {e}"}
    except requests.exceptions.ConnectionError as e:
        return {"ok": False, "error": f"Connection Error: {e}"}
    except requests.exceptions.Timeout:
        return {"ok": False, "error": f"Timeout after {settings.timeout_seconds}s"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Inbound document application
# ---------------------------------------------------------------------------

def apply_inbound_document(payload: dict, settings) -> dict:
    source_doctype = payload.get("source_doctype")
    target_doctype = payload.get("target_doctype") or source_doctype
    source_name = payload.get("source_docname")
    document = payload.get("document") or {}

    if not target_doctype or not document:
        return {"ok": False, "error": "Missing target_doctype or document in payload"}

    rule = _find_matching_rule(source_doctype, settings)
    duplicate_policy = "Skip If Unchanged"
    mapping_doc = None

    if rule:
        duplicate_policy = getattr(rule, "duplicate_policy", "Skip If Unchanged") or "Skip If Unchanged"
        if getattr(rule, "mapping_profile", None):
            try:
                mapping_doc = frappe.get_doc("Neotec Sync Mapping", rule.mapping_profile)
            except Exception:
                pass

    mapped_doc = apply_mapping(document, mapping_doc) if (mapping_doc and mapping_doc.active) else dict(document)

    for key in ("creation", "modified", "modified_by", "owner",
                "docstatus", "__islocal", "nxd_received_from_remote"):
        mapped_doc.pop(key, None)

    mapped_doc["doctype"] = target_doctype

    existing_name = (
        frappe.db.get_value(target_doctype, {"name": source_name})
        or frappe.db.get_value(target_doctype, {"nxd_source_name": source_name})
    )

    if existing_name:
        return _handle_existing_document(
            existing_name, target_doctype, mapped_doc, document,
            duplicate_policy, source_name, payload,
        )
    return _insert_new_document(target_doctype, mapped_doc, source_name)


def _find_matching_rule(source_doctype: str, settings):
    for row in (getattr(settings, "rules", None) or []):
        if getattr(row, "enabled", 1) and row.source_doctype == source_doctype:
            return row
    return None


def _insert_new_document(target_doctype: str, doc_dict: dict, source_name: str) -> dict:
    try:
        doc_dict["nxd_source_name"] = source_name
        new_doc = frappe.get_doc(doc_dict)
        new_doc.insert(ignore_permissions=True, ignore_mandatory=False)
        frappe.db.commit()
        return {"ok": True, "action": "inserted", "target_name": new_doc.name}
    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(title="Neotec Sync: Insert Failed", message=traceback.format_exc())
        return {"ok": False, "action": "insert_failed", "error": str(e)}


def _handle_existing_document(
    existing_name, target_doctype, mapped_doc, original_doc,
    duplicate_policy, source_name, full_payload,
) -> dict:
    if duplicate_policy == "Reject Duplicate":
        return {"ok": False, "action": "rejected_duplicate", "target_name": existing_name,
                "error": "Duplicate — policy is Reject"}

    if duplicate_policy == "Skip If Unchanged":
        existing_hash = _doc_content_hash(target_doctype, existing_name)
        incoming_hash = payload_hash(mapped_doc)
        if existing_hash == incoming_hash:
            return {"ok": True, "action": "skipped_unchanged", "target_name": existing_name}

    if duplicate_policy == "Create Conflict Record":
        _create_conflict_record(
            target_doctype, existing_name,
            full_payload.get("source_instance_id"),
            "Conflict on inbound sync: document already exists",
            original_doc, mapped_doc,
        )
        return {"ok": True, "action": "conflict_created", "target_name": existing_name}

    try:
        existing = frappe.get_doc(target_doctype, existing_name)
        for k, v in mapped_doc.items():
            if k != "doctype":
                setattr(existing, k, v)
        existing.save(ignore_permissions=True)
        frappe.db.commit()
        return {"ok": True, "action": "updated", "target_name": existing_name}
    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(title="Neotec Sync: Update Failed", message=traceback.format_exc())
        return {"ok": False, "action": "update_failed", "error": str(e)}


def _doc_content_hash(doctype: str, name: str) -> str:
    try:
        doc = frappe.get_doc(doctype, name)
        d = doc.as_dict()
        for k in ("modified", "modified_by", "creation"):
            d.pop(k, None)
        return payload_hash(d)
    except Exception:
        return ""


def _create_conflict_record(reference_doctype, reference_name, source_instance_id,
                             reason, payload_before, payload_after):
    try:
        frappe.get_doc({
            "doctype": "Neotec Sync Conflict",
            "reference_doctype": reference_doctype,
            "reference_name": reference_name,
            "source_instance_id": source_instance_id,
            "reason": reason,
            "payload_before": json.dumps(payload_before, indent=2, default=str),
            "payload_after": json.dumps(payload_after, indent=2, default=str),
            "status": "Open",
        }).insert(ignore_permissions=True)
        frappe.db.commit()
    except Exception:
        frappe.log_error(title="Neotec Sync: Conflict Record Error",
                         message=traceback.format_exc())


# ---------------------------------------------------------------------------
# Connection test
# ---------------------------------------------------------------------------

def test_remote_connection(settings) -> dict:
    import time
    if not settings.remote_base_url:
        return {"ok": False, "message": "remote_base_url is not configured"}
    url = settings.remote_base_url.rstrip("/") + "/api/method/frappe.ping"
    headers = {"Authorization": "token {}:{}".format(
        settings.api_key or "", settings.get_password("api_secret") or ""
    )}
    try:
        t0 = time.monotonic()
        resp = requests.get(url, headers=headers,
                            verify=bool(settings.verify_ssl),
                            timeout=int(settings.timeout_seconds or 10))
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        if resp.status_code == 200:
            return {"ok": True, "message": f"Connected ({latency_ms} ms)", "latency_ms": latency_ms}
        return {"ok": False, "message": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}
