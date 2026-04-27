"""
Neotec Dual Sync — Install / Migrate hooks.
Creates required roles and initialises the singleton Settings doc with safe defaults.
"""
import frappe


def ensure_role(role_name: str):
    if not frappe.db.exists("Role", role_name):
        frappe.get_doc({"doctype": "Role", "role_name": role_name}).insert(ignore_permissions=True)


def after_install():
    _setup_roles()
    _setup_settings()


def after_migrate():
    _setup_roles()
    _setup_settings()


def _setup_roles():
    for role in ("Neotec Dual Sync Manager", "Neotec Dual Sync User", "Neotec Dual Sync API"):
        ensure_role(role)


def _setup_settings():
    if frappe.db.exists("Neotec Sync Settings", "Neotec Sync Settings"):
        return  # Already initialised — don't overwrite user configuration

    doc = frappe.get_doc({
        "doctype": "Neotec Sync Settings",
        # Safe defaults — admin must explicitly enable
        "enabled": 0,
        "instance_role": "Source",
        "local_instance_id": frappe.generate_hash(length=12),
        "accept_inbound_sync": 1,
        "allow_outbound_sync": 1,
        "prevent_loopback": 1,
        "max_hop_count": 5,
        "verify_ssl": 1,
        "timeout_seconds": 30,
        "default_trigger_mode": "On Submit",
        "signature_required": 1,
        "batch_size": 50,
        "max_retries": 3,
        "retry_interval_minutes": 10,
        "enable_audit_snapshot": 1,
        "mask_secrets_in_logs": 1,
        "allow_delete_sync": 0,
        "dry_run_mode": 0,
        "log_level": "INFO",
    })
    doc.insert(ignore_permissions=True)
    frappe.db.commit()
