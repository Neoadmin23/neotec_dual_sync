import frappe
import requests
from frappe.utils import now

@frappe.whitelist()
def run_full_sync():
    settings = frappe.get_single("Neotec Sync Settings")

    try:
        update_status("Running")

        total_synced = 0

        for rule in settings.rules:
            print("Processing Rule:", rule.source_doctype)

            count = sync_doctype(rule)
            print("Count:", count)

            total_synced += count

        update_status("Success", total_synced, "Sync completed successfully")

        return str(f"Synced {total_synced} records")

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Neotec Sync Failed")
        update_status("Failed", 0, str(e))
        return str(e)


def sync_doctype(rule):
    doctype = rule.source_doctype

    if not doctype:
        return 0

    docs = frappe.get_all(
        doctype,
        fields=["name"],
        limit=50
    )

    count = 0

    for d in docs:
        doc = frappe.get_doc(doctype, d.name)

        # payload = frappe.as_json(doc.as_dict())
        cleaned = clean_doc(doc)
        payload = frappe.as_json(cleaned)

        # send_to_remote(payload)
        count += 1

    return count


def send_to_remote(data):
    settings = frappe.get_single("Neotec Sync Settings")

    url = f"{settings.remote_base_url}/api/method/neotec_dual_sync.api.sync.receive"

    headers = {
        "Authorization": f"token {settings.api_key}:{settings.api_secret}",
        "Content-Type": "application/json"
    }

    response = requests.post(
        url,
        data=data,
        headers=headers,
        timeout=settings.timeout_seconds or 30,
        verify=settings.verify_ssl
    )

    if response.status_code != 200:
        raise Exception(f"API Error: {response.text}")

@frappe.whitelist(allow_guest=True)
def receive():
    try:
        data = frappe.request.json

        if not data:
            return "No Data Received"

        doctype = data.get("doctype")
        name = data.get("name")

        if not doctype:
            return "Missing Doctype"

        if frappe.db.exists(doctype, name):
            doc = frappe.get_doc(doctype, name)
            doc.update(data)
            doc.save(ignore_permissions=True)
        else:
            doc = frappe.get_doc(data)
            doc.insert(ignore_permissions=True)

        return "OK"

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Neotec Receive Error")
        return str(e)

def update_status(status, count=0, message=""):
    settings = frappe.get_single("Neotec Sync Settings")

    settings.last_sync_status = status
    settings.last_sync_time = now()
    settings.records_synced = count
    settings.last_sync_message = message

    settings.save(ignore_permissions=True)

def clean_doc(doc):
    data = doc.as_dict()

    # Remove problematic/internal fields
    data.pop("_meta", None)
    data.pop("_user_tags", None)
    data.pop("__last_sync_on", None)

    # Convert datetime fields to string
    for key, value in data.items():
        if hasattr(value, "isoformat"):
            data[key] = value.isoformat()

    return data