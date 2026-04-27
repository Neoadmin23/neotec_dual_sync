app_name = "neotec_dual_sync"
app_title = "Neotec Dual Sync"
app_publisher = "Neotec"
app_description = "Production-oriented configurable dual instance synchronization for Frappe/ERPNext"
app_email = "support@neotec.example"
app_license = "MIT"

after_install = "neotec_dual_sync.install.after_install"
after_migrate = "neotec_dual_sync.install.after_migrate"

fixtures = [
    {"dt": "Role", "filters": [["name", "in", [
        "Neotec Dual Sync Manager",
        "Neotec Dual Sync User",
        "Neotec Dual Sync API",
    ]]]}
]

doc_events = {
    "*": {
        "on_submit":              ["neotec_dual_sync.api.events.handle_on_submit"],
        "on_update_after_submit": ["neotec_dual_sync.api.events.handle_update_after_submit"],
        "after_insert":           ["neotec_dual_sync.api.events.handle_after_insert"],
        "on_update":              ["neotec_dual_sync.api.events.handle_on_update"],
    }
}

doctype_js = {
    "Neotec Sync Settings": "public/js/neotec_sync_settings.js",
    "Neotec Sync Batch":    "public/js/neotec_sync_batch.js",
    "Neotec Sync Conflict": "public/js/neotec_sync_conflict.js",
    "Neotec Sync Log":      "public/js/neotec_sync_log.js",
}

scheduler_events = {
    "cron": {
        "*/10 * * * *": [
            "neotec_dual_sync.api.jobs.process_batch_queue",
            "neotec_dual_sync.api.jobs.retry_failed_syncs",
        ],
        # Daily at 2 AM — log cleanup
        "0 2 * * *": [
            "neotec_dual_sync.api.jobs.cleanup_old_logs",
        ],
    }
}
