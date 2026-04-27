// Neotec Sync Conflict — Form JS: conflict resolution UI
frappe.ui.form.on('Neotec Sync Conflict', {

    refresh(frm) {
        if (frm.doc.status !== 'Open') return;

        // Accept incoming data button
        frm.add_custom_button(__('Accept Incoming'), function () {
            frappe.confirm(
                __('Apply the incoming document data to the existing record? This will overwrite the current values.'),
                function () {
                    resolve_conflict(frm, 'accept_incoming');
                }
            );
        }, __('Resolve'));

        // Keep existing data button
        frm.add_custom_button(__('Keep Existing'), function () {
            frappe.confirm(
                __('Mark this conflict as resolved by keeping the current document unchanged?'),
                function () {
                    resolve_conflict(frm, 'keep_existing');
                }
            );
        }, __('Resolve'));

        // Ignore button
        frm.add_custom_button(__('Ignore'), function () {
            resolve_conflict(frm, 'ignore');
        }, __('Resolve'));

        // Open the referenced document
        if (frm.doc.reference_doctype && frm.doc.reference_name) {
            frm.add_custom_button(__('Open Document'), function () {
                frappe.set_route('Form', frm.doc.reference_doctype, frm.doc.reference_name);
            });
        }

        frm.set_intro(__('This conflict requires manual resolution. Compare the payloads below and choose an action.'), 'orange');
    }
});

function resolve_conflict(frm, action) {
    frappe.show_alert({ message: __('Resolving…'), indicator: 'blue' });
    frappe.call({
        method: 'neotec_dual_sync.api.resolve_conflict',
        args: { conflict_name: frm.doc.name, action: action },
        callback(r) {
            if (r.message && r.message.ok) {
                frappe.show_alert({ message: __('Conflict resolved: ') + r.message.status, indicator: 'green' });
                frm.reload_doc();
            } else {
                frappe.show_alert({ message: __('Resolution failed. Check error log.'), indicator: 'red' });
            }
        }
    });
}
