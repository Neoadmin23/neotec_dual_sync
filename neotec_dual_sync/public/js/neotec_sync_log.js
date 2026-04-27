// Neotec Sync Log — Form JS: manual re-queue and document links
frappe.ui.form.on('Neotec Sync Log', {

    refresh(frm) {
        // Manual re-queue for failed/skipped logs
        if (['Failed', 'Skipped'].includes(frm.doc.status) && frm.doc.direction === 'Outbound') {
            frm.add_custom_button(__('Re-queue'), function () {
                frappe.call({
                    method: 'frappe.client.set_value',
                    args: {
                        doctype: 'Neotec Sync Log',
                        name: frm.doc.name,
                        fieldname: { status: 'Queued', retry_count: 0 }
                    },
                    callback() {
                        frappe.show_alert({ message: __('Log re-queued'), indicator: 'green' });
                        frm.reload_doc();
                    }
                });
            });
        }

        // Link to referenced document
        if (frm.doc.reference_doctype && frm.doc.reference_name) {
            frm.add_custom_button(__('Open Source Document'), function () {
                frappe.set_route('Form', frm.doc.reference_doctype, frm.doc.reference_name);
            });
        }

        // Color the status badge
        const color_map = {
            'Success': 'green', 'Failed': 'red', 'Queued': 'yellow',
            'Processing': 'blue', 'Skipped': 'grey', 'Loop Prevented': 'orange',
            'Duplicate': 'grey', 'Received': 'blue'
        };
        const color = color_map[frm.doc.status] || 'grey';
        frm.set_indicator_formatter && frm.set_indicator_formatter('status', () => color);
    }
});
