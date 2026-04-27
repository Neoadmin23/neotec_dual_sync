// Neotec Sync Batch — Form JS
frappe.ui.form.on('Neotec Sync Batch', {
    refresh(frm) {
        if (frm.doc.status !== 'Running') {
            frm.add_custom_button(__('Run Now'), function () {
                frappe.confirm(
                    __('Manually process the batch queue right now?'),
                    function () {
                        frappe.call({
                            method: 'neotec_dual_sync.api.jobs.process_batch_queue',
                            callback() {
                                frappe.show_alert({ message: __('Batch queue processed'), indicator: 'green' });
                            }
                        });
                    }
                );
            });
        }
    }
});
