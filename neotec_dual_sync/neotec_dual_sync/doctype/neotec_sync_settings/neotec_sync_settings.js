frappe.ui.form.on('Neotec Sync Settings', {
    refresh(frm) {
        frm.add_custom_button('Test Sync', () => {
            frappe.call({
                method: 'neotec_dual_sync.api.sync.run_full_sync',
                callback: function(r) {
                    frappe.msgprint(r.message);
                    frm.reload_doc();
                }
            });
        });
    }
});