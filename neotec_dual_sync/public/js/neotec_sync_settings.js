// Neotec Sync Settings — Form JS
frappe.ui.form.on('Neotec Sync Settings', {

    onload(frm) {
        bind_rule_queries(frm);
    },

    refresh(frm) {
        bind_rule_queries(frm);

        // ── Test Connection button ──────────────────────────────────────────
        frm.add_custom_button(__('Test Connection'), function () {
            frappe.show_alert({ message: __('Testing connection…'), indicator: 'blue' });
            frappe.call({
                method: 'neotec_dual_sync.api.validate_connection',
                callback(r) {
                    if (r.message && r.message.ok) {
                        frappe.show_alert({ message: __('✔ ') + r.message.message, indicator: 'green' });
                    } else {
                        const msg = (r.message && r.message.message) || __('Connection failed');
                        frappe.show_alert({ message: __('✘ ') + msg, indicator: 'red' });
                    }
                }
            });
        }, __('Actions'));

        // ── Generate Local Instance ID ──────────────────────────────────────
        if (!frm.doc.local_instance_id) {
            frm.add_custom_button(__('Generate Instance ID'), function () {
                frm.set_value('local_instance_id', frappe.utils.get_random(16));
                frm.save();
            }, __('Actions'));
        }

        // ── View Sync Logs ──────────────────────────────────────────────────
        frm.add_custom_button(__('View Sync Logs'), function () {
            frappe.set_route('List', 'Neotec Sync Log', {});
        }, __('Explore'));

        frm.add_custom_button(__('Open Conflicts'), function () {
            frappe.set_route('List', 'Neotec Sync Conflict', { status: 'Open' });
        }, __('Explore'));

        frm.add_custom_button(__('Idempotency Log'), function () {
            frappe.set_route('List', 'Neotec Sync Idempotency Log', {});
        }, __('Explore'));

        // ── Live dashboard stats ────────────────────────────────────────────
        frappe.call({
            method: 'neotec_dual_sync.api.get_dashboard_stats',
            callback(r) {
                if (!r.message) return;
                const s = r.message;
                const html = `
                  <div style="display:flex;gap:18px;flex-wrap:wrap;padding:10px 0">
                    ${stat_badge('Queued',    s.queued,          '#f59e0b')}
                    ${stat_badge('Success',   s.success,         '#10b981')}
                    ${stat_badge('Failed',    s.failed,          '#ef4444')}
                    ${stat_badge('Received',  s.received,        '#3b82f6')}
                    ${stat_badge('Conflicts', s.open_conflicts,  '#8b5cf6')}
                    ${stat_badge('24h Sent',  s.synced_last_24h, '#10b981')}
                    ${stat_badge('24h Failed',s.failed_last_24h, '#ef4444')}
                  </div>`;
                frm.set_intro(html, false);
            }
        });
    },

    instance_role(frm) {
        // Show/hide outbound fields based on role
        const is_source_or_both = ['Source', 'Both'].includes(frm.doc.instance_role);
        frm.toggle_display('remote_base_url',   is_source_or_both);
        frm.toggle_display('api_key',           is_source_or_both);
        frm.toggle_display('api_secret',        is_source_or_both);
        frm.toggle_display('shared_secret',     is_source_or_both);
        frm.toggle_display('signature_required',is_source_or_both);
        frm.toggle_display('allow_outbound_sync', is_source_or_both);
    }
});

frappe.ui.form.on('Neotec Sync Rule', {
    source_doctype(frm, cdt, cdn) {
        const row = locals[cdt][cdn];
        if (row.source_doctype && !row.target_doctype) {
            frappe.model.set_value(cdt, cdn, 'target_doctype', row.source_doctype);
        }
        if (!row.trigger_mode && frm.doc.default_trigger_mode) {
            frappe.model.set_value(cdt, cdn, 'trigger_mode', frm.doc.default_trigger_mode);
        }
    }
});

function bind_rule_queries(frm) {
    if (!frm.fields_dict.rules || !frm.fields_dict.rules.grid) return;
    const grid = frm.fields_dict.rules.grid;
    const dt_query = () => ({ filters: { istable: 0 } });
    ['source_doctype', 'target_doctype'].forEach(f => {
        if (grid.get_field(f)) grid.get_field(f).get_query = dt_query;
    });
    frm.refresh_field('rules');
}

function stat_badge(label, value, color) {
    return `<div style="background:#f3f4f6;border-radius:8px;padding:8px 14px;text-align:center;min-width:90px">
      <div style="font-size:22px;font-weight:700;color:${color}">${value ?? 0}</div>
      <div style="font-size:11px;color:#6b7280">${label}</div>
    </div>`;
}
