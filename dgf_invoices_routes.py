import os
import sqlite3
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from dgf_invoice_tables import DGFInvoiceTables
from dgf_invoice_processor import DGFInvoiceProcessor


dgf_invoices_bp = Blueprint('dgf_invoices', __name__, url_prefix='/dgf-invoices')

# Ensure tables exist
DGFInvoiceTables()

@dgf_invoices_bp.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        uploaded_by = request.form.get('uploaded_by') or 'JOE XIE'
        replace_existing = request.form.get('replace_existing') == 'on'
        file = request.files.get('invoice_file')
        if not file or file.filename == '':
            flash('Please select an Excel file to upload.', 'error')
            return redirect(request.url)
        tmp_path = os.path.join('uploads', 'tmp', file.filename)
        os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
        file.save(tmp_path)

        proc = DGFInvoiceProcessor()
        mode = (request.form.get('mode') or 'air').lower()
        if mode == 'fcl':
            res = proc.process_fcl_invoice_file(tmp_path, uploaded_by, replace_existing)
        elif mode == 'lcl':
            res = proc.process_lcl_invoice_file(tmp_path, uploaded_by, replace_existing)
        else:
            res = proc.process_air_invoice_file(tmp_path, uploaded_by, replace_existing)
        flash(f"Uploaded: {res['success']} rows, Errors: {res['errors']}", 'info')
        if res.get('errors'):
            # Show up to 5 detailed error messages
            msgs = res.get('messages') or []
            for m in msgs[:5]:
                flash(m, 'error')
        return redirect(url_for('dgf_invoices.air_invoices'))

    return render_template('dgf_invoices/upload.html')

@dgf_invoices_bp.route('/air')
def air_invoices():
    status = request.args.get('status', '')
    quote = request.args.get('quote', '')
    hbl = request.args.get('hbl', '')

    conn = sqlite3.connect('dhl_audit.db')
    cur = conn.cursor()
    where = ['1=1']
    params = []
    if status:
        where.append('status = ?')
        params.append(status)
    if quote:
        where.append('quote_id LIKE ?')
        params.append(f"%{quote}%")
    if hbl:
        where.append('hbl_number LIKE ?')
        params.append(f"%{hbl}%")

    q = f'''SELECT lane_id, quote_id, hbl_number, actual_arrival_date, origin_country, origin_port,
                   pieces, gross_weight, chargeable_weight, terms, freight,
                   origin_currency, origin_fx_rate, destination_charges,
                   destination_currency, destination_fx_rate, total_cny,
                   status, uploaded_at
            FROM dgf_air_invoices
            WHERE {' AND '.join(where)}
            ORDER BY uploaded_at DESC
            LIMIT 200'''
    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    # counts
    conn = sqlite3.connect('dhl_audit.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM dgf_air_invoices WHERE status='ACTIVE'")
    active_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dgf_air_invoices")
    total_count = c.fetchone()[0]
    conn.close()

    return render_template('dgf_invoices/air_invoices.html', invoices=rows, active_count=active_count, total_count=total_count, filters={'status': status, 'quote': quote, 'hbl': hbl})

@dgf_invoices_bp.route('/fcl')
def fcl_invoices():
    status = request.args.get('status', '')
    quote = request.args.get('quote', '')
    hbl = request.args.get('hbl', '')

    conn = sqlite3.connect('dhl_audit.db')
    cur = conn.cursor()
    where = ['1=1']
    params = []
    if status:
        where.append('status = ?')
        params.append(status)
    if quote:
        where.append('quote_id LIKE ?')
        params.append(f"%{quote}%")
    if hbl:
        where.append('hbl_number LIKE ?')
        params.append(f"%{hbl}%")

    q = f'''SELECT lane_id, quote_id, hbl_number, actual_arrival_date, origin_country, origin_port,
                   pieces, gross_weight, chargeable_weight, terms, freight,
                   origin_currency, origin_fx_rate, destination_charges,
                   destination_currency, destination_fx_rate, total_cny,
                   status, uploaded_at
            FROM dgf_fcl_invoices
            WHERE {' AND '.join(where)}
            ORDER BY uploaded_at DESC
            LIMIT 200'''
    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    conn = sqlite3.connect('dhl_audit.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM dgf_fcl_invoices WHERE status='ACTIVE'")
    active_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dgf_fcl_invoices")
    total_count = c.fetchone()[0]
    conn.close()

    return render_template('dgf_invoices/sea_invoices.html', title='DGF FCL Invoices', invoices=rows, active_count=active_count, total_count=total_count, filters={'status': status, 'quote': quote, 'hbl': hbl})

@dgf_invoices_bp.route('/lcl')
def lcl_invoices():
    status = request.args.get('status', '')
    quote = request.args.get('quote', '')
    hbl = request.args.get('hbl', '')

    conn = sqlite3.connect('dhl_audit.db')
    cur = conn.cursor()
    where = ['1=1']
    params = []
    if status:
        where.append('status = ?')
        params.append(status)
    if quote:
        where.append('quote_id LIKE ?')
        params.append(f"%{quote}%")
    if hbl:
        where.append('hbl_number LIKE ?')
        params.append(f"%{hbl}%")

    q = f'''SELECT lane_id, quote_id, hbl_number, actual_arrival_date, origin_country, origin_port,
                   pieces, gross_weight, chargeable_weight, terms, freight,
                   origin_currency, origin_fx_rate, destination_charges,
                   destination_currency, destination_fx_rate, total_cny,
                   status, uploaded_at
            FROM dgf_lcl_invoices
            WHERE {' AND '.join(where)}
            ORDER BY uploaded_at DESC
            LIMIT 200'''
    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    conn = sqlite3.connect('dhl_audit.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM dgf_lcl_invoices WHERE status='ACTIVE'")
    active_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dgf_lcl_invoices")
    total_count = c.fetchone()[0]
    conn.close()

    return render_template('dgf_invoices/sea_invoices.html', title='DGF LCL Invoices', invoices=rows, active_count=active_count, total_count=total_count, filters={'status': status, 'quote': quote, 'hbl': hbl})

@dgf_invoices_bp.route('/api/air/<lane_id>')
def air_invoice_details(lane_id):
    conn = sqlite3.connect('dhl_audit.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM dgf_air_invoices WHERE lane_id=?', (lane_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return jsonify({'invoice': row})
    return jsonify({'error': 'not found'}), 404

@dgf_invoices_bp.route('/api/fcl/<lane_id>')
def fcl_invoice_details(lane_id):
    conn = sqlite3.connect('dhl_audit.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM dgf_fcl_invoices WHERE lane_id=?', (lane_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return jsonify({'invoice': row})
    return jsonify({'error': 'not found'}), 404

@dgf_invoices_bp.route('/api/lcl/<lane_id>')
def lcl_invoice_details(lane_id):
    conn = sqlite3.connect('dhl_audit.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM dgf_lcl_invoices WHERE lane_id=?', (lane_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return jsonify({'invoice': row})
    return jsonify({'error': 'not found'}), 404

@dgf_invoices_bp.route('/air/update', methods=['POST'])
def update_air_invoice():
    lane_id = request.form.get('lane_id')
    if not lane_id:
        flash('Missing lane ID for invoice update.', 'error')
        return redirect(url_for('dgf_invoices.air_invoices'))

    # Collect editable fields
    terms = request.form.get('terms')
    origin_currency = request.form.get('origin_currency')
    origin_fx_rate = request.form.get('origin_fx_rate')
    destination_charges = request.form.get('destination_charges')
    destination_currency = request.form.get('destination_currency')
    destination_fx_rate = request.form.get('destination_fx_rate')
    total_cny = request.form.get('total_cny')
    status = request.form.get('status')

    # Normalize numeric fields
    def to_float(v):
        try:
            if v is None or v == '':
                return None
            return float(str(v).replace(',', '').strip())
        except Exception:
            return None

    origin_fx_rate_f = to_float(origin_fx_rate)
    destination_charges_f = to_float(destination_charges)
    destination_fx_rate_f = to_float(destination_fx_rate)
    total_cny_f = to_float(total_cny)

    conn = sqlite3.connect('dhl_audit.db')
    cur = conn.cursor()
    try:
        cur.execute('''
            UPDATE dgf_air_invoices
            SET terms=?, origin_currency=?, origin_fx_rate=?,
                destination_charges=?, destination_currency=?, destination_fx_rate=?,
                total_cny=?, status=?
            WHERE lane_id=?
        ''', (
            terms, origin_currency, origin_fx_rate_f,
            destination_charges_f, destination_currency, destination_fx_rate_f,
            total_cny_f, status,
            lane_id
        ))
        conn.commit()
        flash('Invoice updated successfully.', 'success')
    except Exception as e:
        flash(f'Update failed: {e}', 'error')
    finally:
        conn.close()
    return redirect(url_for('dgf_invoices.air_invoices'))

def _update_common(table: str, lane_id: str, form):
    # Collect editable fields (same as air)
    terms = form.get('terms')
    origin_currency = form.get('origin_currency')
    origin_fx_rate = form.get('origin_fx_rate')
    destination_charges = form.get('destination_charges')
    destination_currency = form.get('destination_currency')
    destination_fx_rate = form.get('destination_fx_rate')
    total_cny = form.get('total_cny')
    status = form.get('status')

    def to_float(v):
        try:
            if v is None or v == '':
                return None
            return float(str(v).replace(',', '').strip())
        except Exception:
            return None

    origin_fx_rate_f = to_float(origin_fx_rate)
    destination_charges_f = to_float(destination_charges)
    destination_fx_rate_f = to_float(destination_fx_rate)
    total_cny_f = to_float(total_cny)

    conn = sqlite3.connect('dhl_audit.db')
    cur = conn.cursor()
    try:
        cur.execute(f'''
            UPDATE {table}
            SET terms=?, origin_currency=?, origin_fx_rate=?,
                destination_charges=?, destination_currency=?, destination_fx_rate=?,
                total_cny=?, status=?
            WHERE lane_id=?
        ''', (
            terms, origin_currency, origin_fx_rate_f,
            destination_charges_f, destination_currency, destination_fx_rate_f,
            total_cny_f, status,
            lane_id
        ))
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

@dgf_invoices_bp.route('/fcl/update', methods=['POST'])
def update_fcl_invoice():
    lane_id = request.form.get('lane_id')
    if not lane_id:
        flash('Missing lane ID for invoice update.', 'error')
        return redirect(url_for('dgf_invoices.fcl_invoices'))
    ok, err = _update_common('dgf_fcl_invoices', lane_id, request.form)
    flash('Invoice updated successfully.' if ok else f'Update failed: {err}', 'success' if ok else 'error')
    return redirect(url_for('dgf_invoices.fcl_invoices'))

@dgf_invoices_bp.route('/lcl/update', methods=['POST'])
def update_lcl_invoice():
    lane_id = request.form.get('lane_id')
    if not lane_id:
        flash('Missing lane ID for invoice update.', 'error')
        return redirect(url_for('dgf_invoices.lcl_invoices'))
    ok, err = _update_common('dgf_lcl_invoices', lane_id, request.form)
    flash('Invoice updated successfully.' if ok else f'Update failed: {err}', 'success' if ok else 'error')
    return redirect(url_for('dgf_invoices.lcl_invoices'))
