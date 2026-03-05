(function () {
  const state = { page: 1, pageSize: 50, total: 0, items: [], selected: new Set(), previewId: null };
  const $ = (id) => document.getElementById(id);
  const noteEl = $('planner-note');

  function esc(v) { return String(v ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'); }
  async function parseError(res) {
    const text = await res.text();
    try { const body = JSON.parse(text); return body.error?.message || body.detail || text || ('HTTP ' + res.status); } catch (_) { return text || ('HTTP ' + res.status); }
  }
  function setNote(msg) { noteEl.textContent = msg || ''; }

  function queryParams() {
    const p = new URLSearchParams();
    const push = (k, v) => { if (String(v || '').trim()) p.set(k, String(v).trim()); };
    push('channel_slug', $('filter-channel').value);
    push('content_type', $('filter-content-type').value);
    push('q', $('filter-q').value);
    push('sort_by', $('sort-by').value);
    push('sort_dir', $('sort-dir').value);
    p.set('page', String(state.page));
    p.set('page_size', String(state.pageSize));
    return p;
  }

  function editableInput(item, field, value, type) {
    const disabled = item.status !== 'PLANNED';
    const val = value == null ? '' : value;
    return `<input data-id="${item.id}" data-field="${field}" value="${esc(val)}" ${type ? `type="${type}"` : ''} ${disabled ? 'disabled' : ''}>`;
  }

  function renderRows() {
    const tbody = $('planner-tbody');
    if (!state.items.length) {
      tbody.innerHTML = '<tr><td colspan="9" class="muted">No releases.</td></tr>';
      return;
    }
    tbody.innerHTML = state.items.map((item) => `<tr data-row-id="${item.id}">
      <td><input type="checkbox" data-select-id="${item.id}" ${state.selected.has(item.id) ? 'checked' : ''}></td>
      <td>${item.id}</td>
      <td>${esc(item.status)}</td>
      <td>${editableInput(item, 'channel_slug', item.channel_slug)}</td>
      <td>${editableInput(item, 'content_type', item.content_type)}</td>
      <td>${editableInput(item, 'title', item.title)}</td>
      <td>${editableInput(item, 'publish_at', item.publish_at, 'datetime-local')}</td>
      <td>${editableInput(item, 'notes', item.notes)}</td>
      <td>${esc(item.updated_at)}</td>
    </tr>`).join('');

    tbody.querySelectorAll('input[data-select-id]').forEach((el) => {
      el.addEventListener('change', () => {
        const id = Number(el.getAttribute('data-select-id'));
        if (el.checked) state.selected.add(id); else state.selected.delete(id);
        $('bulk-delete-btn').disabled = !state.selected.size;
      });
    });

    tbody.querySelectorAll('input[data-field]').forEach((el) => {
      el.addEventListener('focus', () => { el.dataset.prev = el.value; });
      el.addEventListener('change', async () => {
        const id = Number(el.getAttribute('data-id'));
        const field = el.getAttribute('data-field');
        const value = el.value;
        if (field === 'status') { return; }
        try {
          const res = await fetch(`/v1/planner/releases/${id}`, {
            method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ [field]: value }),
          });
          if (!res.ok) {
            const err = await parseError(res);
            if (res.status === 409) {
              el.value = el.dataset.prev || '';
              setNote(`Release ${id} is locked. Change reverted.`);
              return;
            }
            el.value = el.dataset.prev || '';
            setNote(`Edit failed for ${id}: ${err}`);
            return;
          }
          const row = await res.json();
          const idx = state.items.findIndex((x) => x.id === id);
          if (idx >= 0) state.items[idx] = row;
          renderRows();
          setNote(`Saved release ${id}.`);
        } catch (err) {
          el.value = el.dataset.prev || '';
          setNote(`Edit failed: ${err.message}`);
        }
      });
    });
  }

  async function loadList() {
    state.pageSize = Number($('page-size').value) || 50;
    const res = await fetch(`/v1/planner/releases?${queryParams().toString()}`);
    if (!res.ok) throw new Error(await parseError(res));
    const data = await res.json();
    state.items = data.items || [];
    state.total = Number(data.pagination?.total || 0);
    state.selected.clear();
    $('bulk-delete-btn').disabled = true;
    renderRows();
    const start = (state.page - 1) * state.pageSize + 1;
    const end = Math.min(state.page * state.pageSize, state.total);
    $('page-label').textContent = state.total ? `${start}-${end} / ${state.total}` : '0';
  }

  async function bulkDelete() {
    if (!state.selected.size) return;
    const ids = Array.from(state.selected.values());
    const res = await fetch('/v1/planner/releases/bulk-delete', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ids }),
    });
    if (!res.ok) throw new Error(await parseError(res));
    const out = await res.json();
    setNote(`Deleted ${out.deleted_count || 0} release(s).`);
    await loadList();
  }

  async function submitBulkCreate(mode) {
    const form = $('bulk-create-form');
    const data = Object.fromEntries(new FormData(form).entries());
    data.count = Number(data.count || 1);
    data.mode = mode || 'strict';
    const res = await fetch('/v1/planner/releases/bulk-create', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data),
    });
    if (!res.ok) throw new Error(await parseError(res));
    const out = await res.json();
    $('bulk-create-modal').close();
    setNote(`Created ${out.created_count || 0} release(s).`);
    await loadList();
  }

  async function previewImport() {
    const file = $('import-file').files && $('import-file').files[0];
    if (!file) throw new Error('Choose a file first');
    const fd = new FormData();
    fd.append('file', file, file.name);
    const res = await fetch('/v1/planner/import/preview', { method: 'POST', body: fd });
    if (!res.ok) throw new Error(await parseError(res));
    const out = await res.json();
    state.previewId = out.preview_id;
    $('import-summary').textContent = JSON.stringify(out.summary || {});
    $('import-confirm-strict').disabled = !out.can_confirm_strict;
    $('import-confirm-replace').disabled = !out.can_confirm_replace;
    const rows = out.rows || [];
    $('import-preview-body').innerHTML = rows.length ? rows.map((r, idx) => `<tr>
      <td>${idx + 1}</td><td>${esc(r.channel_slug)}</td><td>${esc(r.content_type)}</td><td>${esc(r.title)}</td><td>${esc(r.publish_at)}</td><td>${esc(r.error || '')}</td><td>${esc(r.conflict || '')}</td>
    </tr>`).join('') : '<tr><td colspan="7" class="muted">No rows.</td></tr>';
  }

  async function confirmImport(mode) {
    if (!state.previewId) throw new Error('Preview first');
    const res = await fetch('/v1/planner/import/confirm', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ preview_id: state.previewId, mode }),
    });
    if (!res.ok) throw new Error(await parseError(res));
    const out = await res.json();
    $('import-modal').close();
    setNote(`Import confirmed (${out.mode}).`);
    await loadList();
  }

  $('reload-btn').addEventListener('click', async () => { state.page = 1; try { await loadList(); } catch (e) { setNote(e.message); } });
  $('prev-page').addEventListener('click', async () => { if (state.page > 1) { state.page -= 1; try { await loadList(); } catch (e) { setNote(e.message); } } });
  $('next-page').addEventListener('click', async () => { if (state.page * state.pageSize < state.total) { state.page += 1; try { await loadList(); } catch (e) { setNote(e.message); } } });
  $('select-all').addEventListener('change', (e) => {
    state.selected.clear();
    if (e.target.checked) state.items.forEach((it) => state.selected.add(it.id));
    renderRows();
    $('bulk-delete-btn').disabled = !state.selected.size;
  });
  $('bulk-delete-btn').addEventListener('click', async () => { try { await bulkDelete(); } catch (e) { setNote(e.message); } });

  $('bulk-create-open').addEventListener('click', () => $('bulk-create-modal').showModal());
  $('bulk-create-cancel').addEventListener('click', () => $('bulk-create-modal').close());
  $('bulk-create-strict').addEventListener('click', async () => { try { await submitBulkCreate('strict'); } catch (e) { setNote(e.message); } });
  $('bulk-create-replace').addEventListener('click', async () => { try { await submitBulkCreate('replace'); } catch (e) { setNote(e.message); } });

  $('import-open').addEventListener('click', () => $('import-modal').showModal());
  $('import-cancel').addEventListener('click', () => $('import-modal').close());
  $('import-preview-btn').addEventListener('click', async () => { try { await previewImport(); } catch (e) { setNote(e.message); } });
  $('import-confirm-strict').addEventListener('click', async () => { try { await confirmImport('strict'); } catch (e) { setNote(e.message); } });
  $('import-confirm-replace').addEventListener('click', async () => { try { await confirmImport('replace'); } catch (e) { setNote(e.message); } });

  loadList().catch((e) => setNote(e.message));
})();
