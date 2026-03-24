(function () {
  const PREVIEW_COLS = 8;
  const state = {
    page: 1, pageSize: 50, total: 0, items: [], selected: new Set(), previewId: null,
    metadataBulk: { sessionId: null, previewItems: [], summary: null },
    readinessDetail: null,
    readinessDetailPlannedReleaseId: null,
  };
  const READINESS_DOMAINS = ['planning_identity', 'scheduling', 'metadata', 'playlist', 'visual_assets'];
  const $ = (id) => document.getElementById(id);
  const noteEl = $('planner-note');
  function apiUrl(path) { return new URL(path, window.location.origin).toString(); }

  function esc(v) { return String(v ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'); }
  async function parseError(res) {
    const text = await res.text();
    try { const body = JSON.parse(text); return body.error?.message || body.detail || text || ('HTTP ' + res.status); } catch (_) { return text || ('HTTP ' + res.status); }
  }
  function setNote(msg) { noteEl.textContent = msg || ''; }
  function setMetadataBulkStaleBanner(isVisible) {
    $('mbp-stale-banner').style.display = isVisible ? 'block' : 'none';
  }

  function queryParams() {
    const p = new URLSearchParams();
    const push = (k, v) => { if (String(v || '').trim()) p.set(k, String(v).trim()); };
    push('channel_slug', $('filter-channel').value);
    push('content_type', $('filter-content-type').value);
    push('q', $('filter-q').value);
    push('readiness_status', $('filter-readiness-status').value);
    push('sort_by', $('sort-by').value);
    push('sort_dir', $('sort-dir').value);
    p.set('include_readiness_summary', 'true');
    p.set('page', String(state.page));
    p.set('page_size', String(state.pageSize));
    return p;
  }

  function readinessUiSummary(item) {
    const readiness = item.readiness || {};
    const aggregate = String(readiness.aggregate_status || 'NOT_READY');
    const reason = String(readiness.primary_reason || '').trim();
    const remediation = String(readiness.primary_remediation_hint || '').trim();
    const titleParts = [`${aggregate}`];
    if (reason) titleParts.push(`Reason: ${reason}`);
    if (remediation) titleParts.push(`Next: ${remediation}`);
    return {
      aggregate,
      reason,
      remediation,
      title: titleParts.join('\n'),
    };
  }

  function renderReadinessBadge(item) {
    const summary = readinessUiSummary(item);
    const cls = summary.aggregate === 'BLOCKED'
      ? 'background:#fee2e2;color:#991b1b;border:1px solid #fecaca;'
      : (summary.aggregate === 'READY_FOR_MATERIALIZATION'
        ? 'background:#dcfce7;color:#166534;border:1px solid #bbf7d0;'
        : 'background:#fff7ed;color:#9a3412;border:1px solid #fed7aa;');
    const preview = summary.reason || (summary.aggregate === 'READY_FOR_MATERIALIZATION'
      ? 'All mandatory domains are ready for materialization.'
      : 'Details available.');
    const previewHint = summary.remediation || (summary.aggregate === 'READY_FOR_MATERIALIZATION'
      ? 'No remediation required.'
      : 'Open details for domain-level reasons and hints.');
    const compactPreview = `${preview} · ${previewHint}`;
    return `<button type="button" data-readiness-open="${item.id}" title="${esc(summary.title)}" style="cursor:pointer; padding:2px 6px; border-radius:12px; ${cls}">
      ${esc(summary.aggregate)}
    </button>
    <div class="muted" style="max-width:260px;" title="${esc(compactPreview)}">${esc(compactPreview)}</div>`;
  }

  function editableInput(item, field, value, type) {
    const disabled = item.status !== 'PLANNED';
    const val = value == null ? '' : value;
    return `<input data-id="${item.id}" data-field="${field}" value="${esc(val)}" ${type ? `type="${type}"` : ''} ${disabled ? 'disabled' : ''}>`;
  }

  function previewPlaceholder(message) {
    return `<tr><td colspan="${PREVIEW_COLS}" class="muted">${esc(message)}</td></tr>`;
  }

  function renderRows() {
    const tbody = $('planner-tbody');
    if (!state.items.length) {
      tbody.innerHTML = '<tr><td colspan="10" class="muted">No releases.</td></tr>';
      return;
    }
    tbody.innerHTML = state.items.map((item) => `<tr data-row-id="${item.id}">
      <td><input type="checkbox" data-select-id="${item.id}" ${state.selected.has(item.id) ? 'checked' : ''}></td>
      <td>${item.id}</td>
      <td>${esc(item.status)}</td>
      <td>${renderReadinessBadge(item)}</td>
      <td>${editableInput(item, 'channel_slug', item.channel_slug)}</td>
      <td>${editableInput(item, 'content_type', item.content_type)}</td>
      <td>${editableInput(item, 'title', item.title)}</td>
      <td>${editableInput(item, 'publish_at', item.publish_at, 'text')}</td>
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

    tbody.querySelectorAll('button[data-readiness-open]').forEach((el) => {
      el.addEventListener('click', async () => {
        const id = Number(el.getAttribute('data-readiness-open'));
        if (!Number.isInteger(id) || id <= 0) return;
        try {
          await openReadinessDialog(id);
        } catch (err) {
          setNote(`Readiness load failed: ${err.message}`);
        }
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
          const payload = {};
          payload[field] = (field === 'publish_at' && value.trim() === '') ? null : value;
          const res = await fetch(apiUrl(`/v1/planner/releases/${id}`), {
            method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
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
          await res.json();
          await loadList();
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
    const res = await fetch(apiUrl(`/v1/planner/releases?${queryParams().toString()}`));
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

  function checkIsActionable(check) {
    return String(check?.status || '') !== 'PASS';
  }

  function domainStatusRank(status) {
    if (status === 'BLOCKED') return 0;
    if (status === 'NOT_READY') return 1;
    return 2;
  }

  function orderedDomains(readiness) {
    const domains = readiness?.domains || {};
    return READINESS_DOMAINS
      .map((domainName) => {
        const domain = domains[domainName] || {};
        const status = String(domain.status || 'NOT_READY');
        return { domainName, domain, status };
      })
      .sort((a, b) => {
        const rankDelta = domainStatusRank(a.status) - domainStatusRank(b.status);
        if (rankDelta !== 0) return rankDelta;
        return READINESS_DOMAINS.indexOf(a.domainName) - READINESS_DOMAINS.indexOf(b.domainName);
      });
  }

  function renderReadinessDomains(readiness, actionableOnly) {
    const domainBlocks = orderedDomains(readiness);
    return domainBlocks.map(({ domainName, domain, status }) => {
      const checks = Array.isArray(domain.checks) ? domain.checks : [];
      const visibleChecks = actionableOnly ? checks.filter(checkIsActionable) : checks;
      const checksMarkup = visibleChecks.length
        ? `<ul>${visibleChecks.map((check) => `<li>
          <strong>${esc(String(check.code || '-'))}</strong>
          <div>Status: ${esc(String(check.status || '-'))}</div>
          <div>${esc(String(check.message || '-'))}</div>
          <div class="muted">Remediation: ${esc(String(check.remediation_hint || '-'))}</div>
        </li>`).join('')}</ul>`
        : '<p class="muted">No checks to show for current filter.</p>';
      return `<section style="border:1px solid #e5e7eb; padding:8px; margin-bottom:8px;">
        <h4 style="margin:0 0 6px 0;">${esc(domainName)} · ${esc(status)}</h4>
        ${checksMarkup}
      </section>`;
    }).join('');
  }

  function renderReadinessDetail() {
    const readiness = state.readinessDetail || {};
    const aggregate = String(readiness.aggregate_status || '-');
    const primaryReason = readiness.primary_reason?.message || '-';
    const primaryRemediation = readiness.primary_remediation_hint || '-';
    const computedAt = String(readiness.computed_at || '-');
    const actionableOnly = $('readiness-actionable-only').checked;

    $('readiness-dialog-aggregate').textContent = aggregate;
    $('readiness-dialog-computed-at').textContent = computedAt;
    $('readiness-dialog-primary-reason').textContent = String(primaryReason || '-');
    $('readiness-dialog-primary-remediation').textContent = String(primaryRemediation || '-');
    $('readiness-domains-body').innerHTML = renderReadinessDomains(readiness, actionableOnly);
  }

  async function openReadinessDialog(plannedReleaseId) {
    state.readinessDetailPlannedReleaseId = plannedReleaseId;
    $('readiness-dialog-release-label').textContent = `planned_release_id=${plannedReleaseId}`;
    $('readiness-domains-body').textContent = 'Loading readiness...';
    $('readiness-dialog').showModal();
    const res = await fetch(apiUrl(`/v1/planner/planned-releases/${plannedReleaseId}/readiness`));
    if (!res.ok) throw new Error(await parseError(res));
    state.readinessDetail = await res.json();
    renderReadinessDetail();
  }

  async function bulkDelete() {
    if (!state.selected.size) return;
    const ids = Array.from(state.selected.values());
    const res = await fetch(apiUrl('/v1/planner/releases/bulk-delete'), {
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
    Object.keys(data).forEach((k) => {
      if (typeof data[k] === 'string' && data[k].trim() === '') delete data[k];
    });
    data.count = Number(data.count || 1);
    data.mode = mode || 'strict';
    const res = await fetch(apiUrl('/v1/planner/releases/bulk-create'), {
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
    $('import-preview-body').innerHTML = previewPlaceholder('Loading preview...');
    const res = await fetch(apiUrl('/v1/planner/import/preview'), { method: 'POST', body: fd });
    if (!res.ok) throw new Error(await parseError(res));
    const out = await res.json();
    state.previewId = out.preview_id;
    $('import-summary').textContent = JSON.stringify(out.summary || {});
    $('import-confirm-strict').disabled = !out.can_confirm_strict;
    $('import-confirm-replace').disabled = !out.can_confirm_replace;
    const rows = out.rows || [];
    $('import-preview-body').innerHTML = rows.length ? rows.map((r) => {
      const n = r.normalized || {};
      const errors = r.errors || [];
      const err = errors.join('; ');
      const conflict = r.conflict ? `CONFLICT id=${r.existing_release_id}` : '';
      const rowClass = errors.length ? 'preview-row-error' : (r.conflict ? 'preview-row-conflict' : '');
      return `<tr${rowClass ? ` class="${rowClass}"` : ''}>
      <td>${esc(r.row_num)}</td><td>${esc(n.channel_slug)}</td><td>${esc(n.content_type)}</td><td>${esc(n.title)}</td><td>${esc(n.publish_at)}</td><td>${esc(n.notes)}</td><td>${esc(err)}</td><td>${esc(conflict)}</td>
    </tr>`).join('') : previewPlaceholder('No rows.');
  }

  async function confirmImport(mode) {
    if (!state.previewId) throw new Error('Preview first');
    const res = await fetch(apiUrl('/v1/planner/import/confirm'), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ preview_id: state.previewId, mode }),
    });
    if (!res.ok) throw new Error(await parseError(res));
    const out = await res.json();
    $('import-modal').close();
    setNote(`Import confirmed (${out.mode}).`);
    await loadList();
  }

  function selectedPlannerItemIds() {
    return Array.from(state.selected.values()).map((v) => Number(v)).filter((v) => Number.isInteger(v) && v > 0);
  }

  function selectedPreviewFields(prefix) {
    const out = [];
    if ($(`${prefix}-title`).checked) out.push('title');
    if ($(`${prefix}-description`).checked) out.push('description');
    if ($(`${prefix}-tags`).checked) out.push('tags');
    return out;
  }

  function parseOverridesJson() {
    const raw = String($('mbp-overrides-json').value || '').trim();
    if (!raw) return {};
    try {
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('Override JSON must be an object.');
      return parsed;
    } catch (err) {
      throw new Error(`Invalid override JSON: ${err.message}`);
    }
  }

  function hasOverwrite(item) {
    const fields = item.fields || {};
    return ['title', 'description', 'tags'].some((k) => {
      const f = fields[k] || {};
      return f.status === 'OVERWRITE_READY' || f.overwrite_required === true;
    });
  }

  function filterMetadataItems(items) {
    const applyableOnly = $('mbp-filter-applyable-only').checked;
    const overwriteOnly = $('mbp-filter-overwrite-only').checked;
    return (items || []).filter((item) => {
      const applyableOk = !applyableOnly || (item.mapping_status === 'RESOLVED_TO_RELEASE' && item.item_applyable === true);
      const overwriteOk = !overwriteOnly || hasOverwrite(item);
      return applyableOk && overwriteOk;
    });
  }

  function mappingReason(item) {
    const errors = item.item_errors || [];
    if (errors.length && errors[0].message) return String(errors[0].message);
    if (item.mapping_status === 'UNRESOLVED_NO_TARGET') return 'Unresolved target';
    if (item.mapping_status === 'DUPLICATE_TARGET') return 'Duplicate target deduped';
    if (item.mapping_status === 'INVALID_SELECTION') return 'Invalid selection';
    return '-';
  }

  function mappingLabel(item) {
    if (item.mapping_status === 'UNRESOLVED_NO_TARGET') return 'Unresolved target';
    if (item.mapping_status === 'DUPLICATE_TARGET') return 'Duplicate target deduped';
    if (item.mapping_status === 'INVALID_SELECTION') return 'Invalid selection';
    return String(item.mapping_status || '-');
  }

  function sourceLabel(source) {
    if (!source) return '-';
    if (source.selection_mode === 'channel_default') return 'Channel default used';
    if (source.selection_mode === 'temporary_override') return 'Temporary override active for this channel';
    return String(source.selection_mode || '-');
  }

  function renderFieldSummary(item) {
    const fields = item.fields || {};
    return ['title', 'description', 'tags'].map((field) => {
      const f = fields[field];
      if (!f) return `${field}: -`;
      const base = `${field}: ${f.status || '-'}`;
      const overwrite = f.overwrite_required ? ' (overwrite confirmation required)' : '';
      const source = `, ${sourceLabel(f.source)}`;
      return base + overwrite + source;
    }).join('\n');
  }

  function renderMetadataPreviewItems() {
    const tbody = $('mbp-items-body');
    const visibleItems = filterMetadataItems(state.metadataBulk.previewItems);
    if (!visibleItems.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="muted">No preview items for current filters.</td></tr>';
      return;
    }
    tbody.innerHTML = visibleItems.map((item) => {
      const plannerId = Number(item.planner_item_id);
      const canSelect = item.mapping_status === 'RESOLVED_TO_RELEASE' && item.item_applyable === true;
      const checked = canSelect && (item._selected !== false);
      const statusLabel = mappingLabel(item);
      const reason = mappingReason(item);
      return `<tr>
        <td><input type="checkbox" data-mbp-select-item="${esc(plannerId)}" ${checked ? 'checked' : ''} ${canSelect ? '' : 'disabled'}></td>
        <td>${esc(plannerId)}</td>
        <td>${esc(statusLabel)}</td>
        <td>${esc(item.release_id ?? '-')}</td>
        <td>${esc(item.duplicate_of_release_id ?? '-')}</td>
        <td>${esc(reason)}</td>
        <td><pre style="white-space:pre-wrap; margin:0;">${esc(renderFieldSummary(item))}</pre></td>
      </tr>`;
    }).join('');
    tbody.querySelectorAll('input[data-mbp-select-item]').forEach((el) => {
      el.addEventListener('change', () => {
        const plannerId = Number(el.getAttribute('data-mbp-select-item'));
        const target = state.metadataBulk.previewItems.find((item) => Number(item.planner_item_id) === plannerId);
        if (target) target._selected = el.checked;
      });
    });
  }

  function renderMetadataSummary() {
    const summary = state.metadataBulk.summary;
    $('mbp-summary').textContent = summary ? JSON.stringify(summary, null, 2) : 'No preview yet.';
    $('mbp-session-label').textContent = state.metadataBulk.sessionId ? `session_id=${state.metadataBulk.sessionId}` : '';
  }

  async function createMetadataPreview() {
    const planner_item_ids = selectedPlannerItemIds();
    if (!planner_item_ids.length) throw new Error('Select planner items on the main table first.');
    const fields = selectedPreviewFields('mbp-field');
    if (!fields.length) throw new Error('Select at least one preview field.');
    const overrides = parseOverridesJson();
    const res = await fetch(apiUrl('/v1/planner/metadata-bulk/preview'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ planner_item_ids, fields, overrides }),
    });
    if (!res.ok) throw new Error(await parseError(res));
    const out = await res.json();
    state.metadataBulk.sessionId = String(out.session_id || '');
    state.metadataBulk.summary = out.summary || null;
    state.metadataBulk.previewItems = (out.items || []).map((item) => ({ ...item, _selected: item.item_applyable === true }));
    setMetadataBulkStaleBanner(false);
    renderMetadataSummary();
    renderMetadataPreviewItems();
  }

  function selectedItemsForApply() {
    return state.metadataBulk.previewItems
      .filter((item) => item._selected === true)
      .map((item) => Number(item.planner_item_id))
      .filter((id) => Number.isInteger(id) && id > 0);
  }

  function buildOverwriteConfirmations(selectedFields) {
    const out = {};
    state.metadataBulk.previewItems.forEach((item) => {
      if (item._selected !== true) return;
      const fields = item.fields || {};
      const needed = selectedFields.filter((field) => {
        const f = fields[field] || {};
        return f.status === 'OVERWRITE_READY' || f.overwrite_required === true;
      });
      if (needed.length) out[String(item.planner_item_id)] = needed;
    });
    return out;
  }

  function looksStaleOrExpired(body, messageText) {
    const text = String(messageText || '').toUpperCase();
    const code = String(body?.error?.code || '').toUpperCase();
    return ['MBP_SESSION_EXPIRED', 'MBP_SESSION_INVALIDATED', 'MBP_PREVIEW_STALE'].includes(code)
      || text.includes('STALE')
      || text.includes('EXPIRED')
      || text.includes('INVALIDATED');
  }

  async function applyMetadataPreview() {
    if (!state.metadataBulk.sessionId) throw new Error('Create a bulk preview first.');
    const selected_items = selectedItemsForApply();
    if (!selected_items.length) throw new Error('Select at least one applyable preview item.');
    const selected_fields = selectedPreviewFields('mbp-apply-field');
    if (!selected_fields.length) throw new Error('Select at least one apply field.');
    const overwrite_confirmed = buildOverwriteConfirmations(selected_fields);
    const res = await fetch(apiUrl(`/v1/planner/metadata-bulk/sessions/${state.metadataBulk.sessionId}/apply`), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ selected_items, selected_fields, overwrite_confirmed }),
    });
    const text = await res.text();
    let body = {};
    try { body = JSON.parse(text || '{}'); } catch (_) { body = {}; }
    if (!res.ok || body.error) {
      const message = body.error?.message || body.detail || text || `HTTP ${res.status}`;
      if (looksStaleOrExpired(body, message)) setMetadataBulkStaleBanner(true);
      throw new Error(message);
    }
    $('mbp-apply-result').textContent = JSON.stringify(body, null, 2);
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
  $('import-preview-btn').addEventListener('click', async () => {
    try {
      await previewImport();
    } catch (e) {
      $('import-preview-body').innerHTML = previewPlaceholder('Preview failed.');
      setNote(e.message);
    }
  });
  $('import-confirm-strict').addEventListener('click', async () => { try { await confirmImport('strict'); } catch (e) { setNote(e.message); } });
  $('import-confirm-replace').addEventListener('click', async () => { try { await confirmImport('replace'); } catch (e) { setNote(e.message); } });

  $('metadata-bulk-open').addEventListener('click', () => {
    $('metadata-bulk-modal').showModal();
    setMetadataBulkStaleBanner(false);
  });
  $('mbp-close-btn').addEventListener('click', () => $('metadata-bulk-modal').close());
  $('mbp-preview-btn').addEventListener('click', async () => {
    try {
      await createMetadataPreview();
      setNote('Metadata bulk preview created.');
    } catch (e) {
      setNote(e.message);
    }
  });
  $('mbp-create-new-preview-btn').addEventListener('click', async () => {
    try {
      await createMetadataPreview();
      setNote('Created fresh metadata bulk preview session.');
    } catch (e) {
      setNote(e.message);
    }
  });
  $('mbp-apply-selected-btn').addEventListener('click', async () => {
    try {
      await applyMetadataPreview();
      setNote('Metadata bulk apply completed.');
    } catch (e) {
      setNote(e.message);
    }
  });
  $('mbp-filter-applyable-only').addEventListener('change', renderMetadataPreviewItems);
  $('mbp-filter-overwrite-only').addEventListener('change', renderMetadataPreviewItems);
  $('mbp-select-all-items').addEventListener('change', (e) => {
    const checked = e.target.checked;
    state.metadataBulk.previewItems.forEach((item) => {
      const canSelect = item.mapping_status === 'RESOLVED_TO_RELEASE' && item.item_applyable === true;
      item._selected = canSelect && checked;
    });
    renderMetadataPreviewItems();
  });
  $('readiness-actionable-only').addEventListener('change', renderReadinessDetail);
  $('readiness-dialog-close').addEventListener('click', () => $('readiness-dialog').close());

  $('import-preview-body').innerHTML = previewPlaceholder('No preview yet.');
  setMetadataBulkStaleBanner(false);
  loadList().catch((e) => setNote(e.message));
})();
