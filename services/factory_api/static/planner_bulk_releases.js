(function () {
  const PREVIEW_COLS = 8;
  const state = {
    page: 1, pageSize: 50, total: 0, items: [], selected: new Set(), previewId: null,
    metadataBulk: { sessionId: null, previewItems: [], summary: null },
    massAction: {
      sessionId: null,
      actionType: 'BATCH_MATERIALIZE_SELECTED',
      expiresAt: null,
      items: [],
      summary: null,
      executeResult: null,
      selectedKinds: new Set(['ALL']),
    },
    monthlyTemplates: {
      items: [],
      activeTemplateId: null,
      activeTemplate: null,
      preview: null,
      applyResult: null,
    },
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
  function setMassActionStaleBanner(isVisible) {
    $('pma-stale-banner').style.display = isVisible ? 'block' : 'none';
  }

  function queryParams() {
    const p = new URLSearchParams();
    const push = (k, v) => { if (String(v || '').trim()) p.set(k, String(v).trim()); };
    push('channel_slug', $('filter-channel').value);
    push('content_type', $('filter-content-type').value);
    push('q', $('filter-q').value);
    push('readiness_status', $('filter-readiness-status').value);
    push('readiness_problem', $('filter-readiness-problem').value);
    push('materialized_state', $('filter-materialized-state').value);
    push('job_creation_state', $('filter-job-creation-state').value);
    push('sort_by', $('sort-by').value);
    if ($('sort-by').value === 'readiness_priority') {
      push('readiness_priority', $('readiness-priority').value);
    }
    push('sort_dir', $('sort-dir').value);
    p.set('include_readiness_summary', 'true');
    p.set('page', String(state.page));
    p.set('page_size', String(state.pageSize));
    return p;
  }

  function renderReadinessSummary(summary) {
    const s = summary || {};
    $('readiness-summary-total').textContent = String(s.scope_total ?? 0);
    $('readiness-summary-ready').textContent = String(s.ready_for_materialization ?? 0);
    $('readiness-summary-not-ready').textContent = String(s.not_ready ?? 0);
    $('readiness-summary-blocked').textContent = String(s.blocked ?? 0);
    $('readiness-summary-attention').textContent = String(s.attention_count ?? 0);
    $('readiness-summary-computed-at').textContent = String(s.computed_at || 'Not available');
  }

  function selectedReadinessFilterValue() {
    return String($('filter-readiness-status').value || '').trim();
  }

  function selectedReadinessProblemValue() {
    return String($('filter-readiness-problem').value || '').trim();
  }

  function emptyPlannerMessage() {
    const readinessStatus = selectedReadinessFilterValue();
    const readinessProblem = selectedReadinessProblemValue();
    const materializedState = String($('filter-materialized-state').value || '').trim();
    const jobCreationState = String($('filter-job-creation-state').value || '').trim();
    if (readinessProblem === 'blocked_only') {
      return 'No BLOCKED items in current planner scope.';
    }
    if (readinessProblem === 'ready_only') {
      return 'No READY_FOR_MATERIALIZATION items in current planner scope.';
    }
    if (readinessStatus || readinessProblem) {
      return 'No items match the selected readiness filter.';
    }
    if (materializedState) {
      return 'No items match the selected materialized_state filter.';
    }
    if (jobCreationState) {
      return 'No items match the selected job_creation_state filter.';
    }
    return 'No planned releases in current planner scope.';
  }

  function renderMaterializationSummary(item) {
    const summary = item.materialization_state_summary || {};
    const stateLabel = String(summary.materialization_state || 'NOT_MATERIALIZED');
    const releaseId = summary.release_id ?? summary.materialized_release_id ?? null;
    const reason = String(summary.action_reason || '').trim();
    const stateText = `State: ${stateLabel}`;
    const linkText = releaseId ? `<a href="/ui/releases/${esc(releaseId)}" target="_blank" rel="noopener">release #${esc(releaseId)}</a>` : 'release: -';
    const reasonText = reason ? `<div class="muted">${esc(reason)}</div>` : '';
    return `<div>${esc(stateText)}</div><div class="muted">${linkText}</div>${reasonText}`;
  }

  function materializeActionState(item) {
    const summary = item.materialization_state_summary || {};
    const stateValue = String(summary.materialization_state || '');
    const reason = String(summary.action_reason || '').trim();
    const disabled = stateValue === 'BINDING_INCONSISTENT'
      || stateValue === 'ALREADY_MATERIALIZED'
      || stateValue === 'ACTION_DISABLED';
    return { disabled, reason: reason || (disabled ? 'Materialization unavailable' : ''), stateValue };
  }

  function openReleaseCta(releaseId) {
    if (!releaseId) return '';
    return `Open release: <a href="/ui/releases/${esc(releaseId)}" target="_blank" rel="noopener">#${esc(releaseId)}</a>`;
  }

  function openJobCta(jobId) {
    if (!jobId) return '';
    return `Open job: <a href="/jobs/${esc(jobId)}" target="_blank" rel="noopener">#${esc(jobId)}</a>`;
  }

  function renderJobCreationSummary(item) {
    const summary = item.job_creation_state_summary || {};
    const stateLabel = String(summary.job_creation_state || 'ACTION_DISABLED');
    const jobId = summary.job_id ?? null;
    const reason = String(summary.action_reason || '').trim();
    const stateText = `State: ${stateLabel}`;
    const linkText = jobId ? `<a href="/jobs/${esc(jobId)}" target="_blank" rel="noopener">job #${esc(jobId)}</a>` : 'job: -';
    const reasonText = reason ? `<div class="muted">${esc(reason)}</div>` : '';
    return `<div>${esc(stateText)}</div><div class="muted">${linkText}</div>${reasonText}`;
  }

  function createJobActionState(item) {
    const summary = item.job_creation_state_summary || {};
    const stateValue = String(summary.job_creation_state || 'ACTION_DISABLED');
    const reason = String(summary.action_reason || '').trim();
    const disabled = stateValue === 'ACTION_DISABLED'
      || stateValue === 'MULTIPLE_OPEN_INCONSISTENT'
      || stateValue === 'CURRENT_POINTER_INCONSISTENT';
    return { disabled, reason: reason || (disabled ? 'Job creation unavailable' : ''), stateValue };
  }

  function readinessUiSummary(item) {
    const readiness = item.readiness || {};
    const hasUnavailableError = readiness.aggregate_status == null && readiness.error?.code === 'PRS_READINESS_UNAVAILABLE';
    const aggregate = hasUnavailableError ? 'UNAVAILABLE' : String(readiness.aggregate_status || 'NOT_READY');
    const reason = hasUnavailableError
      ? String(readiness.error?.message || 'Readiness could not be computed for this item.').trim()
      : String(readiness.primary_reason || '').trim();
    const remediation = hasUnavailableError
      ? 'Use Refresh readiness to retry this view.'
      : String(readiness.primary_remediation_hint || '').trim();
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
        : (summary.aggregate === 'UNAVAILABLE'
          ? 'background:#f3f4f6;color:#374151;border:1px solid #d1d5db;'
          : 'background:#fff7ed;color:#9a3412;border:1px solid #fed7aa;'));
    const preview = summary.reason || (summary.aggregate === 'READY_FOR_MATERIALIZATION'
      ? 'All mandatory domains are ready for materialization.'
      : (summary.aggregate === 'UNAVAILABLE'
        ? 'Readiness could not be computed for this item.'
        : 'Details available.'));
    const previewHint = summary.remediation || (summary.aggregate === 'READY_FOR_MATERIALIZATION'
      ? 'No remediation required.'
      : (summary.aggregate === 'UNAVAILABLE'
        ? 'Use Refresh readiness to retry this view.'
        : 'Open details for domain-level reasons and hints.'));
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
      tbody.innerHTML = `<tr><td colspan="13" class="muted">${esc(emptyPlannerMessage())}</td></tr>`;
      return;
    }
    tbody.innerHTML = state.items.map((item) => {
      const materializeState = materializeActionState(item);
      const createJobState = createJobActionState(item);
      return `<tr data-row-id="${item.id}">
      <td><input type="checkbox" data-select-id="${item.id}" ${state.selected.has(item.id) ? 'checked' : ''}></td>
      <td>${item.id}</td>
      <td>${esc(item.status)}</td>
      <td>${renderReadinessBadge(item)}</td>
      <td>${renderMaterializationSummary(item)}</td>
      <td>${renderJobCreationSummary(item)}</td>
      <td>
        <button type="button" data-materialize-item="${item.id}" ${materializeState.disabled ? 'disabled' : ''}>Materialize</button>
        <button type="button" data-create-job-item="${item.id}" ${createJobState.disabled ? 'disabled' : ''}>Create Job</button>
        <button type="button" data-materialization-detail="${item.id}">Details</button>
        <button type="button" data-job-creation-detail="${item.id}">Job details</button>
        ${materializeState.disabled ? `<div class="muted">${esc(materializeState.reason)}</div>` : ''}
        ${createJobState.disabled ? `<div class="muted">${esc(createJobState.reason)}</div>` : ''}
      </td>
      <td>${editableInput(item, 'channel_slug', item.channel_slug)}</td>
      <td>${editableInput(item, 'content_type', item.content_type)}</td>
      <td>${editableInput(item, 'title', item.title)}</td>
      <td>${editableInput(item, 'publish_at', item.publish_at, 'text')}</td>
      <td>${editableInput(item, 'notes', item.notes)}</td>
      <td>${esc(item.updated_at)}</td>
    </tr>`;
    }).join('');

    tbody.querySelectorAll('input[data-select-id]').forEach((el) => {
      el.addEventListener('change', () => {
        const id = Number(el.getAttribute('data-select-id'));
        if (el.checked) state.selected.add(id); else state.selected.delete(id);
        $('bulk-delete-btn').disabled = !state.selected.size;
        updateMassActionSelectionState();
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

    tbody.querySelectorAll('button[data-materialization-detail]').forEach((el) => {
      el.addEventListener('click', () => {
        const id = Number(el.getAttribute('data-materialization-detail'));
        if (!Number.isInteger(id) || id <= 0) return;
        const item = state.items.find((it) => Number(it.id) === id);
        if (!item) return;
        $('materialization-dialog-release-label').textContent = `planned_release_id=${id}`;
        $('materialization-summary-body').textContent = JSON.stringify(item.materialization_state_summary || {}, null, 2);
        $('materialization-diagnostics-body').textContent = JSON.stringify(item.binding_diagnostics || {}, null, 2);
        const releaseId = item.materialization_state_summary?.release_id ?? item.materialized_release_id;
        $('materialization-open-release-cta').innerHTML = openReleaseCta(releaseId);
        $('materialization-dialog').showModal();
      });
    });

    tbody.querySelectorAll('button[data-job-creation-detail]').forEach((el) => {
      el.addEventListener('click', () => {
        const id = Number(el.getAttribute('data-job-creation-detail'));
        if (!Number.isInteger(id) || id <= 0) return;
        const item = state.items.find((it) => Number(it.id) === id);
        if (!item) return;
        $('job-creation-dialog-release-label').textContent = `planned_release_id=${id}`;
        $('job-creation-summary-body').textContent = JSON.stringify(item.job_creation_state_summary || {}, null, 2);
        $('job-creation-diagnostics-body').textContent = JSON.stringify(item.open_job_diagnostics || {}, null, 2);
        const jobId = item.job_creation_state_summary?.job_id ?? item.open_job_diagnostics?.current_open_job_id;
        $('job-creation-open-job-cta').innerHTML = openJobCta(jobId);
        $('job-creation-dialog').showModal();
      });
    });

    tbody.querySelectorAll('button[data-materialize-item]').forEach((el) => {
      el.addEventListener('click', async () => {
        const id = Number(el.getAttribute('data-materialize-item'));
        if (!Number.isInteger(id) || id <= 0) return;
        const res = await fetch(apiUrl(`/v1/planner/planned-releases/${id}/materialize`), { method: 'POST' });
        const body = await res.json();
        if (!res.ok || body.result === 'FAILED' || body.error) {
          const reason = body?.error?.message || body?.error?.code || `HTTP ${res.status}`;
          setNote(`Materialization failed for ${id}: ${reason}`);
          await loadList();
          return;
        }
        const releaseId = body?.release?.id;
        if (body.result === 'CREATED_NEW') {
          setNote(`Created new canonical release #${releaseId}. ${releaseId ? `Open release: /ui/releases/${releaseId}` : ''}`);
        } else if (body.result === 'RETURNED_EXISTING') {
          setNote(`Returned existing linked release #${releaseId}. ${releaseId ? `Open release: /ui/releases/${releaseId}` : ''}`);
        } else {
          setNote(`Materialization completed for ${id}.`);
        }
        await loadList();
      });
    });

    tbody.querySelectorAll('button[data-create-job-item]').forEach((el) => {
      el.addEventListener('click', async () => {
        const id = Number(el.getAttribute('data-create-job-item'));
        if (!Number.isInteger(id) || id <= 0) return;
        $('job-create-open-job-cta').innerHTML = '';
        const res = await fetch(apiUrl(`/v1/planner/planned-releases/${id}/create-job`), { method: 'POST' });
        const body = await res.json();
        if (!res.ok || body.result === 'FAILED' || body.error) {
          const reason = body?.error?.message || body?.error?.code || `HTTP ${res.status}`;
          setNote(`Job creation failed for ${id}: ${reason}`);
          await loadList();
          return;
        }
        const jobId = body?.job?.id;
        if (body.result === 'CREATED_NEW_JOB') {
          setNote('New job created in DRAFT.');
        } else if (body.result === 'RETURNED_EXISTING_OPEN_JOB') {
          setNote('Existing open job returned. No new job was created.');
        } else {
          setNote(`Job creation completed for ${id}.`);
        }
        $('job-create-open-job-cta').innerHTML = openJobCta(jobId);
        await loadList();
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
    updateMassActionSelectionState();
    renderRows();
    renderReadinessSummary(data.readiness_summary || {});
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
    const computedAt = String(readiness.computed_at || 'Not available');
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

  function updateMassActionSelectionState() {
    const count = selectedPlannerItemIds().length;
    $('mass-actions-open').disabled = count === 0;
    $('mass-actions-selected-count').textContent = `Selected: ${count}`;
    $('pma-selected-count').textContent = String(count);
  }

  function isMassActionExecutableKind(kind) {
    return kind === 'SUCCESS_CREATED_NEW' || kind === 'SUCCESS_RETURNED_EXISTING';
  }

  function massActionVisibleItems() {
    const executableOnly = $('pma-filter-executable-only').checked;
    const selectedKinds = state.massAction.selectedKinds;
    return state.massAction.items.filter((item) => {
      const kind = String(item.result_kind || '');
      if (executableOnly && !isMassActionExecutableKind(kind)) return false;
      if (!selectedKinds.has('ALL') && !selectedKinds.has(kind)) return false;
      return true;
    });
  }

  function formatMassActionMessage(item) {
    return String(item.expected_outcome || item.message || '-');
  }

  function formatMassActionDetails(item) {
    if (item.reason) return `${item.reason.code || '-'}: ${item.reason.message || '-'}`;
    if (item.details) return JSON.stringify(item.details);
    return '-';
  }

  function renderMassActionItems() {
    const tbody = $('pma-items-body');
    const visibleItems = massActionVisibleItems();
    if (!visibleItems.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="muted">No items for current filters.</td></tr>';
      return;
    }
    tbody.innerHTML = visibleItems.map((item) => {
      const plannedId = Number(item.planned_release_id);
      const checked = item._selected !== false;
      return `<tr>
        <td><input type="checkbox" data-pma-select-item="${esc(plannedId)}" ${checked ? 'checked' : ''}></td>
        <td>${esc(plannedId)}</td>
        <td>${esc(item.result_kind || '-')}</td>
        <td>${esc(formatMassActionMessage(item))}</td>
        <td>${esc(formatMassActionDetails(item))}</td>
      </tr>`;
    }).join('');
    tbody.querySelectorAll('input[data-pma-select-item]').forEach((el) => {
      el.addEventListener('change', () => {
        const plannedId = Number(el.getAttribute('data-pma-select-item'));
        const target = state.massAction.items.find((item) => Number(item.planned_release_id) === plannedId);
        if (target) target._selected = el.checked;
      });
    });
  }

  function selectedMassActionItemIds() {
    return state.massAction.items
      .filter((item) => item._selected !== false)
      .map((item) => Number(item.planned_release_id))
      .filter((id) => Number.isInteger(id) && id > 0);
  }

  function renderMassActionSummary() {
    $('pma-session-label').textContent = state.massAction.sessionId ? `session_id=${state.massAction.sessionId}` : '';
    $('pma-summary-json').textContent = state.massAction.summary ? JSON.stringify(state.massAction.summary, null, 2) : 'No preview yet.';
    $('pma-result-json').textContent = state.massAction.executeResult ? JSON.stringify(state.massAction.executeResult, null, 2) : 'No execute yet.';
    const summary = state.massAction.executeResult?.summary || {};
    $('pma-result-total').textContent = String(summary.total_selected || 0);
    $('pma-result-succeeded').textContent = String(summary.succeeded || 0);
    $('pma-result-failed').textContent = String(summary.failed || 0);
    $('pma-result-skipped').textContent = String(summary.skipped || 0);
    $('pma-result-created-new').textContent = String(summary.created_new_entities || 0);
    $('pma-result-returned-existing').textContent = String(summary.returned_existing_entities || 0);
    refreshMassActionExecuteAvailability();
  }

  function refreshMassActionExecuteAvailability() {
    const hasSession = Boolean(state.massAction.sessionId);
    const confirmed = $('pma-execute-confirm').checked;
    $('pma-execute-btn').disabled = !(hasSession && confirmed);
  }

  function parseIsoDate(value) {
    const ts = Date.parse(String(value || ''));
    return Number.isNaN(ts) ? null : new Date(ts);
  }

  function updateMassActionCountdown() {
    const expiresAt = parseIsoDate(state.massAction.expiresAt);
    $('pma-expires-at').textContent = expiresAt ? expiresAt.toISOString() : '-';
    if (!expiresAt) {
      $('pma-ttl-remaining').textContent = '-';
      return;
    }
    const diffMs = expiresAt.getTime() - Date.now();
    if (diffMs <= 0) {
      $('pma-ttl-remaining').textContent = 'expired';
      setMassActionStaleBanner(true);
      return;
    }
    const totalSec = Math.floor(diffMs / 1000);
    const min = Math.floor(totalSec / 60);
    const sec = totalSec % 60;
    $('pma-ttl-remaining').textContent = `${min}m ${String(sec).padStart(2, '0')}s`;
  }

  function massActionErrorCodeFromBody(body) {
    return String(body?.error?.code || '').toUpperCase();
  }

  function isMassActionStaleCode(code) {
    return ['PMA_SESSION_EXPIRED', 'PMA_SESSION_INVALIDATED', 'PMA_SELECTION_SCOPE_MISMATCH'].includes(code);
  }

  function readJsonFromResponseText(text) {
    try { return JSON.parse(text || '{}'); } catch (_) { return {}; }
  }

  async function createMassActionPreview() {
    const selected_item_ids = selectedPlannerItemIds();
    if (!selected_item_ids.length) throw new Error('Select planner items first.');
    const action_type = String($('pma-action-type').value || '').trim();
    if (!action_type) throw new Error('Choose batch action.');
    const res = await fetch(apiUrl('/v1/planner/mass-actions/preview'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action_type, selected_item_ids }),
    });
    const text = await res.text();
    const body = readJsonFromResponseText(text);
    if (!res.ok || body.error) throw new Error(body.error?.message || body.detail || text || `HTTP ${res.status}`);
    state.massAction.sessionId = String(body.session_id || '');
    state.massAction.actionType = String(body.action_type || action_type);
    state.massAction.expiresAt = String(body.expires_at || '');
    state.massAction.items = (body.items || []).map((item) => ({ ...item, _selected: true }));
    state.massAction.summary = {
      session_id: body.session_id,
      action_type: body.action_type,
      selected_count: body.selected_count,
      aggregate: body.aggregate || {},
      created_at: body.created_at,
      expires_at: body.expires_at,
    };
    state.massAction.executeResult = null;
    setMassActionStaleBanner(false);
    updateMassActionCountdown();
    renderMassActionSummary();
    renderMassActionItems();
  }

  async function executeMassAction() {
    if (!state.massAction.sessionId) throw new Error('Create preview first.');
    if (!$('pma-execute-confirm').checked) throw new Error('Explicit confirmation is required.');
    const selected_item_ids = selectedMassActionItemIds();
    if (!selected_item_ids.length) throw new Error('Select at least one preview item.');
    const res = await fetch(apiUrl(`/v1/planner/mass-actions/${state.massAction.sessionId}/execute`), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ selected_item_ids }),
    });
    const text = await res.text();
    const body = readJsonFromResponseText(text);
    if (!res.ok || body.error) {
      const code = massActionErrorCodeFromBody(body);
      if (isMassActionStaleCode(code)) setMassActionStaleBanner(true);
      throw new Error(body.error?.message || body.detail || text || `HTTP ${res.status}`);
    }
    state.massAction.executeResult = body;
    state.massAction.items = (body.items || []).map((item) => ({ ...item, _selected: true }));
    setMassActionStaleBanner(false);
    renderMassActionSummary();
    renderMassActionItems();
  }

  async function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    ta.remove();
  }

  function monthlyTemplateItemsForUi() {
    if (!state.monthlyTemplates.preview || !Array.isArray(state.monthlyTemplates.preview.items)) return [];
    const createableOnly = $('mpt-filter-createable-only').checked;
    const conflictsOnly = $('mpt-filter-conflicts-only').checked;
    return state.monthlyTemplates.preview.items.filter((item) => {
      const outcome = String(item.outcome || '');
      const overlapWarnings = Array.isArray(item.overlap_warnings) ? item.overlap_warnings : [];
      const createableOk = !createableOnly || outcome === 'WOULD_CREATE';
      const conflict = outcome !== 'WOULD_CREATE' || overlapWarnings.length > 0;
      const conflictsOk = !conflictsOnly || conflict;
      return createableOk && conflictsOk;
    });
  }

  function renderMonthlyTemplatePreviewItems() {
    const tbody = $('mpt-preview-items-body');
    const items = monthlyTemplateItemsForUi();
    if (!items.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="muted">No preview items for current filters.</td></tr>';
      return;
    }
    tbody.innerHTML = items.map((item) => {
      const reasons = (item.reasons || []).map((r) => `${r.code || '-'}: ${r.message || '-'}`).join('\n') || '-';
      const overlaps = (item.overlap_warnings || []).map((r) => `${r.code || '-'}: ${r.message || '-'}`).join('\n') || '-';
      return `<tr>
        <td>${esc(item.item_key || '-')}</td>
        <td>${esc(item.slot_code || '-')}</td>
        <td>${esc(item.position ?? '-')}</td>
        <td>${esc(item.planned_date || '-')}</td>
        <td>${esc(item.outcome || '-')}</td>
        <td><pre style="white-space:pre-wrap; margin:0;">${esc(reasons)}</pre></td>
        <td><pre style="white-space:pre-wrap; margin:0;">${esc(overlaps)}</pre></td>
      </tr>`;
    }).join('');
  }

  function renderMonthlyTemplateApplyResult() {
    const body = state.monthlyTemplates.applyResult;
    const summary = body?.summary || {};
    $('mpt-apply-total').textContent = String(summary.total_items ?? 0);
    $('mpt-apply-created').textContent = String(summary.created ?? 0);
    $('mpt-apply-blocked-duplicate').textContent = String(summary.blocked_duplicates ?? 0);
    $('mpt-apply-blocked-invalid-date').textContent = String(summary.blocked_invalid_dates ?? 0);
    $('mpt-apply-failed').textContent = String(summary.failed ?? 0);
    $('mpt-apply-overlap-warnings').textContent = String(summary.overlap_warnings ?? 0);
    const tbody = $('mpt-apply-items-body');
    const items = Array.isArray(body?.items) ? body.items : [];
    if (!items.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="muted">No apply result yet.</td></tr>';
      return;
    }
    tbody.innerHTML = items.map((item) => {
      const reasons = (item.reasons || []).map((r) => `${r.code || '-'}: ${r.message || '-'}`).join('\n') || '-';
      return `<tr>
        <td>${esc(item.item_key || '-')}</td>
        <td>${esc(item.slot_code || '-')}</td>
        <td>${esc(item.outcome || '-')}</td>
        <td>${esc(item.planned_release_id ?? '-')}</td>
        <td><pre style="white-space:pre-wrap; margin:0;">${esc(reasons)}</pre></td>
      </tr>`;
    }).join('');
  }

  function renderMonthlyTemplateDetail() {
    const pane = $('mpt-detail-pane');
    const tpl = state.monthlyTemplates.activeTemplate;
    if (!tpl) {
      pane.innerHTML = '<p class="muted">Select a template to open detail/editor.</p>';
      return;
    }
    const isArchived = String(tpl.status || '').toUpperCase() === 'ARCHIVED';
    const itemsJson = JSON.stringify(tpl.items || [], null, 2);
    pane.innerHTML = `
      <h3>Template #${esc(tpl.id)} · ${esc(tpl.template_name || '-')}</h3>
      <p class="muted">Status: ${esc(tpl.status || '-')} · Channel: ${esc(tpl.channel_id || '-')} · Updated: ${esc(tpl.updated_at || '-')}</p>
      ${isArchived ? '<p class="muted">Archived template is visible but cannot be edited, previewed, or applied.</p>' : ''}
      <div style="display:flex; gap:8px; flex-wrap:wrap;">
        <label>Name <input id="mpt-detail-name" value="${esc(tpl.template_name || '')}" ${isArchived ? 'disabled' : ''}></label>
        <label>Content type <input id="mpt-detail-content-type" value="${esc(tpl.content_type || '')}" ${isArchived ? 'disabled' : ''}></label>
      </div>
      <div style="margin-top:8px;">
        <label>Ordered items JSON</label><br>
        <textarea id="mpt-detail-items-json" rows="10" style="width:100%;" ${isArchived ? 'disabled' : ''}>${esc(itemsJson)}</textarea>
      </div>
      <p class="muted">Usage: apply_run_count=${esc(tpl.apply_run_count ?? 0)}, last_target_month=${esc(tpl.last_applied_target_month || '-')}, last_applied_at=${esc(tpl.last_applied_at || '-')}</p>
      <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:8px;">
        <button type="button" id="mpt-save-btn" ${isArchived ? 'disabled' : ''}>Save template</button>
        <button type="button" id="mpt-archive-btn" ${isArchived ? 'disabled' : ''}>Archive template</button>
        <button type="button" id="mpt-preview-open-btn" ${isArchived ? 'disabled' : ''}>Preview / Apply</button>
      </div>
    `;
    if (!isArchived) {
      $('mpt-save-btn').addEventListener('click', async () => {
        try {
          const itemsRaw = String($('mpt-detail-items-json').value || '').trim();
          const parsedItems = JSON.parse(itemsRaw || '[]');
          const payload = {
            template_name: $('mpt-detail-name').value,
            content_type: $('mpt-detail-content-type').value || null,
            items: parsedItems,
          };
          const res = await fetch(apiUrl(`/v1/planner/monthly-planning-templates/${tpl.id}`), {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          if (!res.ok) throw new Error(await parseError(res));
          state.monthlyTemplates.activeTemplate = await res.json();
          renderMonthlyTemplateDetail();
          await loadMonthlyTemplates();
          setNote('Monthly template updated.');
        } catch (e) {
          setNote(`Template update failed: ${e.message}`);
        }
      });
      $('mpt-archive-btn').addEventListener('click', async () => {
        try {
          const res = await fetch(apiUrl(`/v1/planner/monthly-planning-templates/${tpl.id}/archive`), { method: 'POST' });
          if (!res.ok) throw new Error(await parseError(res));
          state.monthlyTemplates.activeTemplate = await res.json();
          renderMonthlyTemplateDetail();
          await loadMonthlyTemplates();
          setNote('Monthly template archived.');
        } catch (e) {
          setNote(`Archive failed: ${e.message}`);
        }
      });
      $('mpt-preview-open-btn').addEventListener('click', () => {
        $('mpt-preview-template-id').value = String(tpl.id || '');
        $('mpt-preview-channel-id').value = String(tpl.channel_id || $('mpt-channel-id').value || '');
        $('mpt-preview-modal').showModal();
      });
    }
  }

  async function loadMonthlyTemplateDetail(templateId) {
    const res = await fetch(apiUrl(`/v1/planner/monthly-planning-templates/${templateId}`));
    if (!res.ok) throw new Error(await parseError(res));
    state.monthlyTemplates.activeTemplate = await res.json();
    state.monthlyTemplates.activeTemplateId = Number(templateId);
    renderMonthlyTemplateDetail();
  }

  async function loadMonthlyTemplates() {
    const p = new URLSearchParams();
    const channel = String($('mpt-channel-id').value || '').trim();
    const status = String($('mpt-status').value || '').trim();
    const q = String($('mpt-q').value || '').trim();
    if (channel) p.set('channel_id', channel);
    if (status) p.set('status', status);
    if (q) p.set('q', q);
    const res = await fetch(apiUrl(`/v1/planner/monthly-planning-templates?${p.toString()}`));
    if (!res.ok) throw new Error(await parseError(res));
    const body = await res.json();
    state.monthlyTemplates.items = body.items || [];
    const rows = state.monthlyTemplates.items;
    const tbody = $('mpt-list-body');
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="8" class="muted">No templates in scope.</td></tr>';
      renderMonthlyTemplateDetail();
      return;
    }
    tbody.innerHTML = rows.map((item) => `<tr>
      <td>${esc(item.id)}</td>
      <td>${esc(item.template_name)}</td>
      <td>${esc(item.channel_id)}</td>
      <td>${esc(item.status)}</td>
      <td>${esc(item.item_count)}</td>
      <td>${esc(item.updated_at || '-')}</td>
      <td><pre style="white-space:pre-wrap; margin:0;">apply_run_count=${esc(item.apply_run_count ?? 0)}
last_applied_target_month=${esc(item.last_applied_target_month || '-')}
last_applied_at=${esc(item.last_applied_at || '-')}</pre></td>
      <td><button type="button" data-mpt-open="${esc(item.id)}">Open</button></td>
    </tr>`).join('');
    tbody.querySelectorAll('button[data-mpt-open]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const id = Number(btn.getAttribute('data-mpt-open'));
        if (!Number.isInteger(id) || id <= 0) return;
        try {
          await loadMonthlyTemplateDetail(id);
        } catch (e) {
          setNote(`Template load failed: ${e.message}`);
        }
      });
    });
  }

  async function runMonthlyTemplatePreview() {
    const templateId = Number($('mpt-preview-template-id').value);
    if (!Number.isInteger(templateId) || templateId <= 0) throw new Error('Template is required.');
    const channel_id = Number($('mpt-preview-channel-id').value);
    const target_month = String($('mpt-preview-target-month').value || '').trim();
    if (!Number.isInteger(channel_id) || channel_id <= 0) throw new Error('Valid channel_id is required.');
    if (!target_month) throw new Error('target_month is required.');
    const res = await fetch(apiUrl(`/v1/planner/monthly-planning-templates/${templateId}/preview-apply`), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel_id, target_month }),
    });
    if (!res.ok) throw new Error(await parseError(res));
    const body = await res.json();
    state.monthlyTemplates.preview = body;
    state.monthlyTemplates.applyResult = null;
    $('mpt-preview-json').textContent = JSON.stringify(body, null, 2);
    $('mpt-apply-json').textContent = 'No apply yet.';
    $('mpt-apply-run-btn').disabled = !body.preview_fingerprint;
    renderMonthlyTemplatePreviewItems();
    renderMonthlyTemplateApplyResult();
  }

  async function runMonthlyTemplateApply() {
    const templateId = Number($('mpt-preview-template-id').value);
    const preview = state.monthlyTemplates.preview;
    if (!preview || !preview.preview_fingerprint) throw new Error('Run preview first.');
    const payload = {
      channel_id: Number($('mpt-preview-channel-id').value),
      target_month: String($('mpt-preview-target-month').value || '').trim(),
      preview_fingerprint: preview.preview_fingerprint,
    };
    const res = await fetch(apiUrl(`/v1/planner/monthly-planning-templates/${templateId}/apply`), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(await parseError(res));
    const body = await res.json();
    state.monthlyTemplates.applyResult = body;
    $('mpt-apply-json').textContent = JSON.stringify(body, null, 2);
    renderMonthlyTemplateApplyResult();
    await loadMonthlyTemplates();
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
  $('refresh-readiness-btn').addEventListener('click', async () => { try { await loadList(); } catch (e) { setNote(e.message); } });
  $('prev-page').addEventListener('click', async () => { if (state.page > 1) { state.page -= 1; try { await loadList(); } catch (e) { setNote(e.message); } } });
  $('next-page').addEventListener('click', async () => { if (state.page * state.pageSize < state.total) { state.page += 1; try { await loadList(); } catch (e) { setNote(e.message); } } });
  $('sort-by').addEventListener('change', () => {
    $('readiness-priority').disabled = $('sort-by').value !== 'readiness_priority';
  });
  $('readiness-priority').disabled = $('sort-by').value !== 'readiness_priority';
  $('select-all').addEventListener('change', (e) => {
    state.selected.clear();
    if (e.target.checked) state.items.forEach((it) => state.selected.add(it.id));
    renderRows();
    $('bulk-delete-btn').disabled = !state.selected.size;
    updateMassActionSelectionState();
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
  $('mass-actions-open').addEventListener('click', () => {
    updateMassActionSelectionState();
    $('mass-actions-dialog').showModal();
  });
  $('pma-close-btn').addEventListener('click', () => $('mass-actions-dialog').close());
  $('pma-preview-btn').addEventListener('click', async () => {
    try {
      await createMassActionPreview();
      $('pma-execute-confirm').checked = false;
      refreshMassActionExecuteAvailability();
      setNote('Planner mass-action preview created.');
    } catch (e) {
      setNote(e.message);
    }
  });
  $('pma-execute-btn').addEventListener('click', async () => {
    try {
      await executeMassAction();
      setNote('Planner mass-action execute completed.');
      await loadList();
    } catch (e) {
      setNote(e.message);
    }
  });
  $('pma-filter-executable-only').addEventListener('change', renderMassActionItems);
  document.querySelectorAll('button[data-pma-kind-filter]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const targetKind = String(btn.getAttribute('data-pma-kind-filter') || 'ALL');
      state.massAction.selectedKinds = new Set([targetKind]);
      renderMassActionItems();
    });
  });
  $('pma-select-all-items').addEventListener('change', (e) => {
    const checked = e.target.checked;
    state.massAction.items.forEach((item) => { item._selected = checked; });
    renderMassActionItems();
  });
  $('pma-execute-confirm').addEventListener('change', refreshMassActionExecuteAvailability);
  $('pma-copy-summary-json-btn').addEventListener('click', async () => {
    try {
      await copyText($('pma-summary-json').textContent || '');
      setNote('Summary JSON copied.');
    } catch (e) {
      setNote(`Copy failed: ${e.message}`);
    }
  });
  $('pma-copy-result-json-btn').addEventListener('click', async () => {
    try {
      await copyText($('pma-result-json').textContent || '');
      setNote('Result JSON copied.');
    } catch (e) {
      setNote(`Copy failed: ${e.message}`);
    }
  });
  $('mpt-reload-btn').addEventListener('click', async () => {
    try {
      await loadMonthlyTemplates();
      setNote('Monthly templates reloaded.');
    } catch (e) {
      setNote(e.message);
    }
  });
  $('mpt-preview-close-btn').addEventListener('click', () => $('mpt-preview-modal').close());
  $('mpt-preview-run-btn').addEventListener('click', async () => {
    try {
      await runMonthlyTemplatePreview();
      setNote('Monthly template preview loaded.');
    } catch (e) {
      setNote(`Preview failed: ${e.message}`);
    }
  });
  $('mpt-apply-run-btn').addEventListener('click', async () => {
    try {
      await runMonthlyTemplateApply();
      setNote('Monthly template apply completed.');
    } catch (e) {
      setNote(`Apply failed: ${e.message}`);
    }
  });
  $('mpt-filter-createable-only').addEventListener('change', renderMonthlyTemplatePreviewItems);
  $('mpt-filter-conflicts-only').addEventListener('change', renderMonthlyTemplatePreviewItems);
  $('mpt-copy-preview-json-btn').addEventListener('click', async () => {
    try {
      await copyText($('mpt-preview-json').textContent || '');
      setNote('Preview JSON copied.');
    } catch (e) {
      setNote(`Copy failed: ${e.message}`);
    }
  });
  $('mpt-copy-apply-json-btn').addEventListener('click', async () => {
    try {
      await copyText($('mpt-apply-json').textContent || '');
      setNote('Apply result JSON copied.');
    } catch (e) {
      setNote(`Copy failed: ${e.message}`);
    }
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
  $('materialization-dialog-close').addEventListener('click', () => $('materialization-dialog').close());
  $('job-creation-dialog-close').addEventListener('click', () => $('job-creation-dialog').close());

  $('import-preview-body').innerHTML = previewPlaceholder('No preview yet.');
  setMetadataBulkStaleBanner(false);
  setMassActionStaleBanner(false);
  updateMassActionSelectionState();
  refreshMassActionExecuteAvailability();
  setInterval(updateMassActionCountdown, 1000);
  loadMonthlyTemplates().catch((e) => setNote(`Monthly templates load failed: ${e.message}`));
  loadList().catch((e) => setNote(e.message));
})();
