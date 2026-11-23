import {heading, panel} from '../components/common/heading.js';
import {entries, records} from '../components/common/records.js';
import {topic} from '../constants/project.js';
import {date, el, esc} from '../utils/dom.js';
import {submit} from '../hooks/submit.js';

function inventoryCallout(report) {
  if (report.signable !== false) {
    return '<p class="note" data-testid="signable">盘点账已与发布单逐点核对，可以签署。</p>';
  }
  const blockers = (report.blockers || []).map(x=>`<li>${esc(x)}</li>`).join('');
  return `<div class="error" data-testid="blockers"><strong>发布单不能签署</strong>${blockers?`<ul>${blockers}</ul>`:''}</div>`;
}

export function evidencePage(state, identity) {
  const current = state.evidence.find(row=>row.id===identity) || state.evidence[0];
  if (!current) return heading(topic.modules[3]) + '<p class="empty">执行完成后，可查看、复核和导出证据。</p>';
  const {report} = current;
  const metrics = Object.entries(report.metrics).map(([label,value])=>`<div><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`).join('');
  const sections = report.sections.map(x=>panel(x.title, entries(x.rows))).join('');
  const reviewed = Boolean(current.reviewer);
  const signable = report.signable !== false;
  const review = reviewed ? `<p data-testid="reviewed">${esc(current.reviewer)} · ${esc(date(current.reviewed_at))}</p><p>${esc(current.review_note)}</p>`
    : !signable ? '<p class="note">发布单存在差额或待核盘点，需补齐后由复核员签署。</p>'
    : state.session.role==='reviewer'?'<form id="review-form"><label>复核意见<textarea name="note" maxlength="500" required></textarea></label><button>签署复核</button></form>'
    : '<p class="note">待复核员签署</p>';
  const lateLedger = !reviewed && state.session.role==='operator'
    ? `<form id="ledger-form"><h2>补交盘点记录</h2><p class="note">按原始位点核对整区牌面，缺号未到的盘点先挂起。已签署的发布单不再接受补交。</p>
      <label>分区流<input name="stream" maxlength="64" required placeholder="NORTH" /></label>
      <label>流内序号<input name="sequence" type="number" min="0" step="1" required /></label>
      <label>整区在用牌面（JSON 数组）<textarea name="signs" rows="5" spellcheck="false" required>[{"sign_id":"N-07","content":"桂花米酿｜18元"}]</textarea></label>
      <button>补交并核对</button></form>`
    : '';
  const history = state.evidence.map(row=>[`<a href="#evidence/${row.id}">执行 ${row.run_id}</a>`, esc(row.report.summary), esc(row.reviewer || '待复核')]);
  return `${heading(topic.modules[3], report.summary, `<button id="download" data-id="${current.id}">导出证据</button>`)}
    <div class="spread evidence-top"><dl class="metrics">${metrics}</dl><aside class="brief"><h2>复核签署</h2>${inventoryCallout(report)}${review}</aside></div>
    ${sections}<details><summary>历史证据</summary>${records(['执行','结论','复核'], history)}</details>
    ${lateLedger?`<aside class="brief ledger-form">${lateLedger}</aside>`:''}`;
}

export function bindEvidence(api, update, fail) {
  const button = document.getElementById('download');
  if (!button) return;
  const identity = button.dataset.id;
  button.addEventListener('click', async()=>{
    try {
      const response = await api(`/evidence/${identity}/download`, {response:true});
      const url = URL.createObjectURL(await response.blob());
      const link = el('a');
      link.href = url;
      link.download = `evidence-${identity}.json`;
      link.click();
      setTimeout(()=>URL.revokeObjectURL(url), 1000);
    } catch (error) { fail(error); }
  });
  submit('review-form', async data=>{
    await api(`/evidence/${identity}/review`, {method:'POST', data:{note:data.get('note')}});
    await update();
  }, fail);
  submit('ledger-form', async data=>{
    let signs;
    try { signs = JSON.parse(data.get('signs')); } catch { throw new Error('整区在用牌面需为 JSON 数组。'); }
    await api(`/evidence/${identity}/ledgers`, {method:'POST', data:{ledger:{
      stream:data.get('stream').trim(),
      sequence:Number(data.get('sequence')),
      signs,
    }}});
    await update();
  }, fail);
}
