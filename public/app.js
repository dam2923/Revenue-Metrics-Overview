// Kato Board Dashboard — client-side metrics engine v2
// Reads data.json (raw deals + maps) → applies filters → recomputes all KPIs live.

const PALETTE = ['#5b8def','#8b5bef','#3ddc97','#f5a623','#ef5b5b','#23c4f5','#f55be0','#a8e05f','#5be0c4','#e0a55b'];
const fmt   = (n)=> new Intl.NumberFormat('en-GB').format(Math.round(n||0));
const money = (n)=> '£'+ new Intl.NumberFormat('en-GB',{maximumFractionDigits:0}).format(Math.round(n||0));
const moneyK= (n)=> { const v=Math.abs(n||0); if(v>=1e6) return '£'+(n/1e6).toFixed(2)+'m'; if(v>=1e3) return '£'+(n/1e3).toFixed(0)+'k'; return '£'+Math.round(n||0); };
const pct   = (n)=> (n*100).toFixed(1)+'%';

// === Data model constants ===
const BDR_IDS = new Set(['84985567','86092389','89038742','90829313']);
const AE_IDS  = new Set(['1697042347','87838658','89313122','629536424','80627342']);
const CS_IDS  = new Set(['631864499','81605895','80555135','76716019','385591848']);
const SALES_IDS = new Set([...BDR_IDS, ...AE_IDS]);

const RENEWAL_TERMINATED_STAGES = new Set(['175203154','205545808','1579301','205545807','946055240']);
const ACTIVE_WON_STAGES = new Set(['7cd021f6-89a1-494e-b028-cfdd284a0b4e','177605305']);
const SQL_STAGE = '946686494';
const NEW_BIZ_PIPELINE = 'ab692f0b-c232-4121-8802-3d56e83a8564';
const RENEWAL_PIPELINE = '79284585';

const DEALTYPE_PRO = 'newbusiness';
const DEALTYPE_ATLAS = 'Atlas';
const EXPANSION_TYPES = new Set(['expansion','existingbusiness','renewal']);
const NON_RECURRING_TYPES = new Set(['oneoff','Marketplace Advertising','PAF','Landlord API']);
const isActive = (d)=> d && (d.isActiveDeal===true || d.isActiveDeal==='true' || d.isActiveDeal==='Y' || d.isActiveDeal==='yes');
// Annual run rate per deal: prefer activeMrr×12, else fall back to totalCarr/uplift/amount.
// Used for deals on rolling-monthly contracts that don't have an end date and HubSpot hasn't computed MRR for.
const annualRR = (d)=> {
  if (!d || NON_RECURRING_TYPES.has(d.dealtype)) return 0;
  if (d.activeMrr) return Number(d.activeMrr)*12;
  return Number(d.totalCarr || d.uplift || d.amount || 0);
};

const LABEL_PRO = 'Kato Pro';
const LABEL_ATLAS = 'Atlas';
const LABEL_EXPANSION = 'Expansion';

// ---------- state ----------
let RAW = null;
let STATE = {
  tab: 'sales',        // 'sales' | 'marketing' | 'success'
  team: 'all',         // 'all' | 'sales' | 'cs'
  owners: new Set(),
  types:  new Set(),
  pipelines: new Set(),
  rangeKey: 'qtd',
  from:null, to:null,
  targets: { newBiz:500000, growth:100000, pipe:1500000, cov:2, marketingSpend: { q1:0, q2:0, q3:0, q4:0 } }
};
const charts = {};

// ---------- load ----------
async function load() {
  if (window.__BOARD_DATA__ || window.__BOARD_DATA) {
    RAW = window.__BOARD_DATA__ || window.__BOARD_DATA;
  } else {
    try {
      const res = await fetch('data.json?ts='+Date.now());
      if (!res.ok) throw new Error('data.json not found');
      RAW = await res.json();
    } catch (e) {
      showErr(e.message); return;
    }
  }
  document.getElementById('generatedAt').textContent = 'Refreshed '+new Date(RAW.generatedAt).toLocaleString('en-GB');
  document.getElementById('dealCount').textContent = (RAW.deals||[]).length + ' deals loaded';
  initFilters();
  applyRange(STATE.rangeKey);
  syncFromURL();
  setupTabs();
  recompute();
}
function showErr(m){ document.getElementById('generatedAt').textContent='Error: '+m; }

// ---------- Tab Navigation ----------
function setupTabs() {
  document.querySelectorAll('[data-tab]').forEach(btn => {
    btn.addEventListener('click', () => {
      STATE.tab = btn.dataset.tab;
      document.querySelectorAll('[data-tab]').forEach(b => b.classList.toggle('active', b.dataset.tab === STATE.tab));
      document.querySelectorAll('.tab-panel').forEach(p => p.style.display = p.dataset.panel === STATE.tab ? 'block' : 'none');
      recompute();
    });
  });
}

// ---------- URL state ----------
function syncFromURL() {
  const q = new URLSearchParams(location.hash.slice(1));
  if (q.get('tab')) STATE.tab = q.get('tab');
  ['newBiz','growth','pipe','cov'].forEach(k => { if (q.get('t_'+k)) STATE.targets[k] = Number(q.get('t_'+k)); });
  const owners = q.get('owners'); if (owners) owners.split(',').forEach(x=>STATE.owners.add(x));
  const types = q.get('types');   if (types)  types.split('|').forEach(x=>STATE.types.add(x));
  const pipes = q.get('pipes');   if (pipes)  pipes.split(',').forEach(x=>STATE.pipelines.add(x));
  if (q.get('range')) STATE.rangeKey = q.get('range');
  if (q.get('from')) STATE.from = new Date(q.get('from'));
  if (q.get('to'))   STATE.to   = new Date(q.get('to'));
  if (q.get('team')) STATE.team = q.get('team');
  ['tNewBiz','tGrowth','tPipe','tCov'].forEach((id,i)=>{
    const el = document.getElementById(id);
    if (el) el.value = [STATE.targets.newBiz,STATE.targets.growth,STATE.targets.pipe,STATE.targets.cov][i];
  });
}
function writeURL() {
  const q = new URLSearchParams();
  q.set('tab', STATE.tab);
  q.set('team', STATE.team);
  if (STATE.owners.size) q.set('owners', [...STATE.owners].join(','));
  if (STATE.types.size)  q.set('types',  [...STATE.types].join('|'));
  if (STATE.pipelines.size) q.set('pipes', [...STATE.pipelines].join(','));
  q.set('range', STATE.rangeKey);
  if (STATE.rangeKey==='custom' && STATE.from) q.set('from', STATE.from.toISOString().slice(0,10));
  if (STATE.rangeKey==='custom' && STATE.to)   q.set('to',   STATE.to.toISOString().slice(0,10));
  q.set('t_newBiz',STATE.targets.newBiz); q.set('t_growth',STATE.targets.growth);
  q.set('t_pipe',STATE.targets.pipe);     q.set('t_cov',STATE.targets.cov);
  try { history.replaceState(null,'','#'+q.toString()); } catch(_) { /* file:// / sandbox */ }
}

// ---------- filter UI ----------
function initFilters() {
  const ownerOpts = Object.entries(RAW.maps.owners||{}).map(([id,name])=>({id,name})).sort((a,b)=>a.name.localeCompare(b.name));
  const typeOpts = [...new Set((RAW.deals||[]).map(d=>d.dealtype).filter(Boolean))].sort().map(t=>({id:t,name:t}));
  const pipeOpts = Object.entries(RAW.maps.pipelines||{}).map(([id,name])=>({id,name}));
  buildMulti('ownerPicker',   ownerOpts, STATE.owners);
  buildMulti('typePicker',    typeOpts,  STATE.types);
  buildMulti('pipelinePicker',pipeOpts,  STATE.pipelines);

  // Team picker
  document.querySelectorAll('[data-team]').forEach(b => {
    b.addEventListener('click', () => {
      STATE.team = b.dataset.team;
      document.querySelectorAll('[data-team]').forEach(x => x.classList.toggle('active', x.dataset.team === STATE.team));
      // Re-filter owner options
      recompute();
    });
  });

  document.querySelectorAll('.daterange button').forEach(b => b.addEventListener('click', () => applyRange(b.dataset.range,true)));
  document.getElementById('dateFrom').addEventListener('change', e => { STATE.from = e.target.value?new Date(e.target.value):null; STATE.rangeKey='custom'; markRange(); recompute(); });
  document.getElementById('dateTo').addEventListener('change',   e => { STATE.to   = e.target.value?new Date(e.target.value):null; STATE.rangeKey='custom'; markRange(); recompute(); });

  ['tNewBiz','tGrowth','tPipe','tCov'].forEach((id,i)=>{
    const el = document.getElementById(id);
    if (el) {
      const key = ['newBiz','growth','pipe','cov'][i];
      el.addEventListener('input', e => { STATE.targets[key] = Number(e.target.value)||0; recompute(); });
    }
  });
  document.getElementById('refreshBtn').addEventListener('click', load);
  document.getElementById('exportBtn').addEventListener('click', exportExcel);
}
function buildMulti(id, options, selectedSet) {
  const el = document.getElementById(id);
  if (!el) return;
  const render = () => {
    const parts = [];
    const chips = [...selectedSet].slice(0,3).map(s => {
      const o = options.find(x=>x.id===s);
      return `<span class="chip">${esc(o?o.name:s)}</span>`;
    });
    if (selectedSet.size===0) parts.push(`<span class="ph">${el.dataset.placeholder||'All'}</span>`);
    else { parts.push(...chips); if (selectedSet.size>3) parts.push(`<span class="chip">+${selectedSet.size-3}</span>`); }
    parts.push(`<div class="drop">
      <input class="search" placeholder="Search…" />
      <div class="opt" data-id="__all"><input type="checkbox" ${selectedSet.size===0?'checked':''}/><span>All</span></div>
      ${options.map(o => `<div class="opt" data-id="${esc(o.id)}"><input type="checkbox" ${selectedSet.has(o.id)?'checked':''}/><span>${esc(o.name)}</span></div>`).join('')}
    </div>`);
    el.innerHTML = parts.join(' ');
  };
  render();
  el.addEventListener('click', e => {
    if (e.target.closest('.drop')) {
      const opt = e.target.closest('.opt');
      if (!opt) return;
      const oid = opt.dataset.id;
      if (oid === '__all') selectedSet.clear();
      else { if (selectedSet.has(oid)) selectedSet.delete(oid); else selectedSet.add(oid); }
      render(); recompute(); e.stopPropagation(); return;
    }
    el.classList.toggle('open');
  });
  el.addEventListener('input', e => {
    if (e.target.classList.contains('search')) {
      const q = e.target.value.toLowerCase();
      el.querySelectorAll('.drop .opt').forEach(o => {
        if (o.dataset.id==='__all') return;
        o.style.display = o.textContent.toLowerCase().includes(q) ? '' : 'none';
      });
      e.stopPropagation();
    }
  });
  document.addEventListener('click', e => { if (!el.contains(e.target)) el.classList.remove('open'); });
}
function applyRange(key, record) {
  STATE.rangeKey = key;
  const now = new Date();
  const d = new Date(now); d.setHours(0,0,0,0);
  let from=null, to=null;
  if (key==='wtd') { from = new Date(d); from.setDate(d.getDate() - ((d.getDay()+6)%7)); }
  else if (key==='mtd') { from = new Date(d.getFullYear(), d.getMonth(), 1); }
  else if (key==='qtd') { from = new Date(d.getFullYear(), Math.floor(d.getMonth()/3)*3, 1); }
  else if (key==='ytd') { from = new Date(d.getFullYear(),0,1); }
  else if (key==='12m') { from = new Date(d); from.setMonth(d.getMonth()-12); }
  else if (key==='all') { from = null; }
  STATE.from = from; STATE.to = now;
  markRange();
  if (from) document.getElementById('dateFrom').value = from.toISOString().slice(0,10);
  document.getElementById('dateTo').value = now.toISOString().slice(0,10);
  if (record) recompute();
}
function markRange() {
  document.querySelectorAll('.daterange button').forEach(b => b.classList.toggle('active', b.dataset.range===STATE.rangeKey));
}

// ---------- filter engine ----------
function inDateRange(iso) {
  if (!iso) return false;
  const t = Date.parse(iso); if (isNaN(t)) return false;
  if (STATE.from && t < STATE.from.getTime()) return false;
  if (STATE.to   && t > STATE.to.getTime()+86399999) return false;
  return true;
}
function passesScope(d) {
  if (STATE.owners.size && !STATE.owners.has(String(d.ownerId))) return false;
  if (STATE.types.size && !STATE.types.has(d.dealtype)) return false;
  if (STATE.pipelines.size && !STATE.pipelines.has(d.pipeline)) return false;
  return true;
}

// ---------- recompute + render ----------
function recompute() { try { _recompute(); } catch(e) { console.error('recompute error:', e); showErr('Render error: '+e.message); } }
function _recompute() {
  writeURL();
  if (!RAW || !RAW.deals) return;
  const deals = RAW.deals.filter(passesScope);

  // "Active" = HubSpot active_deal_ flag (rolling-monthly contracts with no end date are still active).
  // Fall back to isClosedWon && not-terminated for safety if flag missing.
  const activeWon = deals.filter(d => (isActive(d) || (d.isClosedWon && d.isActiveDeal===undefined))
                                   && !RENEWAL_TERMINATED_STAGES.has(String(d.stage)));
  const closedWonInRange = deals.filter(d => d.isClosedWon && !RENEWAL_TERMINATED_STAGES.has(String(d.stage)) && inDateRange(d.closedate));
  const closedLost = deals.filter(d => d.isClosed && !d.isClosedWon && !RENEWAL_TERMINATED_STAGES.has(String(d.stage)));
  const open = deals.filter(d => !d.isClosed);

  const totalCarr = sum(activeWon, d=>d.totalCarr);
  // Current ARR: source of truth is the line-item mrr___12 sum across all line items belonging to active-won/renewed deals.
  // Matches HubSpot's "New ARR Report" per RevOps. Falls back to per-deal annualRR if lineItems not in data.json.
  // ARR is computed from line items. When user applies filters (owner/type/pipeline), restrict to line items
  // whose parent deal passes the filter; otherwise sum all line items.
  let totalARR = 0;
  if (Array.isArray(RAW.lineItems) && RAW.lineItems.length) {
    // When no filters active, sum all line items (matches HubSpot RevOps ARR report exactly).
    // When filters applied, restrict to line items whose parent deal is in the filtered set.
    // Orphan line items (dealId not in our deal pull — archived deals) are included in the unfiltered total.
    const anyFilter = STATE.owners.size || STATE.types.size || STATE.pipelines.size;
    if (anyFilter) {
      const scopedDealIds = new Set(deals.map(d => String(d.id)));
      totalARR = RAW.lineItems.reduce((s,li)=> scopedDealIds.has(String(li.dealId)) ? s + (Number(li.mrr12)||0) : s, 0);
    } else {
      totalARR = RAW.lineItems.reduce((s,li)=> s + (Number(li.mrr12)||0), 0);
    }
  } else {
    totalARR = sum(activeWon, annualRR);
  }
  const activeMRR = totalARR/12;
  const grossNew  = sum(closedWonInRange.filter(d => [DEALTYPE_PRO, DEALTYPE_ATLAS].includes(d.dealtype)), d=>d.totalCarr||d.uplift);
  const expansion = sum(closedWonInRange.filter(d => EXPANSION_TYPES.has(d.dealtype)), d=>d.totalCarr||d.uplift);
  const totalArrInRange = grossNew + expansion;
  const newBizTarget = STATE.targets.newBiz;
  const growthTarget = STATE.targets.growth;
  const totalTarget  = newBizTarget + growthTarget;
  const targetDelta  = totalArrInRange - totalTarget;

  const openNewBiz = open.filter(d => d.pipeline===NEW_BIZ_PIPELINE && String(d.stage)!==SQL_STAGE);
  const openPipe   = sum(openNewBiz, d=>d.totalCarr||d.uplift||d.amount);
  const openPipeQ  = sum(openNewBiz.filter(d => inThisQuarter(d.closedate)), d=>d.totalCarr||d.uplift||d.amount);
  const coverageActual = newBizTarget ? openPipe/newBizTarget : 0;
  const coverageTarget = STATE.targets.cov;

  const twelveMonthsAgo = new Date(); twelveMonthsAgo.setMonth(twelveMonthsAgo.getMonth()-12);
  const aov12m = activeWon.filter(d => [DEALTYPE_PRO, DEALTYPE_ATLAS].includes(d.dealtype) && d.closedate && new Date(d.closedate)>=twelveMonthsAgo);
  const aov = aov12m.length ? sum(aov12m, d=>d.totalCarr||d.uplift)/aov12m.length : 0;

  const lostInRange = closedLost.filter(d => inDateRange(d.closedate));
  const winRate = (closedWonInRange.length+lostInRange.length) ? closedWonInRange.length/(closedWonInRange.length+lostInRange.length) : 0;

  // Render based on tab
  if (STATE.tab === 'sales') {
    renderSalesTab(deals, activeWon, closedWonInRange, open, openNewBiz, {
      totalCarr, totalARR, grossNew, expansion, openPipe, openPipeQ, aov, winRate,
      newBizTarget, growthTarget, targetDelta, coverageActual, coverageTarget, aov12m
    });
  } else if (STATE.tab === 'marketing') {
    renderMarketingTab(closedWonInRange);
  } else if (STATE.tab === 'success') {
    renderSuccessTab();
  }

  document.getElementById('assumptions').textContent =
    'Rules: cARR=total_carr on active won (excl. renewals/terminated). ARR=Active MRR × 12. New Logo=(Pro|Atlas).';
}

// ===== SALES TAB =====
function renderSalesTab(deals, activeWon, closedWonInRange, open, openNewBiz, metrics) {
  // KPI tiles
  set('kpi-arr',  money(metrics.totalARR), 'MRR £'+fmt(metrics.totalARR/12)+'/mo × 12');
  set('kpi-gross', money(metrics.grossNew), `${closedWonInRange.filter(d=>[DEALTYPE_PRO,DEALTYPE_ATLAS].includes(d.dealtype)).length} new logos`);
  set('kpi-exp',  money(metrics.expansion), `${closedWonInRange.filter(d=>EXPANSION_TYPES.has(d.dealtype)).length} expansion`);
  set('kpi-pipe',  money(metrics.openPipe),  `${openNewBiz.length} deals (SQO+)`);
  set('kpi-cov',   metrics.coverageActual.toFixed(2)+'×', `Target ${metrics.coverageTarget}×`, metrics.coverageActual>=metrics.coverageTarget?'pos':'neg');
  set('kpi-aov',   money(metrics.aov), `${metrics.aov12m.length} new logos last 12m`);

  // Charts
  renderARRChart(activeWon);
  renderStageChart(openNewBiz);
  renderTypeChart(closedWonInRange);

  // Tables
  renderOwnerTable(deals);
  renderConversionTables();
  renderActivityLeaderboard();
  renderTopDealsTable(openNewBiz);
}

// ===== MARKETING TAB =====
function renderMarketingTab(closedWonInRange) {
  // MQL inbounds, pipeline created, conversion rates
  const weeks = RAW.contacts?.createdByWeek || [];
  set('kpi-mql', fmt(weeks.length ? weeks[weeks.length-1].count : 0), 'This week');
  set('kpi-pipe-created', money(sum(closedWonInRange, d=>d.uplift)), 'Deals created in period');
  document.querySelector('#marketing-panel').innerHTML = '<p>Marketing metrics — WIP</p>';
}

// ===== CUSTOMER SUCCESS TAB =====
function renderSuccessTab() {
  document.querySelector('#success-panel').innerHTML = '<p>Customer success metrics — WIP</p>';
}

// ---------- helper: set KPI ----------
function set(id,val,sub,cls){
  const el = document.getElementById(id);
  if (el) {
    el.textContent = val;
    const subEl = document.getElementById(id+'-sub');
    if (subEl) { subEl.textContent = sub||''; subEl.className = 'k-sub '+(cls||''); }
  }
}
function sum(arr, fn){ return arr.reduce((a,b)=>a + (Number(fn(b))||0), 0); }
function inThisQuarter(iso) {
  if (!iso) return false;
  const t = new Date(iso); const n = new Date();
  if (t.getFullYear()!==n.getFullYear()) return false;
  return Math.floor(t.getMonth()/3) === Math.floor(n.getMonth()/3);
}
function esc(s){return String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}

// ---------- charts ----------
function destroy(id){ if (charts[id]){ charts[id].destroy(); delete charts[id]; } }

function lastNMonths(n){
  const out=[]; const d=new Date(); d.setDate(1);
  for (let i=n-1;i>=0;i--) { const x=new Date(d); x.setMonth(d.getMonth()-i); out.push(x.toISOString().slice(0,7)); }
  return out;
}
function renderARRChart(activeWon){
  const months = lastNMonths(12);
  const data = months.map(m => sum(activeWon.filter(d=>(d.closedate||'').slice(0,7)===m && ([DEALTYPE_PRO,DEALTYPE_ATLAS].includes(d.dealtype)||EXPANSION_TYPES.has(d.dealtype))), d=>d.totalCarr||d.uplift));
  const target = (STATE.targets.newBiz + STATE.targets.growth) / 12;
  destroy('chart-arr');
  const can = document.getElementById('chart-arr');
  if (can && typeof Chart !== 'undefined') try { charts['chart-arr'] = new Chart(can,{
    type:'bar',
    data:{ labels:months, datasets:[
      {label:'New ARR (Gross+Exp)', data, backgroundColor:PALETTE[0]+'cc'},
      {label:'Monthly target', type:'line', data:months.map(_=>target), borderColor:PALETTE[3], borderDash:[5,5], pointRadius:0, borderWidth:2}
    ]},
    options:chartOpts()
  }); } catch(e) { console.warn('chart-arr failed:',e); }
}
function renderStageChart(open){
  const buckets = {};
  open.forEach(d => { const s = RAW.maps.stages[d.stage]||d.stage; buckets[s] = (buckets[s]||0) + (Number(d.totalCarr||d.uplift||d.amount)||0); });
  const entries = Object.entries(buckets).sort((a,b)=>b[1]-a[1]);
  destroy('chart-stage');
  const can = document.getElementById('chart-stage');
  if (can && typeof Chart !== 'undefined') try { charts['chart-stage'] = new Chart(can,{
    type:'bar',
    data:{ labels:entries.map(e=>e[0]), datasets:[{data:entries.map(e=>e[1]), backgroundColor:entries.map((_,i)=>PALETTE[i%PALETTE.length])}]},
    options:{...chartOpts(), indexAxis:'y', plugins:{legend:{display:false}}}
  }); } catch(e) { console.warn('chart-stage failed:',e); }
}
function renderTypeChart(won){
  const buckets = {};
  won.forEach(d => { const t=d.dealtype||'—'; buckets[t]=(buckets[t]||0)+(Number(d.totalCarr||d.uplift)||0); });
  const entries = Object.entries(buckets).sort((a,b)=>b[1]-a[1]);
  destroy('chart-type');
  const can = document.getElementById('chart-type');
  if (can && typeof Chart !== 'undefined') try { charts['chart-type'] = new Chart(can,{
    type:'doughnut',
    data:{ labels:entries.map(e=>e[0]), datasets:[{data:entries.map(e=>e[1]), backgroundColor:entries.map((_,i)=>PALETTE[i%PALETTE.length])}]},
    options:{...chartOpts(), plugins:{legend:{position:'right',labels:{color:'#9aa6c0',font:{size:10}}}}}
  }); } catch(e) { console.warn('chart-type failed:',e); }
}
function chartOpts(){
  return { responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{ labels:{color:'#9aa6c0', font:{size:11}}}},
    scales:{ x:{ticks:{color:'#9aa6c0',font:{size:10}},grid:{color:'#27314a'}}, y:{ticks:{color:'#9aa6c0',font:{size:10}},grid:{color:'#27314a'}}}
  };
}

// ---------- tables ----------
function renderOwnerTable(deals) {
  const bk = {};
  deals.forEach(d => {
    const name = RAW.maps.owners[d.ownerId] || 'Unassigned';
    bk[name] = bk[name] || {name, newARR:0, openPipe:0, closingQ:0, wonCount:0, openCount:0, lostCount:0};
    const val = Number(d.totalCarr||d.uplift||d.amount)||0;
    if (d.isClosedWon && !RENEWAL_TERMINATED_STAGES.has(String(d.stage)) && inDateRange(d.closedate)) {
      bk[name].newARR += val; bk[name].wonCount++;
    }
    if (!d.isClosed && d.pipeline===NEW_BIZ_PIPELINE && String(d.stage)!==SQL_STAGE) {
      bk[name].openPipe += val; bk[name].openCount++;
      if (inThisQuarter(d.closedate)) bk[name].closingQ += val;
    }
    if (d.isClosed && !d.isClosedWon && !RENEWAL_TERMINATED_STAGES.has(String(d.stage)) && inDateRange(d.closedate)) bk[name].lostCount++;
  });
  const rows = Object.values(bk).sort((a,b)=>b.newARR-a.newARR).slice(0,15);
  const tbl = document.querySelector('#table-owners tbody');
  if (tbl) tbl.innerHTML = rows.map(r => `
    <tr><td>${esc(r.name)}</td>
    <td class="num">${money(r.newARR)}</td>
    <td class="num">${money(r.openPipe)}</td>
    <td class="num">${money(r.closingQ)}</td>
    <td class="num">${fmt(r.wonCount)}</td>
    <td class="num">${fmt(r.openCount)}</td>
    <td class="num">${(r.wonCount+r.lostCount)?pct(r.wonCount/(r.wonCount+r.lostCount)):'—'}</td></tr>
  `).join('') || '<tr><td colspan="7">No data</td></tr>';
}

function renderConversionTables() {
  // AE Pro Conversion: wins / (wins+opens) by AE, for dealtype=newbusiness, ownerId in AE_IDS
  // If convClosed missing, show placeholder
  const deals = RAW.deals || [];
  const aeProWins = {};
  const aeAtlasWins = {};

  deals.filter(d => AE_IDS.has(String(d.ownerId))).forEach(d => {
    const name = RAW.maps.owners[d.ownerId] || 'Unknown';
    if (d.dealtype === DEALTYPE_PRO) {
      aeProWins[name] = aeProWins[name] || { name, wins: 0, opens: 0 };
      if (d.convClosed === 1) aeProWins[name].wins++;
      else if (d.convClosed === 0) aeProWins[name].opens++;
    }
    if (d.dealtype === DEALTYPE_ATLAS) {
      aeAtlasWins[name] = aeAtlasWins[name] || { name, wins: 0, opens: 0 };
      if (d.convClosed === 1) aeAtlasWins[name].wins++;
      else if (d.convClosed === 0) aeAtlasWins[name].opens++;
    }
  });

  const proRows = Object.values(aeProWins).map(r => ({
    Rep: r.name,
    Wins: fmt(r.wins),
    Opens: fmt(r.opens),
    Rate: (r.wins+r.opens) ? pct(r.wins/(r.wins+r.opens)) : '—'
  }));
  const atlasRows = Object.values(aeAtlasWins).map(r => ({
    Rep: r.name,
    Wins: fmt(r.wins),
    Opens: fmt(r.opens),
    Rate: (r.wins+r.opens) ? pct(r.wins/(r.wins+r.opens)) : '—'
  }));

  const tbl1 = document.querySelector('#table-pro-conv tbody');
  const tbl2 = document.querySelector('#table-atlas-conv tbody');
  if (tbl1) tbl1.innerHTML = proRows.length ? proRows.map(r => `<tr><td>${r.Rep}</td><td class="num">${r.Wins}</td><td class="num">${r.Opens}</td><td class="num">${r.Rate}</td></tr>`).join('') : '<tr><td colspan="4">—</td></tr>';
  if (tbl2) tbl2.innerHTML = atlasRows.length ? atlasRows.map(r => `<tr><td>${r.Rep}</td><td class="num">${r.Wins}</td><td class="num">${r.Opens}</td><td class="num">${r.Rate}</td></tr>`).join('') : '<tr><td colspan="4">—</td></tr>';
}

function renderActivityLeaderboard() {
  // Display per-person meetings + calls + emails from activities.byAssignedOwnerWeek
  const acts = RAW.activities || {};
  const byOwner = {};

  (acts.byAssignedOwnerWeek || []).forEach(a => {
    if (!byOwner[a.ownerId]) byOwner[a.ownerId] = { meetings: 0, calls: 0, emails: 0 };
    byOwner[a.ownerId].meetings += a.meetings || 0;
    byOwner[a.ownerId].calls += a.calls || 0;
    byOwner[a.ownerId].emails += a.emails || 0;
  });

  const rows = Object.entries(byOwner)
    .map(([oid, acts]) => ({ rep: RAW.maps.owners[oid] || oid, ...acts }))
    .sort((a,b) => (b.meetings+b.calls+b.emails) - (a.meetings+a.calls+a.emails));

  const tbl = document.querySelector('#table-activities tbody');
  if (tbl) tbl.innerHTML = rows.slice(0,15).map(r => `
    <tr><td>${esc(r.rep)}</td><td class="num">${fmt(r.meetings)}</td><td class="num">${fmt(r.calls)}</td><td class="num">${fmt(r.emails)}</td></tr>
  `).join('') || '<tr><td colspan="4">No activities</td></tr>';
}

function renderTopDealsTable(openNewBiz) {
  const rows = openNewBiz.slice()
    .sort((a,b)=>(Number(b.totalCarr||b.uplift||0))-(Number(a.totalCarr||a.uplift||0)))
    .slice(0,20);
  const tbl = document.querySelector('#table-topdeals tbody');
  if (tbl) tbl.innerHTML = rows.map(d => `
    <tr><td>${esc(d.name)}</td>
    <td>${esc(RAW.maps.owners[d.ownerId]||'—')}</td>
    <td>${esc(d.dealtype||'—')}</td>
    <td>${esc(RAW.maps.stages[d.stage]||d.stage)}</td>
    <td class="num">${money(d.totalCarr)}</td>
    <td class="num">${money(d.uplift)}</td>
    <td>${d.closedate?new Date(d.closedate).toLocaleDateString('en-GB'):'—'}</td></tr>
  `).join('') || '<tr><td colspan="7">No open deals</td></tr>';
}

// ---------- Excel export ----------
function exportExcel() {
  if (!RAW) return;
  const wb = XLSX.utils.book_new();
  const add = (name, rows) => XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rows||[]), name.slice(0,31));

  const deals = (RAW.deals||[]).filter(passesScope);
  const activeWon = deals.filter(d => d.isClosedWon && !RENEWAL_TERMINATED_STAGES.has(String(d.stage)));

  add('KPIs', Array.from(document.querySelectorAll('.kpi')).map(k => ({
    Metric: k.querySelector('.k-label')?.textContent.trim() || '',
    Value:  k.querySelector('.k-value')?.textContent.trim() || '',
    Note:   k.querySelector('.k-sub')?.textContent.trim() || ''
  })));
  add('All deals (filtered)', deals.map(d => ({
    Id:d.id, Name:d.name, Owner: RAW.maps.owners[d.ownerId]||'',
    Pipeline: RAW.maps.pipelines[d.pipeline]||'', Stage: RAW.maps.stages[d.stage]||'',
    Type:d.dealtype, Created:d.createdate, Closedate:d.closedate,
    Amount:d.amount, Uplift:d.uplift, TotalCARR:d.totalCarr, ForecastCat:d.forecastCategory||'—'
  })));
  add('Active Won deals', activeWon.map(d => ({
    Id:d.id, Name:d.name, Owner: RAW.maps.owners[d.ownerId]||'',
    Type:d.dealtype, Closedate:d.closedate, TotalCARR:d.totalCarr
  })));
  add('Contacts by lifecycle', RAW.contacts?.byLifecycle||[]);

  XLSX.writeFile(wb, `kato-board-${new Date().toISOString().slice(0,10)}.xlsx`);
}

load();
