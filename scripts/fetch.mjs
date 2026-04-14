// scripts/fetch.mjs
// Pulls live HubSpot data and writes public/data.json
// Run locally:   HUBSPOT_TOKEN=pat-xxxx npm run fetch
// Run in CI:     GitHub Action injects HUBSPOT_TOKEN from repo secrets.

import fs from 'node:fs/promises';
import path from 'node:path';

const TOKEN = process.env.HUBSPOT_TOKEN;
if (!TOKEN) {
  console.error('Missing HUBSPOT_TOKEN env var. Create a HubSpot Private App with CRM read scopes and export its access token.');
  process.exit(1);
}

const BASE = 'https://api.hubapi.com';
const HDR  = { 'Authorization': `Bearer ${TOKEN}`, 'Content-Type':'application/json' };

const PIPELINES = {
  'ab692f0b-c232-4121-8802-3d56e83a8564': 'New, Upsell, Additional & One Off',
  '79284585': 'Renewal Pipeline',
  '147097165': 'Landlord Revenue',
  '29715543': 'Fundraising Pipeline',
};

async function api(path, body) {
  const res = await fetch(BASE + path, { method:'POST', headers: HDR, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(`${path} → ${res.status} ${await res.text()}`);
  return res.json();
}
async function get(path) {
  const res = await fetch(BASE + path, { headers: HDR });
  if (!res.ok) throw new Error(`${path} → ${res.status} ${await res.text()}`);
  return res.json();
}

// --- Helpers ---
const monthKey = (iso) => iso ? iso.slice(0,7) : null;
const sum = (arr, fn) => arr.reduce((a,b)=>a + (Number(fn(b))||0), 0);
const lastNMonths = (n) => {
  const out = [];
  const d = new Date(); d.setDate(1);
  for (let i=n-1;i>=0;i--) {
    const x = new Date(d); x.setMonth(d.getMonth()-i);
    out.push(x.toISOString().slice(0,7));
  }
  return out;
};
const daysAgoMs = (d) => Date.now() - d*86400000;

// --- Pull line items for active-ARR deals (drives Current ARR) ---
// Matches HubSpot's RevOps "New ARR Report": sum of mrr___12 across line items belonging to deals
// whose stage starts with "Won:" or equals "Contract Renewed" / "Renewed".
async function pullLineItems(transformedDeals) {
  // Identify ARR-contributing deal IDs from the already-transformed deal list by stage label.
  const activeStageIds = new Set();
  // Caller passes `deals` which has {stage: <id>}; we need stage labels. Re-fetch stage map quickly here.
  const stagesJson = await get('/crm/v3/pipelines/deals');
  const stageLabelById = {};
  (stagesJson.results||[]).forEach(p => (p.stages||[]).forEach(s => {
    stageLabelById[s.id] = s.label;
  }));
  const isArrStage = (label) => !!label && (label.startsWith('Won:') || label==='Contract Renewed' || label==='Renewed');
  const targetDealIds = transformedDeals
    .filter(d => isArrStage(stageLabelById[d.stage]))
    .map(d => String(d.id));
  if (!targetDealIds.length) return [];

  // Step 1: batch-fetch deals with line-item associations
  const dealIdToLineItemIds = {};
  for (let i=0; i<targetDealIds.length; i+=50) {
    const batch = targetDealIds.slice(i, i+50);
    const j = await api('/crm/v3/objects/deals/batch/read', {
      properties: ['dealname'],
      propertiesWithHistory: [],
      inputs: batch.map(id => ({id})),
      associations: ['line_items']
    }).catch(()=>null);
    if (!j) continue;
    for (const d of (j.results||[])) {
      const liIds = ((d.associations?.['line items']?.results) || (d.associations?.line_items?.results) || []).map(r=>String(r.id));
      if (liIds.length) dealIdToLineItemIds[String(d.id)] = liIds;
    }
  }
  const allLineItemIds = [...new Set(Object.values(dealIdToLineItemIds).flat())];
  if (!allLineItemIds.length) return [];

  // Step 2: batch-fetch line item properties
  const items = [];
  const props = ['mrr___12','name','quantity','price','hs_billing_start_date','hs_billing_end_date','hs_recurring_billing_period'];
  for (let i=0; i<allLineItemIds.length; i+=100) {
    const batch = allLineItemIds.slice(i, i+100);
    const j = await api('/crm/v3/objects/line_items/batch/read', {
      properties: props,
      inputs: batch.map(id => ({id}))
    }).catch(()=>null);
    if (!j) continue;
    for (const li of (j.results||[])) {
      const p = li.properties || {};
      // find parent deal
      let dealId = null;
      for (const [did, ids] of Object.entries(dealIdToLineItemIds)) {
        if (ids.includes(String(li.id))) { dealId = did; break; }
      }
      items.push({
        id: String(li.id),
        dealId,
        name: p.name || '',
        mrr12: Number(p.mrr___12) || 0,
        quantity: Number(p.quantity) || 1,
        price: Number(p.price) || 0,
        billingStart: p.hs_billing_start_date || '',
        billingEnd: p.hs_billing_end_date || '',
        period: p.hs_recurring_billing_period || ''
      });
    }
  }
  console.log(`Line items: ${items.length} across ${Object.keys(dealIdToLineItemIds).length} deals, sum mrr12 = £${items.reduce((s,x)=>s+x.mrr12,0).toFixed(2)}`);
  return items;
}

// --- Pull deals (paginated) ---
async function pullDeals() {
  const props = ['dealname','amount','dealstage','pipeline','closedate','createdate','hs_is_closed_won','hs_is_closed','hubspot_owner_id','hs_arr','hs_mrr','hs_acv','hs_tcv','dealtype','hs_deal_stage_probability','conversion_rate_calculator','hs_manual_forecast_category','hs_created_by_user_id','hubspot_owner_assigneddate'];
  let after = undefined, all = [];
  for (;;) {
    const body = { limit:100, properties: props, sorts:[{propertyName:'createdate', direction:'DESCENDING'}] };
    if (after) body.after = after;
    const j = await api('/crm/v3/objects/deals/search', body);
    all = all.concat(j.results || []);
    if (!j.paging?.next?.after) break;
    after = j.paging.next.after;
    if (all.length >= 5000) break; // safety
  }
  return all;
}

async function countContacts(filter) {
  const body = { limit:1, filterGroups:[{ filters:[filter] }] };
  const j = await api('/crm/v3/objects/contacts/search', body);
  return j.total || 0;
}
async function countDeals(filter) {
  const body = { limit:1, filterGroups:[{ filters:[filter] }] };
  const j = await api('/crm/v3/objects/deals/search', body);
  return j.total || 0;
}

async function getOwners() {
  const j = await get('/crm/v3/owners?limit=200');
  const map = {};
  (j.results||[]).forEach(o => map[o.id] = `${o.firstName||''} ${o.lastName||''}`.trim() || o.email || o.id);
  return map;
}

async function getStageMap() {
  // Pull all pipelines + stages
  const j = await get('/crm/v3/pipelines/deals');
  const map = {};
  (j.results||[]).forEach(p => (p.stages||[]).forEach(s => map[s.id] = s.label));
  return map;
}

async function pullEngagements() {
  // Pull meetings, calls, emails from last 90 days
  // Aggregate by owner week + createdBy week for pivoting
  const activitiesByAssignedOwner = [];
  const activitiesByCreatedBy = [];
  const ninetyDaysAgo = Date.now() - 90*86400000;

  const engagement_types = [
    { type: 'meetings', field: 'meetings' },
    { type: 'calls', field: 'calls' },
    { type: 'emails', field: 'emails' }
  ];

  for (const eng of engagement_types) {
    let after = undefined;
    const bucketAssigned = {};  // "ownerId|YYYY-MM-DD" -> {ownerId, week, meetings/calls/emails}
    const bucketCreated = {};   // "createdBy|YYYY-MM-DD" -> {createdBy, week, meetings/calls/emails}

    for (;;) {
      const body = {
        limit: 100,
        properties: ['hubspot_owner_id', 'hs_created_by_user_id', 'hs_timestamp'],
        filterGroups: [{
          filters: [{
            propertyName: 'hs_timestamp',
            operator: 'GTE',
            value: ninetyDaysAgo
          }]
        }]
      };
      if (after) body.after = after;

      try {
        const j = await api(`/crm/v3/objects/${eng.type}/search`, body);
        (j.results || []).forEach(e => {
          const ownerId = e.properties.hubspot_owner_id || null;
          const createdBy = e.properties.hs_created_by_user_id || null;
          const ts = e.properties.hs_timestamp || Date.now();
          const week = new Date(Number(ts)).toISOString().slice(0,10).replace(/-\d\d$/, d => {
            const date = new Date(Number(ts));
            const mon = new Date(date);
            mon.setDate(date.getDate() - ((date.getDay()+6)%7));
            return mon.toISOString().slice(0,10);
          });

          // Actually compute the correct Monday
          const date = new Date(Number(ts));
          const dayOfWeek = date.getDay();
          const diff = date.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1);
          const monday = new Date(date.setDate(diff));
          const weekStr = monday.toISOString().slice(0,10);

          if (ownerId) {
            const keyA = ownerId + '|' + weekStr;
            bucketAssigned[keyA] = bucketAssigned[keyA] || { ownerId, week: weekStr, meetings: 0, calls: 0, emails: 0 };
            bucketAssigned[keyA][eng.field]++;
          }
          if (createdBy) {
            const keyC = createdBy + '|' + weekStr;
            bucketCreated[keyC] = bucketCreated[keyC] || { createdBy, week: weekStr, meetings: 0, calls: 0, emails: 0 };
            bucketCreated[keyC][eng.field]++;
          }
        });

        if (!j.paging?.next?.after) break;
        after = j.paging.next.after;
      } catch (err) {
        console.warn(`Engagement pull for ${eng.type} failed:`, err.message);
        break;
      }
    }

    Object.values(bucketAssigned).forEach(a => {
      const existing = activitiesByAssignedOwner.find(x => x.ownerId === a.ownerId && x.week === a.week);
      if (existing) {
        existing[eng.field] = (existing[eng.field]||0) + a[eng.field];
      } else {
        activitiesByAssignedOwner.push(a);
      }
    });

    Object.values(bucketCreated).forEach(c => {
      const existing = activitiesByCreatedBy.find(x => x.createdBy === c.createdBy && x.week === c.week);
      if (existing) {
        existing[eng.field] = (existing[eng.field]||0) + c[eng.field];
      } else {
        activitiesByCreatedBy.push(c);
      }
    });
  }

  return { byAssignedOwnerWeek: activitiesByAssignedOwner, byCreatedByWeek: activitiesByCreatedBy };
}

async function computeFunnel() {
  // Weekly deal funnel: sqlDeals (deals entering SQL stage), oppsCreated (deals created in New Biz pipeline)
  // sqlDeals requires hs_v2_date_entered_STAGEID which is complex; we'll use null/comment for now
  // oppsCreated = deals created in New Biz pipeline by week
  const weeks = [];
  const d = new Date(); d.setDate(d.getDate() - 84); // last 12 weeks
  for (let i = 0; i < 12; i++) {
    const mon = new Date(d);
    mon.setDate(d.getDate() + i*7 - ((d.getDay()+6)%7));
    weeks.push(mon.toISOString().slice(0,10));
  }

  // For now, return skeleton with nulls
  // In a full impl, query deals by createdate + pipeline
  const funnel = weeks.map(w => ({
    week: w,
    mqlContacts: 0,     // not computable without marketing data
    sqlDeals: null,     // requires stage entry timestamps
    oppsCreated: null   // would query NEW_BIZ_PIPELINE deals created that week
  }));

  return { weekly: funnel };
}

// --- Transform deals to client format ---
function transformDeals(rawDeals, owners) {
  return rawDeals.map(d => {
    const p = d.properties;
    return {
      id: d.id,
      name: p.dealname,
      ownerId: p.hubspot_owner_id,
      pipeline: p.pipeline,
      stage: p.dealstage,
      dealtype: p.dealtype,
      createdate: p.createdate,
      closedate: p.closedate,
      isClosed: p.hs_is_closed === 'true',
      isClosedWon: p.hs_is_closed_won === 'true',
      probability: Number(p.hs_deal_stage_probability || 0.3),
      amount: Number(p.amount || 0),
      uplift: Number(p.amount || 0),  // normalized to amount; app.js may override
      totalCarr: Number(p.hs_arr || p.hs_tcv || p.amount || 0),
      activeMrr: Number(p.hs_mrr || 0),
      activeArr: Number(p.hs_arr || 0),
      effectiveArr: Number(p.hs_arr || Number(p.hs_mrr || 0) * 12),
      convClosed: p.conversion_rate_calculator ? (p.conversion_rate_calculator === 'true' || p.conversion_rate_calculator === true ? 1 : (p.conversion_rate_calculator === 'false' || p.conversion_rate_calculator === false ? 0 : null)) : null,
      forecastCategory: p.hs_manual_forecast_category || null,
      createdBy: p.hs_created_by_user_id || null,
      ownerAssignedDate: p.hubspot_owner_assigneddate || null,
      // Keep as-is from spec (if data.json had them)
      wonActiveDate: null,
      wonOnboardingStartedDate: null,
      isActiveDeal: null,
      users: null
    };
  });
}

// --- Main ---
async function main() {
  console.log('Fetching deals…');
  const rawDeals = await pullDeals();
  console.log('  got', rawDeals.length);

  console.log('Fetching owners + stages…');
  const owners = await getOwners();
  const stages = await getStageMap();
  const deals = transformDeals(rawDeals, owners);

  console.log('Counting contacts/deals…');
  const totals = {
    deals:    (await api('/crm/v3/objects/deals/search', { limit:1 })).total,
    contacts: (await api('/crm/v3/objects/contacts/search', { limit:1 })).total,
    companies:(await api('/crm/v3/objects/companies/search', { limit:1 })).total,
    closedWon:await countDeals({ propertyName:'hs_is_closed_won', operator:'EQ', value:'true' }),
    openDeals:await countDeals({ propertyName:'hs_is_closed', operator:'EQ', value:'false' }),
  };

  const lifecycleStages = [
    ['lead','Lead'],['marketingqualifiedlead','MQL'],['salesqualifiedlead','SQL'],
    ['opportunity','Opportunity'],['customer','Customer'],['subscriber','Subscriber'],
  ];
  const contactsByLifecycle = [];
  for (const [v,label] of lifecycleStages) {
    contactsByLifecycle.push({ stage:label, count: await countContacts({propertyName:'lifecyclestage',operator:'EQ',value:v}) });
  }
  const sources = ['ORGANIC_SEARCH','PAID_SEARCH','EMAIL_MARKETING','SOCIAL_MEDIA','PAID_SOCIAL','REFERRALS','DIRECT_TRAFFIC','OFFLINE','OTHER_CAMPAIGNS','AI_REFERRALS'];
  const contactsBySource = [];
  for (const s of sources) {
    contactsBySource.push({ source:s.replace(/_/g,' '), count: await countContacts({propertyName:'hs_analytics_source',operator:'EQ',value:s}) });
  }

  const newContacts = {
    last30:  await countContacts({propertyName:'createdate',operator:'GT',value:String(daysAgoMs(30))}),
    last90:  await countContacts({propertyName:'createdate',operator:'GT',value:String(daysAgoMs(90))}),
    last365: await countContacts({propertyName:'createdate',operator:'GT',value:String(daysAgoMs(365))}),
  };
  const newDeals = {
    last30:  await countDeals({propertyName:'createdate',operator:'GT',value:String(daysAgoMs(30))}),
    last90:  await countDeals({propertyName:'createdate',operator:'GT',value:String(daysAgoMs(90))}),
    last365: await countDeals({propertyName:'createdate',operator:'GT',value:String(daysAgoMs(365))}),
  };

  // ---- Compute revenue / pipeline metrics from `deals` sample ----
  const isWon  = d => d.properties.hs_is_closed_won === 'true';
  const isOpen = d => d.properties.hs_is_closed === 'false';

  const won  = deals.filter(isWon);
  const open = deals.filter(isOpen);

  const totalClosedWonAmount = sum(won, d => d.properties.amount);
  const totalARR = sum(won, d => d.properties.hs_arr) || totalClosedWonAmount;
  const totalMRR = sum(won, d => d.properties.hs_mrr);
  const openPipelineValue = sum(open, d => d.properties.amount);
  const weightedPipeline  = sum(open, d => Number(d.properties.amount||0) * (Number(d.properties.hs_deal_stage_probability||0.3)));
  const avgDealSize = won.length ? totalClosedWonAmount / won.length : 0;
  const closedLost  = deals.filter(d => d.properties.hs_is_closed === 'true' && !isWon(d)).length;
  const winRate     = (won.length + closedLost) ? won.length/(won.length+closedLost) : 0;

  // New ARR by month (closedate, won only)
  const months = lastNMonths(12);
  const newARRByMonth = months.map(m => ({
    month:m,
    arr: sum(won.filter(d => monthKey(d.properties.closedate)===m), d => Number(d.properties.hs_arr||d.properties.amount||0)),
  }));
  const dealsCreatedByMonth = months.map(m => ({
    month:m,
    count: deals.filter(d => monthKey(d.properties.createdate)===m).length,
  }));

  // By pipeline
  const dealsByPipeline = Object.entries(PIPELINES).map(([id,name]) => {
    const pl = deals.filter(d => d.properties.pipeline === id);
    return {
      pipeline:name,
      open: pl.filter(isOpen).length,
      won:  pl.filter(isWon).length,
      openValue: sum(pl.filter(isOpen), d => d.properties.amount),
      wonValue:  sum(pl.filter(isWon),  d => d.properties.amount),
    };
  });

  // By stage (open)
  const stageBuckets = {};
  open.forEach(d => {
    const sid = d.properties.dealstage;
    const key = (stages[sid]||sid) + ' • ' + (PIPELINES[d.properties.pipeline]||'');
    stageBuckets[key] = stageBuckets[key] || { stage:stages[sid]||sid, pipeline:PIPELINES[d.properties.pipeline]||'', count:0, value:0 };
    stageBuckets[key].count += 1;
    stageBuckets[key].value += Number(d.properties.amount||0);
  });
  const dealsByStage = Object.values(stageBuckets).sort((a,b)=>b.value-a.value);

  // By owner
  const ownerBuckets = {};
  deals.forEach(d => {
    const oid = d.properties.hubspot_owner_id || 'unassigned';
    const name = owners[oid] || 'Unassigned';
    ownerBuckets[name] = ownerBuckets[name] || { owner:name, openCount:0, openValue:0, wonCount:0, wonValue:0 };
    if (isOpen(d)) { ownerBuckets[name].openCount++; ownerBuckets[name].openValue += Number(d.properties.amount||0); }
    if (isWon(d))  { ownerBuckets[name].wonCount++;  ownerBuckets[name].wonValue  += Number(d.properties.amount||0); }
  });
  const dealsByOwner = Object.values(ownerBuckets).sort((a,b)=>b.openValue-a.openValue).slice(0,15);

  // Top open deals
  const topDeals = open
    .map(d => ({
      id:d.id,
      name:d.properties.dealname,
      amount:Number(d.properties.amount||0),
      stage:stages[d.properties.dealstage] || d.properties.dealstage,
      owner:owners[d.properties.hubspot_owner_id]||'',
      closedate:d.properties.closedate,
    }))
    .sort((a,b)=>b.amount-a.amount).slice(0,20);

  // Funnel
  const funnel = {
    lead:        contactsByLifecycle.find(x=>x.stage==='Lead')?.count||0,
    mql:         contactsByLifecycle.find(x=>x.stage==='MQL')?.count||0,
    sql:         contactsByLifecycle.find(x=>x.stage==='SQL')?.count||0,
    opportunity: contactsByLifecycle.find(x=>x.stage==='Opportunity')?.count||0,
    customer:    contactsByLifecycle.find(x=>x.stage==='Customer')?.count||0,
  };

  console.log('Fetching engagement activities…');
  const activities = await pullEngagements();

  console.log('Computing funnel…');
  const funnelData = await computeFunnel();

  const out = {
    generatedAt: new Date().toISOString(),
    currency: 'GBP',
    maps: {
      owners,
      pipelines: PIPELINES,
      stages
    },
    deals,
    contacts: {
      totalCount: totals.contacts,
      byLifecycle: contactsByLifecycle,
      bySource: contactsBySource,
      createdByWeek: []  // would populate from last-N-weeks contact creates
    },
    companies: {
      totalCount: totals.companies
    },
    activities,
    funnel: funnelData,
    lineItems: await pullLineItems(deals),
    totals,
    revenue: { totalClosedWonAmount, totalARR, totalMRR, openPipelineValue, weightedPipeline, avgDealSize, winRate },
    newARRByMonth, dealsCreatedByMonth,
    dealsByPipeline, dealsByStage, topDeals, dealsByOwner,
    contactsByLifecycle, contactsBySource,
    newContacts, newDeals,
    newClosedWon: {
      last30: rawDeals.filter(d => d.properties.hs_is_closed_won === 'true' && Date.parse(d.properties.closedate||0) >= daysAgoMs(30)).length,
      last90: rawDeals.filter(d => d.properties.hs_is_closed_won === 'true' && Date.parse(d.properties.closedate||0) >= daysAgoMs(90)).length,
      last365: rawDeals.filter(d => d.properties.hs_is_closed_won === 'true' && Date.parse(d.properties.closedate||0) >= daysAgoMs(365)).length,
    },
    _note: rawDeals.length < totals.deals
      ? `Revenue/pipeline metrics computed from most recent ${rawDeals.length} of ${totals.deals} deals.`
      : '',
  };

  const outPath = path.join(process.cwd(), 'public', 'data.json');
  await fs.mkdir(path.dirname(outPath), { recursive:true });
  await fs.writeFile(outPath, JSON.stringify(out, null, 2));
  console.log('Wrote', outPath);
}

main().catch(err => { console.error(err); process.exit(1); });
