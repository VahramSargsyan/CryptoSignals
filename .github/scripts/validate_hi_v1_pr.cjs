#!/usr/bin/env node
const fs = require('fs');

const EVENT_IDS = [
  'NEXT',
  'QUICK_ACCEPT',
  'CLARIFY',
  'TEST',
  'TEST_DEEP_OR_BUG_EVIDENCE',
  'LOGIC_OR_DESIGN_CHANGE'
];

function localDateInYerevan(iso) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Yerevan',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(new Date(iso));
  const map = Object.fromEntries(parts.map(p => [p.type, p.value]));
  return map.year + '-' + map.month + '-' + map.day;
}

function parseJsonComments(body, marker) {
  const rx = new RegExp('<!--\\s*' + marker + '\\s+([\\s\\S]*?)\\s*-->', 'g');
  const out = [];
  let match;
  while ((match = rx.exec(String(body || ''))) !== null) {
    try {
      out.push({ ok: true, value: JSON.parse(match[1]) });
    } catch (error) {
      out.push({ ok: false, error: String(error) });
    }
  }
  return out;
}

function validHiMarker(obj, expectedDate) {
  if (!obj || obj.date !== expectedDate) return false;
  if (typeof obj.SESSION_ID !== 'string' || !obj.SESSION_ID.trim()) return false;
  if (!Number.isInteger(Number(obj.REVISION)) || Number(obj.REVISION) < 1) return false;
  for (const id of EVENT_IDS) {
    const value = Number(obj[id] ?? 0);
    if (!Number.isInteger(value) || value < 0) return false;
  }
  const accepted = Number(obj.ACCEPTED_CAPABILITIES ?? 0);
  if (!Number.isInteger(accepted) || accepted < 0) return false;
  return true;
}

function validGapMarker(obj, expectedDate) {
  return Boolean(
    obj &&
    obj.date === expectedDate &&
    typeof obj.SESSION_ID === 'string' &&
    obj.SESSION_ID.trim() &&
    typeof obj.reason === 'string' &&
    obj.reason.trim()
  );
}

function validatePayload(payload) {
  const pr = payload?.pull_request;
  if (!pr) return { status: 'SKIP_NON_PR' };

  const expectedDate = localDateInYerevan(pr.created_at);
  const body = pr.body || '';
  const hi = parseJsonComments(body, 'HI_V1');
  const gaps = parseJsonComments(body, 'HI_V1_CONTEXT_GAP');

  const validHi = hi.filter(x => x.ok && validHiMarker(x.value, expectedDate));
  const validGap = gaps.filter(x => x.ok && validGapMarker(x.value, expectedDate));

  if (validHi.length > 0) {
    return {
      status: 'PASS',
      expectedDate,
      sessionIds: [...new Set(validHi.map(x => x.value.SESSION_ID))],
      revisions: validHi.map(x => Number(x.value.REVISION))
    };
  }

  if (validGap.length > 0) {
    return {
      status: 'PASS_WITH_CONTEXT_GAP',
      expectedDate,
      sessionIds: [...new Set(validGap.map(x => x.value.SESSION_ID))]
    };
  }

  return {
    status: 'FAIL',
    expectedDate,
    hiMarkersFound: hi.length,
    contextGapMarkersFound: gaps.length,
    malformedHiMarkers: hi.filter(x => !x.ok).length,
    malformedGapMarkers: gaps.filter(x => !x.ok).length
  };
}

function selfTest() {
  const createdAt = '2026-10-02T09:00:00Z';
  const date = '2026-10-02';
  const valid = {
    pull_request: {
      created_at: createdAt,
      body: '<!-- HI_V1 {"date":"' + date + '","SESSION_ID":"S1","REVISION":2,"NEXT":1,"QUICK_ACCEPT":0,"CLARIFY":1,"TEST":0,"TEST_DEEP_OR_BUG_EVIDENCE":0,"LOGIC_OR_DESIGN_CHANGE":0,"ACCEPTED_CAPABILITIES":0} -->'
    }
  };
  const gap = {
    pull_request: {
      created_at: createdAt,
      body: '<!-- HI_V1_CONTEXT_GAP {"date":"' + date + '","SESSION_ID":"S2","reason":"CHAT_CONTEXT_UNAVAILABLE"} -->'
    }
  };
  const missing = { pull_request: { created_at: createdAt, body: '' } };
  const wrongDate = {
    pull_request: {
      created_at: createdAt,
      body: '<!-- HI_V1 {"date":"2026-10-01","SESSION_ID":"S3","REVISION":1,"NEXT":1,"QUICK_ACCEPT":0,"CLARIFY":0,"TEST":0,"TEST_DEEP_OR_BUG_EVIDENCE":0,"LOGIC_OR_DESIGN_CHANGE":0,"ACCEPTED_CAPABILITIES":0} -->'
    }
  };
  if (validatePayload(valid).status !== 'PASS') throw new Error('valid HI_V1 fixture must PASS');
  if (validatePayload(gap).status !== 'PASS_WITH_CONTEXT_GAP') throw new Error('context gap fixture must PASS_WITH_CONTEXT_GAP');
  if (validatePayload(missing).status !== 'FAIL') throw new Error('missing fixture must FAIL');
  if (validatePayload(wrongDate).status !== 'FAIL') throw new Error('wrong-date fixture must FAIL');
  console.log('HI_V1_GATE_SELF_TEST PASS');
}

if (process.argv.includes('--self-test')) {
  selfTest();
  process.exit(0);
}

const eventName = process.env.GITHUB_EVENT_NAME || '';
if (eventName && eventName !== 'pull_request') {
  console.log('HI_V1 gate: skip non-pull_request event:', eventName);
  process.exit(0);
}

const eventPath = process.env.GITHUB_EVENT_PATH;
if (!eventPath || !fs.existsSync(eventPath)) {
  console.error('HI_V1 gate: GITHUB_EVENT_PATH is missing');
  process.exit(1);
}

const payload = JSON.parse(fs.readFileSync(eventPath, 'utf8'));
const result = validatePayload(payload);

if (result.status === 'PASS') {
  console.log('HI_V1 gate PASS', JSON.stringify(result));
  process.exit(0);
}
if (result.status === 'PASS_WITH_CONTEXT_GAP') {
  console.log('HI_V1 gate PASS_WITH_CONTEXT_GAP', JSON.stringify(result));
  process.exit(0);
}
if (result.status === 'SKIP_NON_PR') {
  console.log('HI_V1 gate skip: event payload has no pull_request');
  process.exit(0);
}

console.error('HI_V1_TELEMETRY_REQUIRED');
console.error(JSON.stringify(result));
console.error('Add a valid HI_V1 marker for the PR creation date in Asia/Yerevan, or an explicit HI_V1_CONTEXT_GAP marker.');
process.exit(1);
