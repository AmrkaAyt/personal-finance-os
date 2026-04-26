import http from 'k6/http';
import { check } from 'k6';
import { authHeaders, BASE_URL, buildStatementCSV, login, safeJSON, uniqueSuffix } from '../lib/helpers.js';

export const options = {
  vus: Number(__ENV.K6_VUS || 10),
  duration: __ENV.K6_DURATION || '30s',
  thresholds: {
    http_req_failed: ['rate<0.03'],
    http_req_duration: ['p(95)<1200'],
    checks: ['rate>0.99'],
  },
};

export function setup() {
  return { token: login() };
}

export default function (data) {
  const csv = buildStatementCSV();
  const filename = `k6-statement-${uniqueSuffix()}.csv`;
  const form = {
    file: http.file(csv, filename, 'text/csv'),
  };

  const upload = http.post(`${BASE_URL}/imports/raw`, form, {
    headers: authHeaders(data.token),
    tags: { scenario: 'import_pipeline', endpoint: 'imports_raw' },
  });

  const accepted = check(upload, {
    'import accepted': (r) => r.status === 202,
    'import id exists': (r) => !!safeJSON(r).import_id,
  });
  if (!accepted) {
    return;
  }

  const importID = safeJSON(upload).import_id;
  const status = http.get(`${BASE_URL}/imports/${importID}`, {
    headers: authHeaders(data.token),
    tags: { scenario: 'import_pipeline', endpoint: 'imports_status' },
  });

  check(status, {
    'import status query is 200': (r) => r.status === 200,
  });
}
