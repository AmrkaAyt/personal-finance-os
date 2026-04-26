import http from 'k6/http';
import { check } from 'k6';
import ws from 'k6/ws';
import { authHeaders, BASE_URL, buildStatementCSV, currentMonthWindow, login, safeJSON, uniqueSuffix } from '../lib/helpers.js';

export const options = {
  scenarios: {
    auth_login: {
      executor: 'constant-vus',
      exec: 'authLogin',
      vus: Number(__ENV.K6_AUTH_VUS || 10),
      duration: __ENV.K6_AUTH_DURATION || '30s',
    },
    transaction_write: {
      executor: 'constant-vus',
      exec: 'transactionWrite',
      vus: Number(__ENV.K6_WRITE_VUS || 20),
      duration: __ENV.K6_WRITE_DURATION || '45s',
    },
    read_heavy: {
      executor: 'constant-vus',
      exec: 'readHeavy',
      vus: Number(__ENV.K6_READ_VUS || 40),
      duration: __ENV.K6_READ_DURATION || '60s',
    },
    import_pipeline: {
      executor: 'constant-vus',
      exec: 'importPipeline',
      vus: Number(__ENV.K6_IMPORT_VUS || 5),
      duration: __ENV.K6_IMPORT_DURATION || '30s',
    },
    websocket_connect: {
      executor: 'constant-vus',
      exec: 'websocketConnect',
      vus: Number(__ENV.K6_WS_VUS || 10),
      duration: __ENV.K6_WS_DURATION || '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.03'],
    http_req_duration: ['p(95)<1200'],
    checks: ['rate>0.98'],
  },
};

export function setup() {
  return {
    token: login(),
    window: currentMonthWindow(),
  };
}

export function authLogin() {
  const response = http.post(
    `${BASE_URL}/auth/login`,
    JSON.stringify({ username: __ENV.K6_USERNAME || 'demo', password: __ENV.K6_PASSWORD || 'demo' }),
    {
      headers: { 'Content-Type': 'application/json' },
      tags: { scenario: 'mixed_auth_login', endpoint: 'auth_login' },
    }
  );
  check(response, {
    'mixed login is 200': (r) => r.status === 200,
  });
}

export function transactionWrite(data) {
  const suffix = uniqueSuffix();
  const response = http.post(
    `${BASE_URL}/api/v1/transactions`,
    JSON.stringify({
      account_id: 'k6-mixed-account',
      merchant: `k6-mixed-${suffix}`,
      category: 'k6_load',
      currency: 'USD',
      amount_cents: -1000 - (__ITER % 500),
      occurred_at: new Date().toISOString(),
    }),
    {
      headers: authHeaders(data.token, {
        'Content-Type': 'application/json',
        'Idempotency-Key': `mixed-${suffix}`,
      }),
      tags: { scenario: 'mixed_transaction_write', endpoint: 'create_transaction' },
    }
  );
  check(response, {
    'mixed transaction is 201': (r) => r.status === 201,
  });
}

export function readHeavy(data) {
  const responses = http.batch([
    ['GET', `${BASE_URL}/api/v1/transactions?limit=20`, null, { headers: authHeaders(data.token), tags: { scenario: 'mixed_read_heavy', endpoint: 'transactions_list' } }],
    ['GET', `${BASE_URL}/api/v1/categories`, null, { headers: authHeaders(data.token), tags: { scenario: 'mixed_read_heavy', endpoint: 'categories_list' } }],
    ['GET', `${BASE_URL}/api/v1/analytics/projections/summary?from=${data.window.from}&to=${data.window.to}`, null, { headers: authHeaders(data.token), tags: { scenario: 'mixed_read_heavy', endpoint: 'analytics_summary' } }],
  ]);
  check(responses[0], { 'mixed transactions list is 200': (r) => r.status === 200 });
  check(responses[1], { 'mixed categories list is 200': (r) => r.status === 200 });
  check(responses[2], { 'mixed analytics summary is 200': (r) => r.status === 200 });
}

export function importPipeline(data) {
  const upload = http.post(
    `${BASE_URL}/imports/raw`,
    { file: http.file(buildStatementCSV(), `mixed-${uniqueSuffix()}.csv`, 'text/csv') },
    {
      headers: authHeaders(data.token),
      tags: { scenario: 'mixed_import_pipeline', endpoint: 'imports_raw' },
    }
  );
  check(upload, {
    'mixed import accepted': (r) => r.status === 202,
  });
  const importID = safeJSON(upload).import_id;
  if (!importID) {
    return;
  }
  const status = http.get(`${BASE_URL}/imports/${importID}`, {
    headers: authHeaders(data.token),
    tags: { scenario: 'mixed_import_pipeline', endpoint: 'imports_status' },
  });
  check(status, {
    'mixed import status is 200': (r) => r.status === 200,
  });
}

export function websocketConnect(data) {
  const wsURL = BASE_URL.replace('http://', 'ws://').replace('https://', 'wss://');
  const response = ws.connect(`${wsURL}/ws?access_token=${data.token}&channels=dashboard,alerts,transactions`, {}, function (socket) {
    socket.on('open', function () {
      socket.setTimeout(function () {
        socket.close();
      }, 1000);
    });
  });
  check(response, {
    'mixed websocket upgrade is 101': (r) => r && r.status === 101,
  });
}
