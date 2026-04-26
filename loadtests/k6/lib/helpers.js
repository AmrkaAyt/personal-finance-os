import http from 'k6/http';
import { check, fail } from 'k6';

export const BASE_URL = __ENV.K6_BASE_URL || 'http://localhost:8080';
export const USERNAME = __ENV.K6_USERNAME || 'demo';
export const PASSWORD = __ENV.K6_PASSWORD || 'demo';

export function authHeaders(token, extraHeaders = {}) {
  return {
    Authorization: `Bearer ${token}`,
    ...extraHeaders,
  };
}

export function login() {
  const response = http.post(
    `${BASE_URL}/auth/login`,
    JSON.stringify({ username: USERNAME, password: PASSWORD }),
    {
      headers: { 'Content-Type': 'application/json' },
      tags: { endpoint: 'auth_login' },
    }
  );

  const ok = check(response, {
    'login status is 200': (r) => r.status === 200,
    'login returned access token': (r) => {
      const body = safeJSON(r);
      return !!body.access_token;
    },
  });
  if (!ok) {
    fail(`login failed: status=${response.status} body=${response.body}`);
  }

  return safeJSON(response).access_token;
}

export function currentMonthWindow() {
  const now = new Date();
  const from = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  const to = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  return {
    from: formatDate(from),
    to: formatDate(to),
  };
}

export function uniqueSuffix() {
  return `${__VU}-${__ITER}-${Date.now()}`;
}

export function buildStatementCSV() {
  const suffix = uniqueSuffix();
  return [
    `Coffee-${suffix},-4.50,USD,2026-03-01,food`,
    `Salary-${suffix},1000.00,USD,2026-03-02,income`,
    `Netflix-${suffix},-15.99,USD,2026-03-03,subscriptions`,
  ].join('\n');
}

export function safeJSON(response) {
  try {
    return response.json();
  } catch (_) {
    return {};
  }
}

function formatDate(value) {
  return value.toISOString().slice(0, 10);
}
