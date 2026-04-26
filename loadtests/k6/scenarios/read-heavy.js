import http from 'k6/http';
import { check } from 'k6';
import { authHeaders, BASE_URL, currentMonthWindow, login } from '../lib/helpers.js';

export const options = {
  vus: Number(__ENV.K6_VUS || 50),
  duration: __ENV.K6_DURATION || '60s',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<700'],
    checks: ['rate>0.99'],
  },
};

export function setup() {
  return { token: login(), window: currentMonthWindow() };
}

export default function (data) {
  const params = { headers: authHeaders(data.token), tags: { scenario: 'read_heavy' } };
  const requests = [
    ['GET', `${BASE_URL}/api/v1/transactions?limit=20`, null, { ...params, tags: { scenario: 'read_heavy', endpoint: 'transactions_list' } }],
    ['GET', `${BASE_URL}/api/v1/categories`, null, { ...params, tags: { scenario: 'read_heavy', endpoint: 'categories_list' } }],
    ['GET', `${BASE_URL}/api/v1/analytics/projections/summary?from=${data.window.from}&to=${data.window.to}`, null, { ...params, tags: { scenario: 'read_heavy', endpoint: 'analytics_summary' } }],
    ['GET', `${BASE_URL}/api/v1/analytics/projections/daily-spend?from=${data.window.from}&to=${data.window.to}`, null, { ...params, tags: { scenario: 'read_heavy', endpoint: 'analytics_daily_spend' } }],
    ['GET', `${BASE_URL}/api/v1/analytics/projections/alerts?from=${data.window.from}&to=${data.window.to}`, null, { ...params, tags: { scenario: 'read_heavy', endpoint: 'analytics_alerts' } }],
  ];

  const responses = http.batch(requests);

  check(responses[0], { 'transactions list is 200': (r) => r.status === 200 });
  check(responses[1], { 'categories list is 200': (r) => r.status === 200 });
  check(responses[2], { 'analytics summary is 200': (r) => r.status === 200 });
  check(responses[3], { 'analytics daily spend is 200': (r) => r.status === 200 });
  check(responses[4], { 'analytics alerts is 200': (r) => r.status === 200 });
}
