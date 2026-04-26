import http from 'k6/http';
import { check } from 'k6';
import { authHeaders, BASE_URL, login, uniqueSuffix } from '../lib/helpers.js';

export const options = {
  vus: Number(__ENV.K6_VUS || 30),
  duration: __ENV.K6_DURATION || '45s',
  thresholds: {
    http_req_failed: ['rate<0.02'],
    http_req_duration: ['p(95)<800'],
    checks: ['rate>0.99'],
  },
};

export function setup() {
  return { token: login() };
}

export default function (data) {
  const suffix = uniqueSuffix();
  const payload = {
    account_id: 'k6-manual-account',
    merchant: `k6-merchant-${suffix}`,
    category: 'k6_load',
    currency: 'USD',
    amount_cents: -450 - (__ITER % 100),
    occurred_at: new Date().toISOString(),
  };

  const response = http.post(
    `${BASE_URL}/api/v1/transactions`,
    JSON.stringify(payload),
    {
      headers: authHeaders(data.token, {
        'Content-Type': 'application/json',
        'Idempotency-Key': `k6-${suffix}`,
      }),
      tags: { scenario: 'transactions_write', endpoint: 'create_transaction' },
    }
  );

  check(response, {
    'transaction status is 201': (r) => r.status === 201,
    'transaction id exists': (r) => !!r.json('id'),
  });
}
