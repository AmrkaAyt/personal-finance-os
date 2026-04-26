import { check } from 'k6';
import ws from 'k6/ws';
import { BASE_URL, login } from '../lib/helpers.js';

export const options = {
  vus: Number(__ENV.K6_VUS || 20),
  duration: __ENV.K6_DURATION || '30s',
  thresholds: {
    checks: ['rate>0.99'],
  },
};

export function setup() {
  return { token: login() };
}

export default function (data) {
  const wsURL = BASE_URL.replace('http://', 'ws://').replace('https://', 'wss://');
  const response = ws.connect(`${wsURL}/ws?access_token=${data.token}&channels=dashboard,alerts,transactions`, {}, function (socket) {
    socket.on('open', function () {
      socket.setTimeout(function () {
        socket.close();
      }, 1000);
    });
  });

  check(response, {
    'websocket upgrade is 101': (r) => r && r.status === 101,
  });
}
