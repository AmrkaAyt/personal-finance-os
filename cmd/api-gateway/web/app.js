const state = {
  accessToken: localStorage.getItem("pfos.accessToken") || "",
  refreshToken: localStorage.getItem("pfos.refreshToken") || "",
  profile: null,
  ws: null,
  live: [],
  actions: new Map(),
  insights: new Map(),
  preferences: null,
};

const els = {
  loginView: document.querySelector("#loginView"),
  appView: document.querySelector("#appView"),
  loginForm: document.querySelector("#loginForm"),
  loginError: document.querySelector("#loginError"),
  username: document.querySelector("#username"),
  password: document.querySelector("#password"),
  profileTitle: document.querySelector("#profileTitle"),
  dateFrom: document.querySelector("#dateFrom"),
  dateTo: document.querySelector("#dateTo"),
  refreshButton: document.querySelector("#refreshButton"),
  logoutButton: document.querySelector("#logoutButton"),
  spentMetric: document.querySelector("#spentMetric"),
  incomeMetric: document.querySelector("#incomeMetric"),
  netMetric: document.querySelector("#netMetric"),
  countMetric: document.querySelector("#countMetric"),
  periodLabel: document.querySelector("#periodLabel"),
  dailySpendChart: document.querySelector("#dailySpendChart"),
  transactionsBody: document.querySelector("#transactionsBody"),
  transactionCount: document.querySelector("#transactionCount"),
  importForm: document.querySelector("#importForm"),
  statementFile: document.querySelector("#statementFile"),
  importStatus: document.querySelector("#importStatus"),
  importResult: document.querySelector("#importResult"),
  manualForm: document.querySelector("#manualForm"),
  manualMerchant: document.querySelector("#manualMerchant"),
  manualCategory: document.querySelector("#manualCategory"),
  manualAmount: document.querySelector("#manualAmount"),
  manualDate: document.querySelector("#manualDate"),
  manualStatus: document.querySelector("#manualStatus"),
  alertsList: document.querySelector("#alertsList"),
  alertCount: document.querySelector("#alertCount"),
  recurringList: document.querySelector("#recurringList"),
  liveFeed: document.querySelector("#liveFeed"),
  wsDot: document.querySelector("#wsDot"),
  wsState: document.querySelector("#wsState"),
  presenceLabel: document.querySelector("#presenceLabel"),
  notificationStatus: document.querySelector("#notificationStatus"),
  prefsForm: document.querySelector("#prefsForm"),
  prefTelegram: document.querySelector("#prefTelegram"),
  prefBatch: document.querySelector("#prefBatch"),
  prefQuiet: document.querySelector("#prefQuiet"),
  prefQuietStart: document.querySelector("#prefQuietStart"),
  prefQuietEnd: document.querySelector("#prefQuietEnd"),
  prefTimezone: document.querySelector("#prefTimezone"),
  prefDisabled: document.querySelector("#prefDisabled"),
  prefsStatus: document.querySelector("#prefsStatus"),
  telegramCode: document.querySelector("#telegramCode"),
  confirmTelegramButton: document.querySelector("#confirmTelegramButton"),
  demoTelegramButton: document.querySelector("#demoTelegramButton"),
};

function setDefaultDates() {
  const now = new Date();
  const first = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  els.dateFrom.value = first.toISOString().slice(0, 10);
  els.dateTo.value = now.toISOString().slice(0, 10);
  els.manualDate.value = new Date(now.getTime() - now.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
}

function setAuthenticated(pair) {
  state.accessToken = pair.access_token || "";
  state.refreshToken = pair.refresh_token || "";
  localStorage.setItem("pfos.accessToken", state.accessToken);
  localStorage.setItem("pfos.refreshToken", state.refreshToken);
}

function clearSession() {
  state.accessToken = "";
  state.refreshToken = "";
  localStorage.removeItem("pfos.accessToken");
  localStorage.removeItem("pfos.refreshToken");
  if (state.ws) {
    state.ws.close();
    state.ws = null;
  }
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.accessToken) {
    headers.set("Authorization", `Bearer ${state.accessToken}`);
  }
  const response = await fetch(path, { ...options, headers });
  if (response.status === 401 && state.refreshToken) {
    const refreshed = await refreshToken();
    if (refreshed) {
      headers.set("Authorization", `Bearer ${state.accessToken}`);
      return fetch(path, { ...options, headers });
    }
  }
  return response;
}

async function jsonApi(path, options = {}) {
  const response = await api(path, options);
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(payload.error || `${response.status} ${response.statusText}`);
  }
  return payload;
}

async function refreshToken() {
  try {
    const response = await fetch("/auth/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: state.refreshToken }),
    });
    if (!response.ok) {
      return false;
    }
    setAuthenticated(await response.json());
    return true;
  } catch {
    return false;
  }
}

function showApp() {
  els.loginView.classList.add("hidden");
  els.appView.classList.remove("hidden");
}

function showLogin() {
  els.appView.classList.add("hidden");
  els.loginView.classList.remove("hidden");
}

async function login(event) {
  event.preventDefault();
  els.loginError.textContent = "";
  try {
    const response = await fetch("/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: els.username.value.trim(),
        password: els.password.value,
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Login failed");
    }
    setAuthenticated(payload);
    await bootApp();
  } catch (error) {
    els.loginError.textContent = error.message;
  }
}

async function bootApp() {
  showApp();
  await loadAll();
  connectWebSocket();
}

async function loadAll() {
  if (!state.accessToken) {
    showLogin();
    return;
  }
  try {
    const [profile, summary, daily, transactions, recurring, alerts, actions, status, prefs, presence] = await Promise.all([
      jsonApi("/api/v1/profile"),
      jsonApi(`/api/v1/analytics/projections/summary?${periodParams()}`),
      jsonApi(`/api/v1/analytics/projections/daily-spend?${periodParams()}`),
      jsonApi("/api/v1/transactions?limit=50"),
      jsonApi("/api/v1/recurring"),
      jsonApi(`/api/v1/analytics/projections/alerts?${periodParams()}`),
      jsonApi("/api/v1/insights/actions?limit=200"),
      jsonApi("/api/v1/notifications/status"),
      jsonApi("/api/v1/notifications/preferences"),
      jsonApi("/api/v1/presence"),
    ]);
    state.profile = profile;
    state.actions = buildActionMap(actions.actions || []);
    state.insights = new Map();
    renderProfile(profile);
    renderSummary(summary);
    renderDailySpend(daily);
    renderTransactions(transactions.transactions || []);
    renderRecurring(recurring.patterns || []);
    renderAlerts(alerts);
    renderNotificationStatus(status);
    renderPreferences(prefs);
    renderPresence(presence);
  } catch (error) {
    if (String(error.message).includes("token") || String(error.message).includes("unauthorized")) {
      clearSession();
      showLogin();
      return;
    }
    pushLive("Load failed", error.message);
  }
}

function periodParams() {
  return new URLSearchParams({ from: els.dateFrom.value, to: els.dateTo.value }).toString();
}

function renderProfile(profile) {
  els.profileTitle.textContent = profile.user_id || "Dashboard";
  els.periodLabel.textContent = `${els.dateFrom.value} to ${els.dateTo.value}`;
}

function renderSummary(payload) {
  const row = firstDataRow(payload);
  const debit = numberValue(row.debit_cents);
  const credit = numberValue(row.credit_cents);
  const count = numberValue(row.transaction_count);
  els.spentMetric.textContent = money(debit);
  els.incomeMetric.textContent = money(credit);
  els.netMetric.textContent = money(credit - debit);
  els.countMetric.textContent = String(count);
}

function renderDailySpend(payload) {
  const rows = dataRows(payload);
  const byDate = new Map();
  for (const row of rows) {
    const key = row.event_date || "unknown";
    byDate.set(key, (byDate.get(key) || 0) + numberValue(row.debit_cents));
  }
  const items = Array.from(byDate.entries()).sort(([a], [b]) => a.localeCompare(b));
  const max = Math.max(1, ...items.map(([, value]) => value));
  els.dailySpendChart.innerHTML = items.length
    ? items.map(([date, cents]) => `
      <div class="bar-row">
        <span>${escapeHTML(date)}</span>
        <div class="bar-track"><div class="bar-fill" style="width:${Math.max(2, (cents / max) * 100)}%"></div></div>
        <span class="amount-cell">${money(cents)}</span>
      </div>
    `).join("")
    : emptyState("No spend for this period");
}

function renderTransactions(transactions) {
  els.transactionCount.textContent = `${transactions.length} rows`;
  els.transactionsBody.innerHTML = transactions.length
    ? transactions.map((item) => {
      const amountClass = numberValue(item.amount_cents) < 0 ? "amount-negative" : "amount-positive";
      return `
        <tr>
          <td>${escapeHTML(shortDate(item.occurred_at))}</td>
          <td>${escapeHTML(item.merchant || "")}</td>
          <td>${escapeHTML(item.category || "")}</td>
          <td class="amount-cell ${amountClass}">${money(numberValue(item.amount_cents))}</td>
        </tr>
      `;
    }).join("")
    : `<tr><td colspan="4">${emptyState("No transactions yet")}</td></tr>`;
}

function renderAlerts(payload) {
  const rows = dataRows(payload);
  els.alertCount.textContent = `${rows.length} alert rows`;
  els.alertsList.innerHTML = rows.length
    ? rows.map((row) => {
      const insight = alertInsight(row);
      return `
      <div class="event-item actionable">
        <strong>${escapeHTML(row.type || "alert")} · ${escapeHTML(row.severity || "unknown")}</strong>
        <span>${escapeHTML(row.event_date || "")} · ${numberValue(row.alert_count)} events</span>
        ${actionControls(insight, [
          ["acknowledge", "Acknowledge"],
          ["snooze", "Snooze 24h"],
          ["resolve", "Resolve"],
          ["suppress_similar", "Suppress similar"],
        ])}
      </div>
    `;
    }).join("")
    : emptyState("No alerts");
}

function renderRecurring(patterns) {
  els.recurringList.innerHTML = patterns.length
    ? patterns.map((pattern) => {
      const insight = recurringInsight(pattern);
      return `
      <div class="event-item actionable">
        <strong>${escapeHTML(pattern.merchant || "merchant")}</strong>
        <span>${escapeHTML(pattern.category || "category")} · every ${numberValue(pattern.interval_days)} days · ${money(numberValue(pattern.amount_cents))}</span>
        ${actionControls(insight, [
          ["confirm_recurring", "Confirm"],
          ["reject_recurring", "Reject"],
        ])}
      </div>
    `;
    }).join("")
    : emptyState("No recurring patterns");
}

function renderNotificationStatus(status) {
  const telegram = status.telegram_enabled ? "Telegram ready" : "Telegram disabled";
  els.notificationStatus.textContent = `${telegram} · ${status.queue || "queue"}`;
}

function renderPreferences(prefs) {
  state.preferences = prefs;
  els.prefTelegram.checked = Boolean(prefs.telegram_enabled);
  els.prefBatch.checked = Boolean(prefs.batch_non_critical);
  els.prefQuiet.checked = Boolean(prefs.quiet_hours_enabled);
  els.prefQuietStart.value = minutesToTime(numberValue(prefs.quiet_start_minute));
  els.prefQuietEnd.value = minutesToTime(numberValue(prefs.quiet_end_minute));
  els.prefTimezone.value = prefs.quiet_timezone || "UTC";
  els.prefDisabled.value = (prefs.disabled_alert_types || []).join(", ");
}

function buildActionMap(actions) {
  const result = new Map();
  for (const action of actions) {
    result.set(action.insight_id, action);
  }
  return result;
}

function alertInsight(row) {
  const eventDate = row.event_date || "unknown";
  const type = row.type || "alert";
  const severity = row.severity || "unknown";
  return {
    id: `alert:${eventDate}:${type}:${severity}`,
    type: "alert",
    metadata: {
      event_date: String(eventDate),
      alert_type: String(type),
      severity: String(severity),
    },
  };
}

function recurringInsight(pattern) {
  const merchant = pattern.merchant || "merchant";
  const category = pattern.category || "category";
  const amount = String(numberValue(pattern.amount_cents));
  const interval = String(numberValue(pattern.interval_days));
  return {
    id: `recurring:${merchant}:${category}:${amount}:${interval}`,
    type: "recurring",
    metadata: {
      merchant: String(merchant),
      category: String(category),
      amount_cents: amount,
      interval_days: interval,
    },
  };
}

function actionControls(insight, actions) {
  state.insights.set(insight.id, insight);
  const latest = state.actions.get(insight.id);
  return `
    <div class="action-state">${latest ? actionStatus(latest) : "Needs review"}</div>
    <div class="action-row">
      ${actions.map(([action, label]) => `
        <button type="button" data-insight-id="${escapeHTML(insight.id)}" data-action="${escapeHTML(action)}">${escapeHTML(label)}</button>
      `).join("")}
    </div>
  `;
}

function actionStatus(action) {
  const label = String(action.action || "").replaceAll("_", " ");
  if (action.action === "snooze" && action.snoozed_until) {
    return `Snoozed until ${shortDate(action.snoozed_until)}`;
  }
  return `Last action: ${label}`;
}

function renderPresence(presence) {
  const count = numberValue(presence.local_connections);
  els.presenceLabel.textContent = `${count} local connection${count === 1 ? "" : "s"}`;
}

async function uploadImport(event) {
  event.preventDefault();
  const file = els.statementFile.files[0];
  if (!file) {
    return;
  }
  els.importStatus.textContent = "Uploading";
  els.importResult.innerHTML = "";
  const body = new FormData();
  body.append("file", file);
  try {
    const result = await jsonApi("/imports/raw", { method: "POST", body });
    els.importStatus.textContent = result.already_exists ? "Already exists" : "Queued";
    els.importResult.innerHTML = lineItems({
      import_id: result.import_id,
      status: result.status || "queued",
    });
    pollImport(result.import_id);
  } catch (error) {
    els.importStatus.textContent = "Failed";
    els.importResult.innerHTML = emptyState(error.message);
  }
}

async function pollImport(importID) {
  for (let attempt = 0; attempt < 30; attempt += 1) {
    await sleep(1000);
    try {
      const raw = await jsonApi(`/imports/${encodeURIComponent(importID)}`);
      els.importStatus.textContent = raw.status || "processing";
      if (raw.status === "parsed") {
        const parsed = await jsonApi(`/parser/results/${encodeURIComponent(importID)}`);
        els.importResult.innerHTML = lineItems({
          import_id: importID,
          status: parsed.status,
          transactions: parsed.summary?.transaction_count ?? 0,
          debit: money(numberValue(parsed.summary?.total_debit_cents)),
          credit: money(numberValue(parsed.summary?.total_credit_cents)),
        });
        await loadAll();
        return;
      }
    } catch (error) {
      els.importResult.innerHTML = emptyState(error.message);
    }
  }
}

async function addManualTransaction(event) {
  event.preventDefault();
  els.manualStatus.textContent = "Saving";
  const amountCents = Math.round(Number(els.manualAmount.value || 0) * 100);
  const occurredAt = new Date(els.manualDate.value).toISOString();
  try {
    await jsonApi("/api/v1/transactions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": randomID(),
      },
      body: JSON.stringify({
        account_id: "manual-web",
        merchant: els.manualMerchant.value.trim(),
        category: els.manualCategory.value.trim() || "uncategorized",
        currency: "USD",
        amount_cents: amountCents,
        occurred_at: occurredAt,
      }),
    });
    els.manualStatus.textContent = "Saved";
    els.manualForm.reset();
    setDefaultDates();
    await loadAll();
  } catch (error) {
    els.manualStatus.textContent = error.message;
  }
}

async function savePreferences(event) {
  event.preventDefault();
  els.prefsStatus.textContent = "Saving";
  try {
    const prefs = await jsonApi("/api/v1/notifications/preferences", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        telegram_enabled: els.prefTelegram.checked,
        batch_non_critical: els.prefBatch.checked,
        quiet_hours_enabled: els.prefQuiet.checked,
        quiet_start_minute: timeToMinutes(els.prefQuietStart.value),
        quiet_end_minute: timeToMinutes(els.prefQuietEnd.value),
        quiet_timezone: els.prefTimezone.value.trim() || "UTC",
        disabled_alert_types: els.prefDisabled.value.split(",").map((item) => item.trim()).filter(Boolean),
      }),
    });
    renderPreferences(prefs);
    els.prefsStatus.textContent = "Saved";
  } catch (error) {
    els.prefsStatus.textContent = error.message;
  }
}

async function confirmTelegram() {
  const code = els.telegramCode.value.trim();
  if (!code) {
    els.prefsStatus.textContent = "Code is required";
    return;
  }
  try {
    await jsonApi("/api/v1/notifications/telegram/link/confirm", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code }),
    });
    els.prefsStatus.textContent = "Telegram linked";
    els.telegramCode.value = "";
  } catch (error) {
    els.prefsStatus.textContent = error.message;
  }
}

async function sendTelegramDemo() {
  try {
    await jsonApi("/api/v1/notifications/telegram/demo", { method: "POST" });
    els.prefsStatus.textContent = "Demo queued";
  } catch (error) {
    els.prefsStatus.textContent = error.message;
  }
}

async function handleInsightAction(event) {
  const button = event.target.closest("button[data-insight-id][data-action]");
  if (!button) {
    return;
  }
  const insight = state.insights.get(button.dataset.insightId);
  if (!insight) {
    return;
  }

  button.disabled = true;
  const action = button.dataset.action;
  const body = {
    insight_id: insight.id,
    insight_type: insight.type,
    action,
    reason: "web_cockpit",
    metadata: insight.metadata,
  };
  if (action === "snooze") {
    body.snoozed_until = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  }

  try {
    const stored = await jsonApi("/api/v1/insights/actions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    state.actions.set(stored.insight_id, stored);
    if (action === "suppress_similar" && insight.metadata.alert_type) {
      await suppressSimilarAlertType(insight.metadata.alert_type);
    }
    pushLive("Insight action", `${action.replaceAll("_", " ")} saved`);
    await loadAll();
  } catch (error) {
    pushLive("Insight action failed", error.message);
  } finally {
    button.disabled = false;
  }
}

async function suppressSimilarAlertType(alertType) {
  const current = new Set((state.preferences?.disabled_alert_types || []).map(String));
  current.add(String(alertType));
  const prefs = await jsonApi("/api/v1/notifications/preferences", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      telegram_enabled: els.prefTelegram.checked,
      batch_non_critical: els.prefBatch.checked,
      quiet_hours_enabled: els.prefQuiet.checked,
      quiet_start_minute: timeToMinutes(els.prefQuietStart.value),
      quiet_end_minute: timeToMinutes(els.prefQuietEnd.value),
      quiet_timezone: els.prefTimezone.value.trim() || "UTC",
      disabled_alert_types: Array.from(current).sort(),
    }),
  });
  renderPreferences(prefs);
}

function connectWebSocket() {
  if (!state.accessToken || state.ws) {
    return;
  }
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const url = `${scheme}://${location.host}/ws?access_token=${encodeURIComponent(state.accessToken)}&channels=dashboard,transactions,alerts`;
  state.ws = new WebSocket(url);
  state.ws.addEventListener("open", () => setWSState("Online", "online"));
  state.ws.addEventListener("close", () => {
    setWSState("Offline", "");
    state.ws = null;
  });
  state.ws.addEventListener("error", () => setWSState("Error", "error"));
  state.ws.addEventListener("message", (event) => {
    const payload = JSON.parse(event.data);
    pushLive(payload.type || "event", liveSummary(payload));
    if (payload.type === "transaction.upserted" || payload.type === "alert.created") {
      loadAll();
    }
  });
}

function setWSState(label, className) {
  els.wsState.textContent = label;
  els.wsDot.className = `dot ${className}`.trim();
}

function pushLive(title, detail) {
  state.live.unshift({ title, detail, at: new Date() });
  state.live = state.live.slice(0, 20);
  els.liveFeed.innerHTML = state.live.map((item) => `
    <div class="event-item">
      <strong>${escapeHTML(item.title)}</strong>
      <span>${escapeHTML(item.at.toLocaleTimeString())} · ${escapeHTML(item.detail || "")}</span>
    </div>
  `).join("");
}

function liveSummary(payload) {
  if (payload.payload?.merchant) {
    return `${payload.payload.merchant} ${money(numberValue(payload.payload.amount_cents))}`;
  }
  if (payload.payload?.message) {
    return payload.payload.message;
  }
  return payload.channel || "";
}

function dataRows(payload) {
  return payload?.result?.data || [];
}

function firstDataRow(payload) {
  return dataRows(payload)[0] || {};
}

function numberValue(value) {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function money(cents) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(numberValue(cents) / 100);
}

function shortDate(value) {
  if (!value) {
    return "";
  }
  return String(value).slice(0, 10);
}

function minutesToTime(minutes) {
  const normalized = Math.max(0, Math.min(1439, minutes || 0));
  const hours = String(Math.floor(normalized / 60)).padStart(2, "0");
  const mins = String(normalized % 60).padStart(2, "0");
  return `${hours}:${mins}`;
}

function timeToMinutes(value) {
  const [hours, minutes] = String(value || "00:00").split(":").map(Number);
  return (hours || 0) * 60 + (minutes || 0);
}

function lineItems(items) {
  return Object.entries(items).map(([key, value]) => `<span><strong>${escapeHTML(key)}</strong>: ${escapeHTML(String(value))}</span>`).join("");
}

function emptyState(message) {
  return `<div class="event-item"><span>${escapeHTML(message)}</span></div>`;
}

function escapeHTML(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  })[char]);
}

function randomID() {
  if (crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return `web-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

els.loginForm.addEventListener("submit", login);
els.refreshButton.addEventListener("click", loadAll);
els.logoutButton.addEventListener("click", () => {
  clearSession();
  showLogin();
});
els.importForm.addEventListener("submit", uploadImport);
els.manualForm.addEventListener("submit", addManualTransaction);
els.prefsForm.addEventListener("submit", savePreferences);
els.confirmTelegramButton.addEventListener("click", confirmTelegram);
els.demoTelegramButton.addEventListener("click", sendTelegramDemo);
els.alertsList.addEventListener("click", handleInsightAction);
els.recurringList.addEventListener("click", handleInsightAction);
els.dateFrom.addEventListener("change", loadAll);
els.dateTo.addEventListener("change", loadAll);

setDefaultDates();
if (state.accessToken) {
  bootApp();
} else {
  showLogin();
}
