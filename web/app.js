(function () {
  const state = {
    token: localStorage.getItem("qsm_access_token") || "",
    user: null,
    env: "dev",
    activeView: "dashboardView",
    caches: {
      events: [],
      alerts: [],
      sources: [],
      manual: [],
      users: [],
      auditLogs: [],
    },
    filteredEvents: [],
    selectedAlerts: new Set(),
    liveEventIds: new Set(),
    liveStream: {
      socket: null,
      retryTimer: null,
      path: "/ws/events",
    },
    pagination: {
      events: { page: 1, size: 20 },
      alerts: { page: 1, size: 20 },
      sources: { page: 1, size: 20 },
      manual: { page: 1, size: 20 },
      users: { page: 1, size: 10 },
      audit: { page: 1, size: 20 },
    },
  };

  const $ = (id) => document.getElementById(id);
  const PAGERS = {
    eventsPagination: "events",
    alertsPagination: "alerts",
    sourcesPagination: "sources",
    manualPagination: "manual",
    usersPagination: "users",
    auditPagination: "audit",
  };

  function toast(msg, level = "info") {
    const node = $("toast");
    node.className = level === "error" ? "error" : level === "warn" ? "warn" : "";
    node.textContent = msg || "";
  }

  function htmlEscape(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function toCsvList(value) {
    return (value || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function authHeaders() {
    return state.token ? { Authorization: `Bearer ${state.token}` } : {};
  }

  function setRealtimeBadge(isLive) {
    const badge = $("realtimeBadge");
    badge.textContent = isLive ? "实时: 已连接" : "实时: 断开";
    badge.classList.toggle("live", isLive);
    badge.classList.toggle("offline", !isLive);
  }

  function setLoading(containerId, loading) {
    const node = $(containerId);
    if (node) {
      node.classList.toggle("loading", loading);
    }
  }

  async function api(path, options = {}) {
    const opts = { ...options };
    const headers = { ...(opts.headers || {}), ...authHeaders() };
    if (opts.body && typeof opts.body === "object" && !(opts.body instanceof FormData)) {
      headers["Content-Type"] = "application/json";
      opts.body = JSON.stringify(opts.body);
    }
    opts.headers = headers;
    const response = await fetch(path, opts);
    const contentType = response.headers.get("content-type") || "";
    const payload = contentType.includes("application/json") ? await response.json() : await response.text();
    if (!response.ok) {
      const detail = typeof payload === "string" ? payload : payload.detail || JSON.stringify(payload);
      const error = new Error(`${response.status} ${detail}`);
      error.status = response.status;
      throw error;
    }
    return payload;
  }

  function openDrawer(title, payload) {
    $("drawerTitle").textContent = title;
    $("drawerContent").textContent = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
    $("detailDrawer").classList.remove("hidden");
  }

  function closeDrawer() {
    $("detailDrawer").classList.add("hidden");
  }

  function setView(viewId) {
    state.activeView = viewId;
    document.querySelectorAll(".view").forEach((node) => node.classList.remove("active"));
    document.querySelectorAll(".nav-btn").forEach((btn) => btn.classList.remove("active"));
    const view = $(viewId);
    if (view) view.classList.add("active");
    const activeBtn = document.querySelector(`.nav-btn[data-view="${viewId}"]`);
    if (activeBtn) activeBtn.classList.add("active");
    $("breadcrumb").textContent = activeBtn ? activeBtn.textContent : "总览";
  }

  function applyRoleVisibility() {
    const isAdmin = state.user && state.user.role === "admin";
    document.querySelectorAll(".admin-only").forEach((node) => {
      if (isAdmin) {
        node.classList.remove("hidden");
      } else {
        node.classList.add("hidden");
      }
    });
    if (!isAdmin && state.activeView === "adminView") {
      setView("dashboardView");
    }
  }

  function renderSimpleList(containerId, rows, mapper) {
    const container = $(containerId);
    container.innerHTML = rows.map(mapper).join("") || '<div class="list-item">暂无数据</div>';
  }

  function renderTable(containerId, columns, rows, rowDataAttrs = {}) {
    const header = columns.map((col) => `<th>${htmlEscape(col.title)}</th>`).join("");
    const body = rows
      .map((row) => {
        const attrs = Object.entries(rowDataAttrs)
          .map(([attr, key]) => `data-${attr}="${htmlEscape(row[key])}"`)
          .join(" ");
        const tds = columns.map((col) => `<td>${col.render ? col.render(row) : htmlEscape(row[col.key])}</td>`).join("");
        return `<tr ${attrs}>${tds}</tr>`;
      })
      .join("");
    $(containerId).innerHTML = `<table><thead><tr>${header}</tr></thead><tbody>${body || "<tr><td colspan='99'>暂无数据</td></tr>"}</tbody></table>`;
  }

  function pageSlice(key, rows) {
    const pageState = state.pagination[key];
    const total = rows.length;
    const pages = Math.max(1, Math.ceil(total / pageState.size));
    if (pageState.page > pages) pageState.page = pages;
    if (pageState.page < 1) pageState.page = 1;
    const start = (pageState.page - 1) * pageState.size;
    return { rows: rows.slice(start, start + pageState.size), total, pages, page: pageState.page };
  }

  function renderPager(containerId, key, total) {
    const pageState = state.pagination[key];
    const pages = Math.max(1, Math.ceil(total / pageState.size));
    const disabledPrev = pageState.page <= 1 ? "disabled" : "";
    const disabledNext = pageState.page >= pages ? "disabled" : "";
    $(containerId).innerHTML = `
      <button data-page-key="${key}" data-page-action="prev" ${disabledPrev}>上一页</button>
      <span class="page-info">第 ${pageState.page} / ${pages} 页 · 共 ${total} 条</span>
      <button data-page-key="${key}" data-page-action="next" ${disabledNext}>下一页</button>
    `;
  }

  function drawBarChart(canvasId, labels, values, color) {
    const canvas = $(canvasId);
    if (!(canvas instanceof HTMLCanvasElement)) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const width = canvas.clientWidth || 480;
    const height = canvas.height || 190;
    canvas.width = width;
    canvas.height = height;
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#0f1a2d";
    ctx.fillRect(0, 0, width, height);
    const padding = 26;
    const barAreaH = height - padding * 2;
    const barW = Math.max(24, (width - padding * 2) / Math.max(values.length, 1) - 14);
    const maxValue = Math.max(1, ...values);
    labels.forEach((label, index) => {
      const value = values[index] || 0;
      const h = Math.round((value / maxValue) * (barAreaH - 16));
      const x = padding + index * (barW + 14);
      const y = height - padding - h;
      ctx.fillStyle = color;
      ctx.fillRect(x, y, barW, h);
      ctx.fillStyle = "#d9e5ff";
      ctx.font = "12px sans-serif";
      ctx.fillText(String(value), x, y - 4);
      ctx.fillStyle = "#95abd2";
      ctx.fillText(label, x, height - 8);
    });
  }

  function renderDashboardCharts() {
    const events = state.caches.events.slice(0, 120);
    const levelMap = { P0: 0, P1: 0, P2: 0 };
    const marketMap = {};
    for (const row of events) {
      const level = row.importance_level || "P2";
      levelMap[level] = (levelMap[level] || 0) + 1;
      for (const market of row.impacted_markets || []) {
        marketMap[market] = (marketMap[market] || 0) + 1;
      }
    }
    drawBarChart("eventsLevelChart", ["P0", "P1", "P2"], [levelMap.P0 || 0, levelMap.P1 || 0, levelMap.P2 || 0], "#4f81d8");
    const topMarkets = Object.entries(marketMap)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);
    drawBarChart(
      "marketHeatChart",
      topMarkets.map((item) => item[0]),
      topMarkets.map((item) => item[1]),
      "#45a47a"
    );
  }

  function matchesEventFilter(row) {
    const market = $("eventsFilterMarket").value.trim().toLowerCase();
    const score = Number($("eventsFilterScore").value || "0");
    const keyword = $("eventsFilterKeyword").value.trim().toLowerCase();
    if (market && !(row.impacted_markets || []).map((item) => String(item).toLowerCase()).includes(market)) return false;
    if (Number.isFinite(score) && score > 0 && Number(row.importance_score || 0) < score) return false;
    if (keyword && !String(row.title || "").toLowerCase().includes(keyword)) return false;
    return true;
  }

  function renderEventsPage() {
    const pageData = pageSlice("events", state.filteredEvents);
    renderTable(
      "eventsTable",
      [
        { key: "detected_at", title: "时间", render: (r) => htmlEscape(String(r.detected_at || "").replace("T", " ").slice(0, 19)) },
        { key: "title", title: "标题" },
        { key: "importance_level", title: "等级" },
        { key: "importance_score", title: "分数" },
        { key: "impacted_markets", title: "市场", render: (r) => htmlEscape((r.impacted_markets || []).join(",")) },
        { key: "top_impacted_instruments", title: "Top品种", render: (r) => htmlEscape((r.top_impacted_instruments || []).join(",")) },
      ],
      pageData.rows,
      { eventid: "event_id" }
    );
    renderPager("eventsPagination", "events", pageData.total);
  }

  function updateAlertsSelectionBar() {
    $("alertsSelectedCount").textContent = `已选 ${state.selectedAlerts.size} 条`;
    $("alertsAckSelectedBtn").disabled = state.selectedAlerts.size === 0;
    const canRevoke = state.user && (state.user.role === "admin" || state.user.role === "trader");
    $("alertsRevokeSelectedBtn").disabled = state.selectedAlerts.size === 0 || !canRevoke;
  }

  function renderAlertsPage() {
    const pageData = pageSlice("alerts", state.caches.alerts);
    const canRevoke = state.user && (state.user.role === "admin" || state.user.role === "trader");
    renderTable(
      "alertsTable",
      [
        { key: "selected", title: '<input id="selectAllAlerts" type="checkbox" />', render: (r) => `<input type="checkbox" data-alert-check="${htmlEscape(r.alert_id)}" ${state.selectedAlerts.has(r.alert_id) ? "checked" : ""} />` },
        { key: "alert_id", title: "告警ID" },
        { key: "title", title: "标题" },
        { key: "importance_level", title: "等级" },
        { key: "status", title: "状态" },
        { key: "created_at", title: "创建时间", render: (r) => htmlEscape(String(r.created_at || "").replace("T", " ").slice(0, 19)) },
        {
          key: "actions",
          title: "操作",
          render: (r) =>
            `<button data-action="ack" data-alert-id="${htmlEscape(r.alert_id)}">ACK</button>
             ${canRevoke ? `<button data-action="revoke" data-alert-id="${htmlEscape(r.alert_id)}">撤销</button>` : ""}
             <button data-action="detail" data-alert-id="${htmlEscape(r.alert_id)}">详情</button>`,
        },
      ],
      pageData.rows
    );
    renderPager("alertsPagination", "alerts", pageData.total);
    updateAlertsSelectionBar();
  }

  function renderSourcesPage() {
    const pageData = pageSlice("sources", state.caches.sources);
    renderTable(
      "sourcesTable",
      [
        { key: "source_id", title: "source_id" },
        { key: "display_name", title: "名称", render: (r) => htmlEscape(r.display_name || "-") },
        { key: "tier", title: "tier" },
        { key: "region", title: "region" },
        { key: "enabled", title: "enabled" },
        { key: "source_weight", title: "source_weight" },
        { key: "effective_source_weight", title: "effective_weight" },
        {
          key: "ops",
          title: "操作",
          render: (r) =>
            `<button data-action="versions" data-source-id="${htmlEscape(r.source_id)}">版本</button>
             <button data-action="compliance" data-source-id="${htmlEscape(r.source_id)}">合规</button>`,
        },
      ],
      pageData.rows
    );
    renderPager("sourcesPagination", "sources", pageData.total);
  }

  function renderManualPage() {
    const pageData = pageSlice("manual", state.caches.manual);
    renderTable(
      "manualTable",
      [
        { key: "manual_message_id", title: "消息ID" },
        { key: "status", title: "状态" },
        { key: "importance_level", title: "级别" },
        { key: "importance_score", title: "分数" },
        { key: "updated_at", title: "更新时间", render: (r) => htmlEscape(String(r.updated_at || "").replace("T", " ").slice(0, 19)) },
        {
          key: "ops",
          title: "操作",
          render: (r) =>
            `<button data-action="detail" data-mid="${htmlEscape(r.manual_message_id)}">详情</button>
             <button data-action="submit" data-mid="${htmlEscape(r.manual_message_id)}">提交</button>
             <button data-action="publish" data-mid="${htmlEscape(r.manual_message_id)}">发布</button>`,
        },
      ],
      pageData.rows
    );
    renderPager("manualPagination", "manual", pageData.total);
  }

  function renderUsersPage() {
    const pageData = pageSlice("users", state.caches.users);
    renderTable(
      "usersTable",
      [
        { key: "username", title: "用户名" },
        { key: "role", title: "角色" },
        { key: "plan", title: "套餐" },
        { key: "usage", title: "事件用量", render: (r) => htmlEscape(r.usage?.events_ingested || 0) },
        { key: "quota", title: "事件配额", render: (r) => htmlEscape(r.quota?.monthly_event_quota || 0) },
        {
          key: "ops",
          title: "调整套餐",
          render: (r) =>
            `<select data-plan-user="${htmlEscape(r.username)}">
              <option value="basic" ${r.plan === "basic" ? "selected" : ""}>basic</option>
              <option value="pro" ${r.plan === "pro" ? "selected" : ""}>pro</option>
              <option value="enterprise" ${r.plan === "enterprise" ? "selected" : ""}>enterprise</option>
             </select>
             <button data-action="save-plan" data-plan-user="${htmlEscape(r.username)}">保存</button>`,
        },
      ],
      pageData.rows
    );
    renderPager("usersPagination", "users", pageData.total);
  }

  function renderAuditPage() {
    const pageData = pageSlice("audit", state.caches.auditLogs);
    renderTable(
      "auditTable",
      [
        { key: "created_at", title: "时间", render: (r) => htmlEscape(String(r.created_at || "").replace("T", " ").slice(0, 19)) },
        { key: "actor", title: "actor" },
        { key: "action", title: "action" },
        { key: "detail", title: "detail", render: (r) => `<button data-action="audit-detail" data-audit-id="${htmlEscape(r.audit_id)}">查看</button>` },
      ],
      pageData.rows,
      { auditid: "audit_id" }
    );
    renderPager("auditPagination", "audit", pageData.total);
  }

  async function showEventDetails(eventId) {
    const [impact, features, credibility] = await Promise.all([
      api(`/api/v1/events/${eventId}/impact`),
      api(`/api/v1/events/${eventId}/features`),
      api(`/api/v1/events/${eventId}/credibility`),
    ]);
    openDrawer(`事件详情 ${eventId}`, { impact, features, credibility });
  }

  async function loadDashboard() {
    const [metrics, health, events, alerts, queueStats] = await Promise.all([
      api("/api/v1/metrics/summary"),
      api("/api/v1/health"),
      api("/api/v1/events/feed?page=1&page_size=12"),
      api("/api/v1/alerts/feed?limit=20"),
      api("/api/v1/collector/tasks/stats"),
    ]);
    $("kpiEventsTotal").textContent = metrics.events_total;
    $("kpiEventsP0").textContent = metrics.events_p0;
    $("kpiAlertsActive").textContent = metrics.alerts_active;
    $("kpiWebhookQueue").textContent = metrics.webhook.queue_size;
    $("kpiNotificationsQueued").textContent = metrics.notifications_queued;
    $("kpiCollectorQueue").textContent = queueStats.queue_size;
    renderSimpleList(
      "dashboardEvents",
      events.events || [],
      (row) =>
        `<div class="list-item">
          <div class="item-title">${htmlEscape(row.title)}</div>
          <div class="item-meta">${htmlEscape(row.importance_level)} | ${htmlEscape(row.importance_score)} | ${htmlEscape(
            (row.impacted_markets || []).join(",")
          )}</div>
        </div>`
    );
    $("dashboardHealth").textContent = JSON.stringify({ health, alerts_total: alerts.total }, null, 2);
    renderDashboardCharts();
  }

  async function loadEvents() {
    setLoading("eventsTable", true);
    const market = $("eventsFilterMarket").value.trim();
    const score = Number($("eventsFilterScore").value || "0");
    const keyword = $("eventsFilterKeyword").value.trim().toLowerCase();
    const query = new URLSearchParams({ page: "1", page_size: "240" });
    if (market) query.set("market", market);
    if (Number.isFinite(score) && score > 0) query.set("importance_min", String(score));
    try {
      const payload = await api(`/api/v1/events/feed?${query.toString()}`);
      const rows = (payload.events || []).filter((row) => (keyword ? String(row.title || "").toLowerCase().includes(keyword) : true));
      state.caches.events = rows;
      state.filteredEvents = rows;
      state.liveEventIds = new Set(rows.map((row) => row.event_id));
      state.pagination.events.page = 1;
      renderEventsPage();
      renderDashboardCharts();
    } finally {
      setLoading("eventsTable", false);
    }
  }

  async function loadAlerts() {
    setLoading("alertsTable", true);
    const status = $("alertsStatusFilter").value;
    const qs = new URLSearchParams({ limit: "300" });
    if (status) qs.set("status", status);
    try {
      const payload = await api(`/api/v1/alerts/feed?${qs.toString()}`);
      state.caches.alerts = payload.alerts || [];
      state.selectedAlerts.clear();
      state.pagination.alerts.page = 1;
      renderAlertsPage();
    } finally {
      setLoading("alertsTable", false);
    }
  }

  async function loadSources() {
    setLoading("sourcesTable", true);
    try {
      const payload = await api("/api/v1/sources?enabled=true");
      state.caches.sources = payload.sources || [];
      state.pagination.sources.page = 1;
      renderSourcesPage();
    } finally {
      setLoading("sourcesTable", false);
    }
  }

  async function loadManualMessages() {
    setLoading("manualTable", true);
    try {
      const payload = await api("/api/v1/manual/messages?limit=240");
      state.caches.manual = payload.rows || [];
      state.pagination.manual.page = 1;
      renderManualPage();
    } finally {
      setLoading("manualTable", false);
    }
  }

  async function loadProfileAndUsers() {
    const profile = await api("/api/v1/users/me");
    state.user = { username: profile.username, role: profile.role };
    state.env = profile.username === "demo" ? "dev" : "prod-like";
    $("envBadge").textContent = `ENV: ${state.env}`;
    applyRoleVisibility();
    $("prefKeywords").value = (profile.preferences?.focus_keywords || []).join(",");
    $("prefMarkets").value = (profile.preferences?.focus_markets || []).join(",");
    $("prefInstruments").value = (profile.preferences?.focus_instruments || []).join(",");
    $("prefAlertLevel").value = profile.preferences?.alert_level_min || "P2";
    $("profileResult").textContent = JSON.stringify(profile, null, 2);

    if (state.user.role === "admin") {
      const users = await api("/api/v1/admin/users");
      state.caches.users = users.rows || [];
      state.pagination.users.page = 1;
      renderUsersPage();
    } else {
      $("usersTable").innerHTML = "<div class='list-item'>仅管理员可查看全部用户配额。</div>";
      $("usersPagination").innerHTML = "";
    }
  }

  async function loadAdminPanel() {
    if (!state.user || state.user.role !== "admin") {
      $("adminInfo").textContent = "仅管理员可访问。";
      $("auditTable").innerHTML = "<div class='list-item'>仅管理员可访问。</div>";
      $("auditPagination").innerHTML = "";
      return;
    }
    const [metrics, modelStatus, notifyStatus, queueStats, auditLogs] = await Promise.all([
      api("/api/v1/metrics/summary"),
      api("/api/v1/model/inference/status"),
      api("/api/v1/notifications/status"),
      api("/api/v1/collector/tasks/stats"),
      api("/api/v1/audit/logs?limit=200"),
    ]);
    $("adminInfo").textContent = JSON.stringify(
      { metrics, model_inference: modelStatus, notifications: notifyStatus, collector_queue: queueStats },
      null,
      2
    );
    state.caches.auditLogs = auditLogs.logs || [];
    state.pagination.audit.page = 1;
    renderAuditPage();
  }

  function runGlobalSearch() {
    const keyword = $("globalSearchInput").value.trim().toLowerCase();
    if (!keyword) {
      $("searchResultsBar").classList.add("hidden");
      $("searchResults").innerHTML = "";
      return;
    }
    const rows = [];
    for (const event of state.caches.events) {
      if (String(event.title || "").toLowerCase().includes(keyword)) {
        rows.push({ type: "event", label: event.title, id: event.event_id });
      }
    }
    for (const alert of state.caches.alerts) {
      if (String(alert.title || "").toLowerCase().includes(keyword)) {
        rows.push({ type: "alert", label: alert.title, id: alert.alert_id });
      }
    }
    for (const source of state.caches.sources) {
      const sourceText = `${source.source_id || ""} ${source.display_name || ""}`.toLowerCase();
      if (sourceText.includes(keyword)) {
        rows.push({ type: "source", label: `${source.source_id} ${source.display_name || ""}`, id: source.source_id });
      }
    }
    $("searchResultsBar").classList.remove("hidden");
    renderSimpleList(
      "searchResults",
      rows.slice(0, 30),
      (row) =>
        `<div class="list-item">
          <div class="item-title">[${htmlEscape(row.type)}] ${htmlEscape(row.label)}</div>
          <div class="item-meta">${htmlEscape(row.id)}</div>
        </div>`
    );
  }

  function exportEvents() {
    const blob = new Blob([JSON.stringify(state.filteredEvents, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `events_${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  function scheduleWsReconnect() {
    if (!state.token || state.liveStream.retryTimer) return;
    state.liveStream.retryTimer = window.setTimeout(() => {
      state.liveStream.retryTimer = null;
      connectEventStream().catch((err) => toast(`实时连接重试失败: ${err.message}`, "warn"));
    }, 5000);
  }

  function disconnectEventStream() {
    if (state.liveStream.retryTimer) {
      clearTimeout(state.liveStream.retryTimer);
      state.liveStream.retryTimer = null;
    }
    if (state.liveStream.socket) {
      state.liveStream.socket.close();
      state.liveStream.socket = null;
    }
    setRealtimeBadge(false);
  }

  function onLiveEvent(rawData) {
    let payload;
    try {
      payload = JSON.parse(rawData);
    } catch (_err) {
      return;
    }
    if (!payload || !payload.event_id) return;
    if (state.liveEventIds.has(payload.event_id)) return;
    state.liveEventIds.add(payload.event_id);
    state.caches.events.unshift(payload);
    if (state.caches.events.length > 500) {
      state.caches.events.length = 500;
    }
    if (matchesEventFilter(payload)) {
      state.filteredEvents.unshift(payload);
      if (state.filteredEvents.length > 500) {
        state.filteredEvents.length = 500;
      }
      if (state.activeView === "eventsView") {
        renderEventsPage();
      }
    }
    if (state.activeView === "dashboardView") {
      renderDashboardCharts();
    }
    toast(`实时事件: ${payload.title || payload.event_id}`);
  }

  async function connectEventStream() {
    if (!state.token) return;
    disconnectEventStream();
    try {
      const metadata = await api("/api/v1/stream/events");
      if (metadata.path) {
        state.liveStream.path = metadata.path;
      }
    } catch (_err) {
      state.liveStream.path = "/ws/events";
    }
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}${state.liveStream.path}`;
    const socket = new WebSocket(wsUrl);
    state.liveStream.socket = socket;
    socket.onopen = () => setRealtimeBadge(true);
    socket.onerror = () => setRealtimeBadge(false);
    socket.onclose = () => {
      state.liveStream.socket = null;
      setRealtimeBadge(false);
      scheduleWsReconnect();
    };
    socket.onmessage = (event) => onLiveEvent(event.data);
  }

  async function onLoginSubmit(event) {
    event.preventDefault();
    try {
      const payload = await api("/api/v1/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: { username: $("loginUsername").value.trim(), password: $("loginPassword").value },
      });
      state.token = payload.access_token;
      localStorage.setItem("qsm_access_token", state.token);
      $("loginPanel").classList.add("hidden");
      $("appShell").classList.remove("hidden");
      await bootstrapAfterLogin();
      await connectEventStream();
      toast("登录成功");
    } catch (err) {
      toast(`登录失败: ${err.message}`, "error");
    }
  }

  async function bootstrapAfterLogin() {
    await loadProfileAndUsers();
    await Promise.all([loadDashboard(), loadEvents(), loadAlerts(), loadSources(), loadManualMessages(), loadAdminPanel()]);
    setView("dashboardView");
  }

  function changePage(key, action) {
    const pageState = state.pagination[key];
    if (!pageState) return;
    if (action === "prev") pageState.page -= 1;
    if (action === "next") pageState.page += 1;
    if (key === "events") renderEventsPage();
    if (key === "alerts") renderAlertsPage();
    if (key === "sources") renderSourcesPage();
    if (key === "manual") renderManualPage();
    if (key === "users") renderUsersPage();
    if (key === "audit") renderAuditPage();
  }

  async function bulkAckSelected() {
    if (state.selectedAlerts.size === 0) return;
    let success = 0;
    let failed = 0;
    for (const alertId of state.selectedAlerts) {
      try {
        await api(`/api/v1/alerts/${alertId}/ack`, { method: "POST", body: { note: "ui_bulk_ack" } });
        success += 1;
      } catch (_err) {
        failed += 1;
      }
    }
    await Promise.all([loadAlerts(), loadDashboard()]);
    toast(`批量ACK完成: success=${success}, failed=${failed}`, failed > 0 ? "warn" : "info");
  }

  async function bulkRevokeSelected() {
    if (state.selectedAlerts.size === 0) return;
    let success = 0;
    let failed = 0;
    for (const alertId of state.selectedAlerts) {
      try {
        await api(`/api/v1/alerts/${alertId}/revoke?reason=ui_bulk_revoke`, { method: "POST" });
        success += 1;
      } catch (_err) {
        failed += 1;
      }
    }
    await Promise.all([loadAlerts(), loadDashboard()]);
    toast(`批量撤销完成: success=${success}, failed=${failed}`, failed > 0 ? "warn" : "info");
  }

  function bindEvents() {
    $("loginForm").addEventListener("submit", onLoginSubmit);
    $("logoutBtn").addEventListener("click", () => {
      disconnectEventStream();
      state.token = "";
      state.user = null;
      localStorage.removeItem("qsm_access_token");
      $("appShell").classList.add("hidden");
      $("loginPanel").classList.remove("hidden");
      closeDrawer();
      toast("已退出登录");
    });

    document.querySelectorAll(".nav-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const viewId = btn.getAttribute("data-view");
        if (viewId) setView(viewId);
      });
    });

    $("drawerCloseBtn").addEventListener("click", closeDrawer);
    $("detailDrawer").addEventListener("click", (event) => {
      if (event.target === $("detailDrawer")) closeDrawer();
    });

    Object.entries(PAGERS).forEach(([containerId, key]) => {
      $(containerId).addEventListener("click", (event) => {
        const target = event.target;
        if (!(target instanceof HTMLElement)) return;
        const action = target.getAttribute("data-page-action");
        const pageKey = target.getAttribute("data-page-key");
        if (!action || pageKey !== key) return;
        changePage(key, action);
      });
    });

    $("dashboardRefreshBtn").addEventListener("click", async () => {
      try {
        await loadDashboard();
        toast("总览刷新完成");
      } catch (err) {
        toast(err.message, "error");
      }
    });

    $("eventsFilterForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        await loadEvents();
      } catch (err) {
        toast(`事件查询失败: ${err.message}`, "error");
      }
    });
    $("eventsExportBtn").addEventListener("click", exportEvents);
    $("eventsTable").addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.tagName === "BUTTON" || target.tagName === "INPUT" || target.tagName === "A") return;
      const tr = target.closest("tr");
      if (!tr) return;
      const eventId = tr.getAttribute("data-eventid");
      if (!eventId) return;
      try {
        await showEventDetails(eventId);
      } catch (err) {
        toast(`加载事件详情失败: ${err.message}`, "error");
      }
    });

    $("ingestForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        await api("/api/v1/events/ingest", {
          method: "POST",
          body: {
            source_id: $("ingestSource").value.trim(),
            title: $("ingestTitle").value.trim(),
            content: $("ingestContent").value.trim(),
            related_instruments: toCsvList($("ingestInstruments").value),
          },
        });
        $("ingestTitle").value = "";
        $("ingestContent").value = "";
        await Promise.all([loadEvents(), loadAlerts(), loadDashboard()]);
        toast("事件接入成功");
      } catch (err) {
        toast(`事件接入失败: ${err.message}`, "error");
      }
    });

    $("alertsRefreshBtn").addEventListener("click", () => loadAlerts().catch((err) => toast(err.message, "error")));
    $("alertsStatusFilter").addEventListener("change", () => loadAlerts().catch((err) => toast(err.message, "error")));
    $("alertsAckSelectedBtn").addEventListener("click", () => bulkAckSelected().catch((err) => toast(err.message, "error")));
    $("alertsRevokeSelectedBtn").addEventListener("click", () => bulkRevokeSelected().catch((err) => toast(err.message, "error")));
    $("alertsEscalateBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/alerts/escalate?force=true&limit=50", { method: "POST" });
        toast(`升级完成: escalated=${result.escalated}`);
        await loadAlerts();
      } catch (err) {
        toast(`升级失败: ${err.message}`, "error");
      }
    });
    $("alertsTable").addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      if (target instanceof HTMLInputElement && target.id === "selectAllAlerts") {
        const checked = target.checked;
        const pageRows = pageSlice("alerts", state.caches.alerts).rows;
        for (const row of pageRows) {
          if (checked) {
            state.selectedAlerts.add(row.alert_id);
          } else {
            state.selectedAlerts.delete(row.alert_id);
          }
        }
        renderAlertsPage();
        return;
      }
      if (target instanceof HTMLInputElement && target.dataset.alertCheck) {
        const alertId = target.dataset.alertCheck;
        if (target.checked) state.selectedAlerts.add(alertId);
        else state.selectedAlerts.delete(alertId);
        updateAlertsSelectionBar();
        return;
      }
      const action = target.getAttribute("data-action");
      const alertId = target.getAttribute("data-alert-id");
      if (!action || !alertId) return;
      try {
        if (action === "ack") {
          await api(`/api/v1/alerts/${alertId}/ack`, { method: "POST", body: { note: "ui_ack" } });
          toast(`告警 ${alertId} 已 ACK`);
          await loadAlerts();
        } else if (action === "revoke") {
          await api(`/api/v1/alerts/${alertId}/revoke?reason=ui_revoke`, { method: "POST" });
          toast(`告警 ${alertId} 已撤销`);
          await loadAlerts();
        } else if (action === "detail") {
          const row = state.caches.alerts.find((item) => item.alert_id === alertId);
          openDrawer(`告警详情 ${alertId}`, row || { alert_id: alertId });
        }
      } catch (err) {
        toast(`告警操作失败: ${err.message}`, "error");
      }
    });

    $("sourcesRefreshBtn").addEventListener("click", () => loadSources().catch((err) => toast(err.message, "error")));
    $("sourcesReloadBtn").addEventListener("click", async () => {
      try {
        await api("/api/v1/sources/reload", { method: "POST" });
        await loadSources();
        toast("来源配置已重载");
      } catch (err) {
        toast(`重载失败: ${err.message}`, "error");
      }
    });
    $("sourcesTable").addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const action = target.getAttribute("data-action");
      const sourceId = target.getAttribute("data-source-id");
      if (!action || !sourceId) return;
      try {
        if (action === "versions") {
          const payload = await api(`/api/v1/sources/${encodeURIComponent(sourceId)}/versions?limit=20`);
          openDrawer(`来源版本 ${sourceId}`, payload);
        } else if (action === "compliance") {
          const payload = await api(`/api/v1/sources/${encodeURIComponent(sourceId)}/compliance`);
          openDrawer(`来源合规 ${sourceId}`, payload);
        }
      } catch (err) {
        toast(`来源操作失败: ${err.message}`, "error");
      }
    });
    $("patchSourceForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      const sourceId = $("sourceIdInput").value.trim();
      try {
        await api(`/api/v1/sources/${encodeURIComponent(sourceId)}`, {
          method: "PATCH",
          body: {
            enabled: $("sourceEnabledInput").value === "true",
            source_weight: Number($("sourceWeightInput").value || "0"),
          },
        });
        await loadSources();
        toast("来源更新成功");
      } catch (err) {
        toast(`来源更新失败: ${err.message}`, "error");
      }
    });

    $("manualRefreshBtn").addEventListener("click", () => loadManualMessages().catch((err) => toast(err.message, "error")));
    $("manualForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        const payload = await api("/api/v1/manual/messages", {
          method: "POST",
          body: {
            title: $("manualTitle").value.trim(),
            content: $("manualContent").value.trim(),
            operator_id: $("manualOperatorId").value.trim(),
            operator_role: $("manualOperatorRole").value.trim(),
            related_instruments: toCsvList($("manualInstruments").value),
          },
        });
        $("manualResult").textContent = JSON.stringify(payload, null, 2);
        await loadManualMessages();
        toast("手工消息创建成功");
      } catch (err) {
        toast(`手工消息创建失败: ${err.message}`, "error");
      }
    });
    $("manualTable").addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const action = target.getAttribute("data-action");
      const mid = target.getAttribute("data-mid");
      if (!action || !mid) return;
      try {
        if (action === "detail") {
          const row = state.caches.manual.find((item) => item.manual_message_id === mid);
          openDrawer(`手工消息 ${mid}`, row || { manual_message_id: mid });
        } else if (action === "submit") {
          await api(`/api/v1/manual/messages/${mid}/submit`, { method: "POST" });
          await loadManualMessages();
          toast(`消息 ${mid} 已提交`);
        } else if (action === "publish") {
          await api(`/api/v1/manual/messages/${mid}/publish`, { method: "POST" });
          await loadManualMessages();
          toast(`消息 ${mid} 已发布`);
        }
      } catch (err) {
        toast(`手工消息操作失败: ${err.message}`, "error");
      }
    });

    $("prefsForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        const payload = await api("/api/v1/users/me/preferences", {
          method: "PUT",
          body: {
            focus_domains: [],
            focus_keywords: toCsvList($("prefKeywords").value),
            focus_markets: toCsvList($("prefMarkets").value),
            focus_instruments: toCsvList($("prefInstruments").value),
            alert_level_min: $("prefAlertLevel").value,
          },
        });
        $("profileResult").textContent = JSON.stringify(payload, null, 2);
        toast("个人配置已保存");
      } catch (err) {
        toast(`保存失败: ${err.message}`, "error");
      }
    });
    $("usersRefreshBtn").addEventListener("click", () => loadProfileAndUsers().catch((err) => toast(err.message, "error")));
    $("usersTable").addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const action = target.getAttribute("data-action");
      const username = target.getAttribute("data-plan-user");
      if (action !== "save-plan" || !username) return;
      const select = document.querySelector(`select[data-plan-user="${username}"]`);
      if (!(select instanceof HTMLSelectElement)) return;
      try {
        await api(`/api/v1/admin/quotas/users/${encodeURIComponent(username)}?plan=${encodeURIComponent(select.value)}`, {
          method: "PUT",
        });
        await loadProfileAndUsers();
        toast(`用户 ${username} 套餐已更新为 ${select.value}`);
      } catch (err) {
        toast(`更新套餐失败: ${err.message}`, "error");
      }
    });

    $("adminRefreshBtn").addEventListener("click", () => loadAdminPanel().catch((err) => toast(err.message, "error")));
    $("auditRefreshBtn").addEventListener("click", () => loadAdminPanel().catch((err) => toast(err.message, "error")));
    $("processNotificationsBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/notifications/process?limit=100", { method: "POST" });
        toast(`通知处理：processed=${result.processed} delivered=${result.delivered} failed=${result.failed}`);
        await loadAdminPanel();
      } catch (err) {
        toast(`通知处理失败: ${err.message}`, "error");
      }
    });
    $("retryNotificationsBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/notifications/retry-failures?limit=100", { method: "POST" });
        toast(`失败通知重试入队：${result.requeued}`);
        await loadAdminPanel();
      } catch (err) {
        toast(`通知重试失败: ${err.message}`, "error");
      }
    });
    $("processCollectorTasksBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/collector/tasks/process?max_tasks=20", { method: "POST" });
        toast(`采集任务处理：${result.processed}`);
        await loadAdminPanel();
      } catch (err) {
        toast(`采集任务处理失败: ${err.message}`, "error");
      }
    });
    $("auditTable").addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const action = target.getAttribute("data-action");
      const auditId = target.getAttribute("data-audit-id");
      if (action !== "audit-detail" || !auditId) return;
      const row = state.caches.auditLogs.find((item) => item.audit_id === auditId);
      openDrawer(`审计日志 ${auditId}`, row || { audit_id: auditId });
    });

    $("globalSearchBtn").addEventListener("click", runGlobalSearch);
    $("globalSearchInput").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        runGlobalSearch();
      }
    });
    $("clearSearchBtn").addEventListener("click", () => {
      $("globalSearchInput").value = "";
      $("searchResultsBar").classList.add("hidden");
      $("searchResults").innerHTML = "";
    });
  }

  async function init() {
    bindEvents();
    setRealtimeBadge(false);
    if (!state.token) return;
    $("loginPanel").classList.add("hidden");
    $("appShell").classList.remove("hidden");
    try {
      await bootstrapAfterLogin();
      await connectEventStream();
    } catch (err) {
      state.token = "";
      localStorage.removeItem("qsm_access_token");
      $("appShell").classList.add("hidden");
      $("loginPanel").classList.remove("hidden");
      toast(`自动恢复会话失败，请重新登录: ${err.message}`, "error");
    }
  }

  init().catch((err) => toast(`初始化失败: ${err.message}`, "error"));
})();
