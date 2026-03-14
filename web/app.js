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
    },
    filteredEvents: [],
  };

  const $ = (id) => document.getElementById(id);

  function toast(msg) {
    $("toast").textContent = msg || "";
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
      throw new Error(`${response.status} ${detail}`);
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
    const label = activeBtn ? activeBtn.textContent : "总览";
    $("breadcrumb").textContent = label || "总览";
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
  }

  async function loadEvents() {
    const market = $("eventsFilterMarket").value.trim();
    const score = Number($("eventsFilterScore").value || "0");
    const keyword = $("eventsFilterKeyword").value.trim().toLowerCase();
    const query = new URLSearchParams({ page: "1", page_size: "120" });
    if (market) query.set("market", market);
    if (Number.isFinite(score) && score > 0) query.set("importance_min", String(score));
    const payload = await api(`/api/v1/events/feed?${query.toString()}`);
    const rows = (payload.events || []).filter((row) => (keyword ? String(row.title || "").toLowerCase().includes(keyword) : true));
    state.caches.events = rows;
    state.filteredEvents = rows;
    renderTable(
      "eventsTable",
      [
        { key: "detected_at", title: "时间", render: (r) => htmlEscape(String(r.detected_at || "").replace("T", " ").slice(0, 19)) },
        { key: "title", title: "标题" },
        { key: "importance_level", title: "等级" },
        { key: "importance_score", title: "分数" },
        { key: "impacted_markets", title: "市场", render: (r) => htmlEscape((r.impacted_markets || []).join(",")) },
        {
          key: "top_impacted_instruments",
          title: "Top品种",
          render: (r) => htmlEscape((r.top_impacted_instruments || []).join(",")),
        },
      ],
      rows,
      { eventid: "event_id" }
    );
  }

  async function showEventDetails(eventId) {
    const [impact, features, credibility] = await Promise.all([
      api(`/api/v1/events/${eventId}/impact`),
      api(`/api/v1/events/${eventId}/features`),
      api(`/api/v1/events/${eventId}/credibility`),
    ]);
    openDrawer(`事件详情 ${eventId}`, { impact, features, credibility });
  }

  async function loadAlerts() {
    const status = $("alertsStatusFilter").value;
    const qs = new URLSearchParams({ limit: "200" });
    if (status) qs.set("status", status);
    const payload = await api(`/api/v1/alerts/feed?${qs.toString()}`);
    const rows = payload.alerts || [];
    state.caches.alerts = rows;
    const canRevoke = state.user && (state.user.role === "admin" || state.user.role === "trader");
    renderTable(
      "alertsTable",
      [
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
      rows
    );
  }

  async function loadSources() {
    const payload = await api("/api/v1/sources?enabled=true");
    const rows = payload.sources || [];
    state.caches.sources = rows;
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
      rows
    );
  }

  async function loadManualMessages() {
    const payload = await api("/api/v1/manual/messages?limit=100");
    const rows = payload.rows || [];
    state.caches.manual = rows;
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
      rows
    );
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
        state.caches.users
      );
    } else {
      $("usersTable").innerHTML = "<div class='list-item'>仅管理员可查看全部用户配额。</div>";
    }
  }

  async function loadAdminPanel() {
    if (!state.user || state.user.role !== "admin") {
      $("adminInfo").textContent = "仅管理员可访问。";
      return;
    }
    const [metrics, modelStatus, notifyStatus, queueStats, auditLogs] = await Promise.all([
      api("/api/v1/metrics/summary"),
      api("/api/v1/model/inference/status"),
      api("/api/v1/notifications/status"),
      api("/api/v1/collector/tasks/stats"),
      api("/api/v1/audit/logs?limit=30"),
    ]);
    $("adminInfo").textContent = JSON.stringify(
      { metrics, model_inference: modelStatus, notifications: notifyStatus, collector_queue: queueStats },
      null,
      2
    );
    renderTable(
      "auditTable",
      [
        { key: "created_at", title: "时间", render: (r) => htmlEscape(String(r.created_at || "").replace("T", " ").slice(0, 19)) },
        { key: "actor", title: "actor" },
        { key: "action", title: "action" },
        { key: "detail", title: "detail", render: (r) => `<button data-action="audit-detail" data-audit-id="${htmlEscape(r.audit_id)}">查看</button>` },
      ],
      auditLogs.logs || [],
      { auditid: "audit_id" }
    );
    state.caches.auditLogs = auditLogs.logs || [];
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
      if (String(source.source_id || "").toLowerCase().includes(keyword) || String(source.display_name || "").toLowerCase().includes(keyword)) {
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
      toast("登录成功");
    } catch (err) {
      toast(`登录失败: ${err.message}`);
    }
  }

  async function bootstrapAfterLogin() {
    await loadProfileAndUsers();
    await Promise.all([loadDashboard(), loadEvents(), loadAlerts(), loadSources(), loadManualMessages(), loadAdminPanel()]);
    setView("dashboardView");
  }

  function bindEvents() {
    $("loginForm").addEventListener("submit", onLoginSubmit);
    $("logoutBtn").addEventListener("click", () => {
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

    $("dashboardRefreshBtn").addEventListener("click", async () => {
      try {
        await loadDashboard();
        toast("总览刷新完成");
      } catch (err) {
        toast(err.message);
      }
    });

    $("eventsFilterForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        await loadEvents();
      } catch (err) {
        toast(`事件查询失败: ${err.message}`);
      }
    });
    $("eventsExportBtn").addEventListener("click", exportEvents);
    $("eventsTable").addEventListener("click", async (event) => {
      const tr = event.target.closest("tr");
      if (!tr) return;
      const eventId = tr.getAttribute("data-eventid");
      if (!eventId) return;
      try {
        await showEventDetails(eventId);
      } catch (err) {
        toast(`加载事件详情失败: ${err.message}`);
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
        toast(`事件接入失败: ${err.message}`);
      }
    });

    $("alertsRefreshBtn").addEventListener("click", () => loadAlerts().catch((err) => toast(err.message)));
    $("alertsStatusFilter").addEventListener("change", () => loadAlerts().catch((err) => toast(err.message)));
    $("alertsEscalateBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/alerts/escalate?force=true&limit=50", { method: "POST" });
        toast(`升级完成: escalated=${result.escalated}`);
        await loadAlerts();
      } catch (err) {
        toast(`升级失败: ${err.message}`);
      }
    });
    $("alertsTable").addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
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
        toast(`告警操作失败: ${err.message}`);
      }
    });

    $("sourcesRefreshBtn").addEventListener("click", () => loadSources().catch((err) => toast(err.message)));
    $("sourcesReloadBtn").addEventListener("click", async () => {
      try {
        await api("/api/v1/sources/reload", { method: "POST" });
        await loadSources();
        toast("来源配置已重载");
      } catch (err) {
        toast(`重载失败: ${err.message}`);
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
        toast(`来源操作失败: ${err.message}`);
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
        toast(`来源更新失败: ${err.message}`);
      }
    });

    $("manualRefreshBtn").addEventListener("click", () => loadManualMessages().catch((err) => toast(err.message)));
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
        toast(`手工消息创建失败: ${err.message}`);
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
        toast(`手工消息操作失败: ${err.message}`);
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
        toast(`保存失败: ${err.message}`);
      }
    });
    $("usersRefreshBtn").addEventListener("click", () => loadProfileAndUsers().catch((err) => toast(err.message)));
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
        toast(`更新套餐失败: ${err.message}`);
      }
    });

    $("adminRefreshBtn").addEventListener("click", () => loadAdminPanel().catch((err) => toast(err.message)));
    $("auditRefreshBtn").addEventListener("click", () => loadAdminPanel().catch((err) => toast(err.message)));
    $("processNotificationsBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/notifications/process?limit=100", { method: "POST" });
        toast(`通知处理：processed=${result.processed} delivered=${result.delivered} failed=${result.failed}`);
        await loadAdminPanel();
      } catch (err) {
        toast(`通知处理失败: ${err.message}`);
      }
    });
    $("retryNotificationsBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/notifications/retry-failures?limit=100", { method: "POST" });
        toast(`失败通知重试入队：${result.requeued}`);
        await loadAdminPanel();
      } catch (err) {
        toast(`通知重试失败: ${err.message}`);
      }
    });
    $("processCollectorTasksBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/collector/tasks/process?max_tasks=20", { method: "POST" });
        toast(`采集任务处理：${result.processed}`);
        await loadAdminPanel();
      } catch (err) {
        toast(`采集任务处理失败: ${err.message}`);
      }
    });
    $("auditTable").addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const action = target.getAttribute("data-action");
      const auditId = target.getAttribute("data-audit-id");
      if (action !== "audit-detail" || !auditId) return;
      const row = (state.caches.auditLogs || []).find((item) => item.audit_id === auditId);
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
    if (!state.token) return;
    $("loginPanel").classList.add("hidden");
    $("appShell").classList.remove("hidden");
    try {
      await bootstrapAfterLogin();
    } catch (err) {
      state.token = "";
      localStorage.removeItem("qsm_access_token");
      $("appShell").classList.add("hidden");
      $("loginPanel").classList.remove("hidden");
      toast(`自动恢复会话失败，请重新登录: ${err.message}`);
    }
  }

  init().catch((err) => toast(`初始化失败: ${err.message}`));
})();
