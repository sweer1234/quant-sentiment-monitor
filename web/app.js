(function () {
  const state = {
    token: localStorage.getItem("qsm_access_token") || "",
    user: null,
  };

  const $ = (id) => document.getElementById(id);

  function toast(msg) {
    $("toast").textContent = msg || "";
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

  function bindTabs() {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
        document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
        btn.classList.add("active");
        const tabId = btn.getAttribute("data-tab");
        const tab = document.getElementById(tabId);
        if (tab) tab.classList.add("active");
      });
    });
  }

  function updateSessionInfo() {
    const box = $("sessionInfo");
    if (!state.user) {
      box.textContent = "未登录";
      return;
    }
    box.innerHTML = `已登录：${state.user.username} (${state.user.role}) <button id="logoutBtn">退出</button>`;
    $("logoutBtn").addEventListener("click", () => {
      state.token = "";
      state.user = null;
      localStorage.removeItem("qsm_access_token");
      $("appPanel").classList.add("hidden");
      $("loginPanel").classList.remove("hidden");
      updateSessionInfo();
      toast("已退出登录");
    });
  }

  function renderEvents(rows) {
    const container = $("eventsList");
    container.innerHTML = "";
    rows.forEach((row) => {
      const div = document.createElement("div");
      div.className = "list-item";
      div.innerHTML = `
        <div class="item-title">${row.title}</div>
        <div class="item-meta">级别=${row.importance_level} 分数=${row.importance_score} 市场=${(row.impacted_markets || []).join(",")}</div>
        <div class="item-meta">Top=${(row.top_impacted_instruments || []).join(",")} net=${row.net_bias_score}</div>
      `;
      container.appendChild(div);
    });
  }

  async function loadEvents() {
    const payload = await api("/api/v1/events/feed?page=1&page_size=30");
    renderEvents(payload.events || []);
  }

  function renderAlerts(rows) {
    const container = $("alertsList");
    container.innerHTML = "";
    rows.forEach((row) => {
      const controls = [];
      controls.push(`<button data-action="ack" data-alert-id="${row.alert_id}">ACK</button>`);
      if (state.user && (state.user.role === "admin" || state.user.role === "trader")) {
        controls.push(`<button data-action="revoke" data-alert-id="${row.alert_id}">REVOKE</button>`);
      }
      const div = document.createElement("div");
      div.className = "list-item";
      div.innerHTML = `
        <div class="item-title">${row.title}</div>
        <div class="item-meta">alert=${row.alert_id} level=${row.importance_level} status=${row.status}</div>
        <div class="actions">${controls.join("")}</div>
      `;
      container.appendChild(div);
    });
  }

  async function loadAlerts() {
    const payload = await api("/api/v1/alerts/feed?importance_min=P2&limit=100");
    renderAlerts(payload.alerts || []);
  }

  function bindAlertActions() {
    $("alertsList").addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const action = target.getAttribute("data-action");
      const alertId = target.getAttribute("data-alert-id");
      if (!action || !alertId) return;
      try {
        if (action === "ack") {
          await api(`/api/v1/alerts/${alertId}/ack`, { method: "POST", body: { note: "ui_ack" } });
        } else if (action === "revoke") {
          await api(`/api/v1/alerts/${alertId}/revoke?reason=ui_revoke`, { method: "POST" });
        }
        toast(`告警 ${alertId} 操作成功`);
        await loadAlerts();
      } catch (err) {
        toast(`告警操作失败: ${err.message}`);
      }
    });
  }

  function renderSources(rows) {
    const container = $("sourcesList");
    container.innerHTML = "";
    rows.slice(0, 40).forEach((row) => {
      const div = document.createElement("div");
      div.className = "list-item";
      div.innerHTML = `
        <div class="item-title">${row.source_id} (${row.display_name || "-"})</div>
        <div class="item-meta">enabled=${row.enabled} tier=${row.tier} region=${row.region} weight=${row.source_weight}</div>
      `;
      container.appendChild(div);
    });
  }

  async function loadSources() {
    const payload = await api("/api/v1/sources?enabled=true");
    renderSources(payload.sources || []);
  }

  async function loadProfile() {
    const profile = await api("/api/v1/users/me");
    state.user = { username: profile.username, role: profile.role };
    updateSessionInfo();
    const prefs = profile.preferences || {};
    $("prefKeywords").value = (prefs.focus_keywords || []).join(",");
    $("prefMarkets").value = (prefs.focus_markets || []).join(",");
    $("prefInstruments").value = (prefs.focus_instruments || []).join(",");
    $("prefAlertLevel").value = prefs.alert_level_min || "P2";
    $("profileResult").textContent = JSON.stringify(profile, null, 2);
    if (state.user.role !== "admin") {
      $("adminTab").innerHTML = "<h2>系统面板</h2><div class='hint'>仅管理员可访问。</div>";
    }
  }

  async function loadAdminInfo() {
    try {
      const [metrics, modelStatus, notifyStatus, queueStats] = await Promise.all([
        api("/api/v1/metrics/summary"),
        api("/api/v1/model/inference/status"),
        api("/api/v1/notifications/status"),
        api("/api/v1/collector/tasks/stats"),
      ]);
      $("adminInfo").textContent = JSON.stringify(
        { metrics, model_inference: modelStatus, notifications: notifyStatus, collector_queue: queueStats },
        null,
        2
      );
    } catch (err) {
      $("adminInfo").textContent = `无权限或加载失败: ${err.message}`;
    }
  }

  async function onLoginSubmit(event) {
    event.preventDefault();
    try {
      const payload = await api("/api/v1/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: {
          username: $("loginUsername").value.trim(),
          password: $("loginPassword").value,
        },
      });
      state.token = payload.access_token;
      localStorage.setItem("qsm_access_token", state.token);
      $("loginPanel").classList.add("hidden");
      $("appPanel").classList.remove("hidden");
      await bootstrapAfterLogin();
      toast("登录成功");
    } catch (err) {
      toast(`登录失败: ${err.message}`);
    }
  }

  async function bootstrapAfterLogin() {
    await loadProfile();
    await Promise.all([loadEvents(), loadAlerts(), loadSources(), loadAdminInfo()]);
  }

  function bindForms() {
    $("loginForm").addEventListener("submit", onLoginSubmit);
    $("refreshEventsBtn").addEventListener("click", () => loadEvents().catch((e) => toast(e.message)));
    $("refreshAlertsBtn").addEventListener("click", () => loadAlerts().catch((e) => toast(e.message)));
    $("refreshSourcesBtn").addEventListener("click", () => loadSources().catch((e) => toast(e.message)));
    $("refreshAdminBtn").addEventListener("click", () => loadAdminInfo().catch((e) => toast(e.message)));
    $("processNotificationsBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/notifications/process?limit=50", { method: "POST" });
        toast(`通知处理完成: ${JSON.stringify(result)}`);
        await loadAdminInfo();
      } catch (err) {
        toast(`通知处理失败: ${err.message}`);
      }
    });
    $("processCollectorTasksBtn").addEventListener("click", async () => {
      try {
        const result = await api("/api/v1/collector/tasks/process?max_tasks=5", { method: "POST" });
        toast(`采集任务处理完成: ${result.processed}`);
        await loadAdminInfo();
      } catch (err) {
        toast(`采集任务处理失败: ${err.message}`);
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
        toast("事件提交成功");
        $("ingestTitle").value = "";
        $("ingestContent").value = "";
        await Promise.all([loadEvents(), loadAlerts(), loadAdminInfo()]);
      } catch (err) {
        toast(`事件提交失败: ${err.message}`);
      }
    });

    $("patchSourceForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      const sourceId = $("sourceIdInput").value.trim();
      try {
        const enabledRaw = $("sourceEnabledInput").value.trim().toLowerCase();
        const sourceWeight = Number($("sourceWeightInput").value);
        const body = {
          enabled: enabledRaw === "true" || enabledRaw === "1" || enabledRaw === "yes",
          source_weight: Number.isFinite(sourceWeight) ? sourceWeight : null,
        };
        await api(`/api/v1/sources/${encodeURIComponent(sourceId)}`, { method: "PATCH", body });
        toast("站点更新成功");
        await loadSources();
      } catch (err) {
        toast(`站点更新失败: ${err.message}`);
      }
    });

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
        toast("手工消息创建成功");
      } catch (err) {
        toast(`手工消息创建失败: ${err.message}`);
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
        toast("个人配置更新成功");
      } catch (err) {
        toast(`个人配置更新失败: ${err.message}`);
      }
    });
  }

  async function init() {
    bindTabs();
    bindAlertActions();
    bindForms();
    updateSessionInfo();
    if (state.token) {
      $("loginPanel").classList.add("hidden");
      $("appPanel").classList.remove("hidden");
      try {
        await bootstrapAfterLogin();
      } catch (err) {
        state.token = "";
        localStorage.removeItem("qsm_access_token");
        $("loginPanel").classList.remove("hidden");
        $("appPanel").classList.add("hidden");
        updateSessionInfo();
        toast(`自动恢复会话失败，请重新登录: ${err.message}`);
      }
    }
  }

  init().catch((err) => toast(`初始化失败: ${err.message}`));
})();
