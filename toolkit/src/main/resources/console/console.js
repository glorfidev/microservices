"use strict";

const byId = id => document.getElementById(id);
const dialog = byId("posting-dialog");
const form = byId("posting-form");
const state = { snapshot: null, jobs: [], filter: "All", busy: null, connected: false, jobsOnline: false };
let refreshing = false;
let draft = null;

const nodeActive = node => node.status !== "Stopped";
const serviceTitle = id => id === "job-workflow" ? "Job workflow" : "Posting review";

function element(tag, text, classes) {
  const result = document.createElement(tag);
  if (text != null) result.textContent = text;
  if (classes) result.className = classes;
  return result;
}

function pill(status) {
  return element("span", status, "pill " + status.toLowerCase().replace(/[^a-z]/g, ""));
}

function notice(id, text) {
  const target = byId(id);
  target.hidden = !text;
  target.textContent = text;
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: options.method === "POST" ? {
      "X-Toolkit-Request": "1",
      ...(options.body ? { "Content-Type": "application/json" } : {})
    } : {}
  });
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || "The operation could not be completed.");
  return body;
}

function actionButton(label, target, id, operation, disabled = false) {
  const button = element("button", label, "secondary small");
  button.type = "button";
  button.disabled = disabled || !state.connected || Boolean(state.busy);
  button.setAttribute("aria-label", label + " " + id);
  button.addEventListener("click", () => change(target, id, operation));
  return button;
}

function renderSnapshot() {
  const indicator = byId("connection");
  indicator.textContent = state.connected ? "Local connection" : "Disconnected";
  indicator.classList.toggle("connected", state.connected);
  byId("open-posting").disabled = !state.connected || !state.jobsOnline || !state.snapshot;
  if (!state.snapshot) return;
  const { nodes, services, employers } = state.snapshot;
  byId("online").textContent = nodes.filter(node => node.status === "Up").length + " / " + nodes.length;
  byId("running").textContent = services.filter(service => service.status === "Running").length + " / " + services.length;

  const focusLabel = document.activeElement?.getAttribute("aria-label");
  const nodeRows = nodes.map(node => {
    const row = element("div", null, "item");
    const name = element("div", node.name, "item-name");
    name.append(element("small", node.port ? "127.0.0.1:" + node.port : "No active address"));
    row.append(name, pill(node.status), actionButton(nodeActive(node) ? "Stop" : "Start", "nodes", node.id, nodeActive(node) ? "stop" : "start"));
    return row;
  });
  const serviceRows = services.map(service => {
    const row = element("div", null, "item");
    const host = nodes.find(node => node.id === service.nodeId);
    const name = element("div", serviceTitle(service.id), "item-name");
    const failed = service.status === "Failed";
    name.append(element("small", host?.name || service.nodeId));
    row.append(name, pill(service.status), actionButton(failed ? "Restart" : service.enabled ? "Pause" : "Resume",
      "services", service.id, service.enabled && !failed ? "stop" : "start", !host || !nodeActive(host)));
    return row;
  });
  byId("node-list").replaceChildren(...nodeRows);
  byId("service-list").replaceChildren(...serviceRows);
  if (focusLabel) {
    [...document.querySelectorAll(".item button")].find(button => button.getAttribute("aria-label") === focusLabel)?.focus();
  }

  if (byId("employer").options.length !== employers.length) {
    const selected = byId("employer").value;
    byId("employer").replaceChildren(...employers.map(employer => {
      const option = element("option", employer.name);
      option.value = employer.id;
      return option;
    }));
    if (selected) byId("employer").value = selected;
  }
}

function renderJobs() {
  byId("pending").textContent = state.jobsOnline
    ? state.jobs.filter(job => job.status === "Pending").length : "—";
  const target = byId("job-list");
  const rows = state.jobs.filter(job => state.filter === "All" || state.filter === job.status);
  if (!rows.length) {
    const empty = element("div", null, "empty");
    empty.append(element("strong", state.jobsOnline
      ? (state.filter === "All" ? "No postings yet" : "No " + state.filter.toLowerCase() + " postings")
      : "Postings unavailable"));
    if (state.jobsOnline && state.filter === "All") {
      empty.append(element("span", "Submit a fictional posting to see it move from pending to a decision."));
    }
    target.replaceChildren(empty);
    return;
  }
  const scroll = element("div", null, "table-scroll");
  const table = element("table");
  table.setAttribute("aria-label", "Job postings");
  const header = table.createTHead().insertRow();
  ["Posting", "Positions", "Status", "Submitted"].forEach(label => {
    const cell = element("th", label);
    cell.scope = "col";
    header.append(cell);
  });
  const body = table.createTBody();
  rows.forEach(job => {
    const row = body.insertRow();
    const posting = row.insertCell();
    posting.append(element("strong", job.title), element("small", job.employer));
    row.insertCell().textContent = job.positions;
    const status = row.insertCell();
    status.append(pill(job.status));
    if (job.reason) status.append(element("small", job.reason));
    row.insertCell().textContent = new Date(job.submittedAt).toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
  });
  scroll.append(table);
  const previousScroll = target.firstElementChild?.scrollLeft || 0;
  target.replaceChildren(scroll);
  scroll.scrollLeft = previousScroll;
}

async function refresh() {
  if (refreshing || state.busy) return;
  refreshing = true;
  try {
    const [cluster, jobs] = await Promise.allSettled([request("/api/snapshot"), request("/api/jobs")]);
    if (state.busy) return;
    const nextSnapshot = cluster.status === "fulfilled" ? cluster.value : state.snapshot;
    const nextJobs = jobs.status === "fulfilled" ? jobs.value.jobs : state.jobs;
    const connected = cluster.status === "fulfilled";
    const jobsOnline = jobs.status === "fulfilled";
    const clusterChanged = connected !== state.connected || jobsOnline !== state.jobsOnline ||
      JSON.stringify(nextSnapshot) !== JSON.stringify(state.snapshot);
    const jobsChanged = jobsOnline !== state.jobsOnline || JSON.stringify(nextJobs) !== JSON.stringify(state.jobs);
    Object.assign(state, { snapshot: nextSnapshot, jobs: nextJobs, connected, jobsOnline });
    if (clusterChanged) renderSnapshot();
    if (jobsChanged || !jobsOnline) renderJobs();
    notice("job-notice", jobsOnline ? "" : "The workflow service is unavailable. Start its node or resume the service to continue.");
  } finally {
    refreshing = false;
  }
}

async function change(target, id, operation) {
  if (state.busy) return;
  state.busy = id;
  notice("notice", "");
  renderSnapshot();
  try {
    state.snapshot = await request("/api/" + target + "/" + id + "/" + operation, { method: "POST" });
  } catch (error) {
    notice("notice", error.message);
  } finally {
    state.busy = null;
    renderSnapshot();
    refresh();
  }
}

byId("open-posting").addEventListener("click", () => {
  notice("form-error", "");
  dialog.showModal();
  byId("job-title").focus();
});
["close-posting", "cancel-posting"].forEach(id => byId(id).addEventListener("click", () => dialog.close()));

form.addEventListener("submit", async event => {
  event.preventDefault();
  const submit = byId("submit-posting");
  if (submit.disabled) return;
  notice("form-error", "");
  const payload = {
    employerId: byId("employer").value,
    title: byId("job-title").value.trim(),
    positions: Number(byId("positions").value)
  };
  const fingerprint = JSON.stringify(payload);
  if (draft?.fingerprint !== fingerprint) draft = { fingerprint, id: crypto.randomUUID() };
  submit.disabled = true;
  try {
    await request("/api/jobs", { method: "POST", body: JSON.stringify({ ...payload, id: draft.id }) });
    draft = null;
    form.reset();
    dialog.close();
    refresh();
  } catch (error) {
    notice("form-error", error.message);
  } finally {
    submit.disabled = false;
  }
});

document.querySelectorAll("[data-filter]").forEach(button => button.addEventListener("click", () => {
  state.filter = button.dataset.filter;
  document.querySelectorAll("[data-filter]").forEach(candidate => {
    candidate.setAttribute("aria-pressed", String(candidate === button));
  });
  renderJobs();
}));
function updateNavigation() {
  const selected = location.hash || "#overview";
  document.querySelectorAll("nav a").forEach(link => link.classList.toggle("active", link.hash === selected));
}
window.addEventListener("hashchange", updateNavigation);
updateNavigation();
refresh();
window.setInterval(refresh, 1800);
