const elements = {
  queued: document.querySelector("#queued"),
  running: document.querySelector("#running"),
  completed: document.querySelector("#completed"),
  failed: document.querySelector("#failed"),
  cancelled: document.querySelector("#cancelled"),
  healthyWorkers: document.querySelector("#healthy-workers"),
  metrics: document.querySelector("#metrics-cards"),
  jobs: document.querySelector("#jobs"),
  workers: document.querySelector("#workers"),
  lastUpdated: document.querySelector("#last-updated"),
  historyModal: document.querySelector("#history-modal"),
  historyTitle: document.querySelector("#history-title"),
  historyCommand: document.querySelector("#history-command"),
  historyResult: document.querySelector("#history-result"),
  history: document.querySelector("#history"),
};

async function fetchJSON(path, options) {
  const response = await fetch(path, options);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

function formatTime(value) {
  if (!value) {
    return "—";
  }
  return new Date(value).toLocaleString();
}

function cell(value) {
  const element = document.createElement("td");
  element.textContent = value;
  return element;
}

function statusBadge(status) {
  const badge = document.createElement("span");
  badge.className = `status-badge status-${status}`;
  badge.textContent = status;
  return badge;
}

function statusCell(status) {
  const element = document.createElement("td");
  element.append(statusBadge(status));
  return element;
}

function renderJobs(jobs) {
  elements.jobs.replaceChildren();
  for (const job of jobs) {
    const row = document.createElement("tr");
    row.append(
      cell(job.id),
      commandCell(job.payload),
      statusCell(job.status),
      cell(String(job.priority)),
      cell(String(job.attempts)),
      cell(job.worker_id || "—"),
    );

    const actions = document.createElement("td");
    const historyButton = document.createElement("button");
    historyButton.className = "button secondary";
    historyButton.textContent = "View history";
    historyButton.addEventListener("click", () => showHistory(job.id));
    actions.append(historyButton);

    if (job.status === "queued" || job.status === "running") {
      const cancelButton = document.createElement("button");
      cancelButton.className = "button danger";
      cancelButton.textContent = "Cancel";
      cancelButton.addEventListener("click", () => cancelJob(job.id));
      actions.append(cancelButton);
    }
    row.append(actions);
    elements.jobs.append(row);
  }
}

function renderWorkers(workers) {
  elements.workers.replaceChildren();
  for (const worker of workers) {
    const row = document.createElement("tr");
    row.append(
      cell(worker.id),
      statusCell(worker.status),
      cell(formatTime(worker.last_seen)),
    );
    elements.workers.append(row);
  }
}

function commandCell(command) {
  const element = document.createElement("td");
  element.className = "command-cell";
  element.title = command;
  element.textContent = command;
  return element;
}

function renderMetrics(values) {
  elements.metrics.replaceChildren();
  Object.entries(values)
    .filter(([name]) => name.includes("total") || name.includes("depth") || name.includes("active"))
    .sort(([left], [right]) => left.localeCompare(right))
    .slice(0, 8)
    .forEach(([name, value]) => {
      const card = document.createElement("div");
      card.className = "metric";
      const label = document.createElement("span");
      label.textContent = name;
      const number = document.createElement("strong");
      number.textContent = String(value);
      card.append(label, number);
      elements.metrics.append(card);
    });
}

async function refresh() {
  try {
    const [stats, jobs, workers] = await Promise.all([
      fetchJSON("/v1/stats"),
      fetchJSON("/v1/jobs"),
      fetchJSON("/v1/workers"),
    ]);
    elements.queued.textContent = stats.queue.queued;
    elements.running.textContent = stats.queue.running;
    elements.completed.textContent = stats.queue.completed;
    elements.failed.textContent = stats.queue.failed;
    elements.cancelled.textContent = stats.queue.cancelled;
    elements.healthyWorkers.textContent = stats.workers.healthy;
    renderMetrics(stats.metrics);
    renderJobs(jobs.jobs);
    renderWorkers(workers.workers);
    elements.lastUpdated.textContent = `updated ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    elements.lastUpdated.textContent = `error: ${error.message}`;
  }
}

async function cancelJob(id) {
  try {
    await fetchJSON(`/v1/jobs/${encodeURIComponent(id)}/cancel`, { method: "POST" });
    await refresh();
  } catch (error) {
    elements.lastUpdated.textContent = `error: ${error.message}`;
  }
}

async function showHistory(id) {
  elements.historyTitle.textContent = `Job history: ${id}`;
  elements.historyCommand.textContent = "Loading…";
  elements.historyResult.textContent = "Loading…";
  elements.history.replaceChildren();
  const loading = document.createElement("p");
  loading.className = "muted";
  loading.textContent = "Loading history…";
  elements.history.append(loading);
  elements.historyModal.showModal();
  try {
    const [job, history] = await Promise.all([
      fetchJSON(`/v1/jobs/${encodeURIComponent(id)}`),
      fetchJSON(`/v1/jobs/${encodeURIComponent(id)}/history`),
    ]);
    elements.historyCommand.textContent = job.payload || "—";
    elements.historyResult.textContent = job.error || job.result || "—";
    renderHistory(history.events);
  } catch (error) {
    elements.historyCommand.textContent = "Unavailable";
    elements.historyResult.textContent = "Unavailable";
    elements.history.replaceChildren();
    const message = document.createElement("p");
    message.className = "error-message";
    message.textContent = `Unable to load history: ${error.message}`;
    elements.history.append(message);
    elements.lastUpdated.textContent = `error: ${error.message}`;
  }
}

function renderHistory(events) {
  elements.history.replaceChildren();
  if (events.length === 0) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = "No history recorded";
    elements.history.append(empty);
    return;
  }

  const timeline = document.createElement("ol");
  timeline.className = "timeline-list";
  for (const event of events) {
    const item = document.createElement("li");
    const heading = document.createElement("div");
    heading.className = "timeline-heading";
    const eventType = document.createElement("strong");
    eventType.className = "timeline-type";
    eventType.textContent = event.type;
    heading.append(statusBadge(event.status), eventType);
    const time = document.createElement("time");
    time.dateTime = event.at;
    time.textContent = formatTime(event.at);
    heading.append(time);
    const message = document.createElement("p");
    message.textContent = event.message || "No additional details";
    item.append(heading, message);
    if (event.worker_id) {
      const worker = document.createElement("span");
      worker.className = "timeline-worker";
      worker.textContent = `Worker: ${event.worker_id}`;
      item.append(worker);
    }
    timeline.append(item);
  }
  elements.history.append(timeline);
}

document.querySelector("#refresh").addEventListener("click", refresh);
document.querySelector("#close-history").addEventListener("click", () => {
  elements.historyModal.close();
});
elements.historyModal.addEventListener("click", (event) => {
  if (event.target === elements.historyModal) {
    elements.historyModal.close();
  }
});
refresh();
setInterval(refresh, 2000);
