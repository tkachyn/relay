const elements = {
  queued: document.querySelector("#queued"),
  running: document.querySelector("#running"),
  completed: document.querySelector("#completed"),
  failed: document.querySelector("#failed"),
  healthyWorkers: document.querySelector("#healthy-workers"),
  metrics: document.querySelector("#metrics-cards"),
  jobs: document.querySelector("#jobs"),
  workers: document.querySelector("#workers"),
  lastUpdated: document.querySelector("#last-updated"),
  historyPanel: document.querySelector("#history-panel"),
  historyTitle: document.querySelector("#history-title"),
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

function renderJobs(jobs) {
  elements.jobs.replaceChildren();
  for (const job of jobs) {
    const row = document.createElement("tr");
    row.append(
      cell(job.id),
      cell(job.type),
      cell(job.status),
      cell(String(job.priority)),
      cell(String(job.attempts)),
      cell(job.worker_id || "—"),
    );

    const actions = document.createElement("td");
    const historyButton = document.createElement("button");
    historyButton.className = "button link";
    historyButton.textContent = "History";
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
      cell(worker.status),
      cell(formatTime(worker.registered_at)),
      cell(formatTime(worker.last_seen)),
    );
    elements.workers.append(row);
  }
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
  try {
    const history = await fetchJSON(`/v1/jobs/${encodeURIComponent(id)}/history`);
    elements.historyTitle.textContent = `Job history: ${id}`;
    elements.history.textContent = JSON.stringify(history.events, null, 2);
    elements.historyPanel.classList.remove("hidden");
  } catch (error) {
    elements.lastUpdated.textContent = `error: ${error.message}`;
  }
}

document.querySelector("#refresh").addEventListener("click", refresh);
document.querySelector("#close-history").addEventListener("click", () => {
  elements.historyPanel.classList.add("hidden");
});
refresh();
setInterval(refresh, 2000);
