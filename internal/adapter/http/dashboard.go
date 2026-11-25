package httpadapter

import (
	"net/http"
)

const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MiniKafka</title>
  <style>
    :root { color-scheme: light dark; font: 16px/1.5 system-ui, sans-serif; }
    body { max-width: 960px; margin: 2rem auto; padding: 0 1rem; }
    h1 { margin-bottom: .25rem; }
    section { margin: 2rem 0; }
    table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
    th, td { padding: .65rem; border-bottom: 1px solid #8886; text-align: left; }
    input, button { font: inherit; padding: .4rem .6rem; }
    #status { min-height: 1.5rem; }
  </style>
</head>
<body>
  <h1>MiniKafka</h1>
  <p>Partition offsets and consumer group lag</p>
  <section>
    <h2>Topics and partitions</h2>
    <button id="refresh-topics">Refresh topics</button>
    <table><thead><tr><th>Topic</th><th>Partition</th><th>Latest offset</th></tr></thead><tbody id="topics"></tbody></table>
  </section>
  <section>
    <h2>Consumer lag</h2>
    <label for="group">Consumer group</label>
    <input id="group" autocomplete="off" placeholder="orders-workers">
    <button id="refresh-lag">Load lag</button>
    <table><thead><tr><th>Topic</th><th>Partition</th><th>Committed</th><th>Latest</th><th>Lag</th></tr></thead><tbody id="lag"></tbody></table>
  </section>
  <p id="status" role="status"></p>
  <script>
    const status = document.querySelector('#status');
    async function getJSON(path) {
      const response = await fetch(path);
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || response.statusText);
      return payload;
    }
    function renderRows(target, rows, values) {
      const body = document.querySelector(target);
      body.replaceChildren();
      for (const row of rows) {
        const tr = document.createElement('tr');
        for (const value of values(row)) {
          const cell = document.createElement('td');
          cell.textContent = value;
          tr.append(cell);
        }
        body.append(tr);
      }
    }
    async function loadTopics() {
      try {
        const rows = await getJSON('/topics');
        renderRows('#topics', rows, row => [row.topic, row.partition, row.latestOffset]);
        status.textContent = '';
      } catch (error) { status.textContent = error.message; }
    }
    async function loadLag() {
      const group = document.querySelector('#group').value.trim();
      if (!group) { status.textContent = 'Enter a consumer group.'; return; }
      try {
        const rows = await getJSON('/lag?group=' + encodeURIComponent(group));
        renderRows('#lag', rows, row => [row.topic, row.partition, row.committedOffset, row.latestOffset, row.lag]);
        status.textContent = '';
      } catch (error) { status.textContent = error.message; }
    }
    document.querySelector('#refresh-topics').addEventListener('click', loadTopics);
    document.querySelector('#refresh-lag').addEventListener('click', loadLag);
    loadTopics();
  </script>
</body>
</html>`

// DashboardHandler serves the topic and consumer lag dashboard.
type DashboardHandler struct{}

// ServeHTTP renders the self-contained dashboard page.
func (DashboardHandler) ServeHTTP(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = response.Write([]byte(dashboardHTML))
}
