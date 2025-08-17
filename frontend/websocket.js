const ws   = new WebSocket(import.meta.env.VITE_SOCKET_URL);
const rows = new Map();   // ticker → <tr>
const books= new Map();   // eventKey → <table class=book>
ws.onopen  = () => console.log("📡 connected");
ws.onclose = () => console.log("❌ closed");  

document.title = import.meta.env.VITE_PAGE_TITLE
ws.onmessage = (evt) => {
  const msg = JSON.parse(evt.data);
  /* ----  NEW: type check  ---- */
  if (msg.type === "orderbook") {
    const { ticker, yes, no } = msg.data;
    const { site, date, strike } = parseTicker(ticker);
    if (!strike) return;

    const siteKey = site;

    /* ---------- rest of your existing code ---------- */
    let siteRow = [...master.tBodies[0].rows]
                  .find(r => r.dataset.siteKey === siteKey);
    if (!siteRow) {
      siteRow = master.tBodies[0].insertRow();
      siteRow.dataset.siteKey = siteKey;
    }

    let dateCell = [...siteRow.cells]
                   .find(c => c.dataset.date === date);
    if (!dateCell) {
      dateCell = document.createElement('td');
      dateCell.dataset.date = date;
      dateCell.dataset.site = siteKey;

      const allCells = [...siteRow.cells];
      let insertBefore = null;
      for (const c of allCells) {
        if (new Date(date) > new Date(c.dataset.date)) {
          insertBefore = c;
          break;
        }
      }
      siteRow.insertBefore(dateCell, insertBefore);

      dateCell.innerHTML = `<div class="siteDayLabel">${site} ${date}</div>`;
      const book = document.createElement('table');
      book.className = 'book';
      dateCell.appendChild(book);
      books.set(`${site}-${date}`, book);
    }

    const book = books.get(`${site}-${date}`);
    const tbody = book.tBodies[0] || book.appendChild(document.createElement('tbody'));
    let tr = [...tbody.rows].find(r => r.dataset.ticker === ticker);
    if (tr) {
      tr.children[1].textContent = yes;
      tr.children[2].textContent = no;
    } else {
      tr = tbody.insertRow();
      tr.dataset.ticker = ticker;
      tr.innerHTML = `
        <td>${strike}</td>
        <td>${yes}</td>
        <td>${no}</td>`;
    }

    [...tbody.children]
      .sort((a, b) => {
        const sa = parseFloat(a.children[0].textContent.match(/[TB]([\d.]+)/)[1]);
        const sb = parseFloat(b.children[0].textContent.match(/[TB]([\d.]+)/)[1]);
        return sa - sb;
      })
      .forEach(r => tbody.appendChild(r));

  } else if (msg.type === "SensorPoll") {
  const siteToRows = msg.payload || {};
  [...master.tBodies[0].rows].forEach(row => {
    const siteKey = row.dataset.siteKey;
    const cell    = getOrCreateSensorCol(row, 'sensorCol');

    const rowsForSite = siteToRows[siteKey] || [];
    if (!rowsForSite.length) {
      cell.innerHTML = '';
      return;
    }

    // Build the red-key strings
    const items = rowsForSite
      .slice(0, 50)
      .map(([k, v]) => `<span style="color:red">${k}</span>:${v}`);

    // 3-column grid
    let html = '<table border="1" cellpadding="4" cellspacing="0">';
    const cols = 3;
    for (let i = 0; i < items.length; i += cols) {
      html += '<tr>';
      for (let j = 0; j < cols; j++) {
        html += `<td>${items[i + j] || ''}</td>`;
      }
      html += '</tr>';
    }
    html += '</table>';

    cell.innerHTML = html;
  });
}
else if (msg.type === "ForecastPoll") {
  const siteKey = msg.site;
  const row     = master.tBodies[0].querySelector(`tr[data-site-key="${siteKey}"]`);
  if (!row) return;

  const cell = getOrCreateForecastCol(row);

  const rows = msg.payload || [];
  let inner = '';

  if (rows.length) {
    const cols = 3;
    inner = '<table border="1" cellpadding="2" cellspacing="0" style="font-size:1em;">';

    for (let i = 0; i < rows.length; i += cols) {
      inner += '<tr>';
      for (let j = 0; j < cols; j++) {
        const idx = i + j;
        if (idx < rows.length) {
          const [h, v] = rows[idx];
          inner += `<td><span style="color:red;">${String(h).slice(8,13)}</span>:${v}</td>`;
        } else {
          inner += '<td></td>';
        }
      }
      inner += '</tr>';
    }
    inner += '</table>';
  }
  cell.innerHTML = inner;
}
  else {
    console.log('Non-orderbook message:', msg);
  }
};




function parseTicker(raw) {
  const p = raw.split('-');
  return { site: p[0], date: p[1], strike: p[2] || null };
}

    /* ---------- Countdown to 3 AM EST ---------- */
    const fmt = n => n.toString().padStart(2,'0');

    function updateCountdown() {
      const now = new Date();
      const target = new Date(now);
      target.setHours(3,0,0,0);
      if (now >= target) target.setDate(target.getDate()+1);

      const diff = target - now;
      const h = Math.floor(diff / 36e5);
      const m = Math.floor((diff % 36e5) / 6e4);
      const s = Math.floor((diff % 6e4) / 1e3);

      document.getElementById('countdown').textContent =
        `Time to 3 AM EST: ${h}:${fmt(m)}:${fmt(s)}`;
    }
    updateCountdown();
    setInterval(updateCountdown,1000);

    /* ---------- Portfolio Balance ---------- */
    const balInput = document.getElementById('balance');
    balInput.value = localStorage.getItem('portfolioBalance') || '';
    balInput.addEventListener('input', () =>
      localStorage.setItem('portfolioBalance', balInput.value)
    );

function getOrCreateSensorCol(row) {
  const old = row.querySelector('td.sensorCol');
  if (old) old.remove();

  const dateCells = [...row.cells].filter(c => c.dataset.date);
  const idx = dateCells.length ? dateCells[dateCells.length - 1].cellIndex + 1 : 0;

  const cell = row.insertCell(idx);
  cell.className = 'sensorCol';
  return cell;
}

function getOrCreateForecastCol(row) {
  const old = row.querySelector('td.forecastCol');
  if (old) old.remove();

  const sensorCell = row.querySelector('td.sensorCol');
  const idx = sensorCell ? sensorCell.cellIndex + 1
                         : (row.cells.length);   // fallback if no sensor yet
  const cell = row.insertCell(idx);
  cell.className = 'forecastCol';
  return cell;
}