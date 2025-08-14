const ws   = new WebSocket("ws://0.0.0.0:8000/ws");
const rows = new Map();   // ticker → <tr>
const books= new Map();   // eventKey → <table class=book>

ws.onopen  = () => console.log("📡 connected");
ws.onclose = () => console.log("❌ closed");  

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
        <td>${ticker}</td>
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
    console.log(msg.payload);

    const masterTbody = master.tBodies[0];
    const siteToRows = msg.payload || {};

    [...masterTbody.rows].forEach(row => {
      const siteKey = row.dataset.siteKey;

      /* 1. destroy any existing sensor cell, wherever it might be */
      const oldCell = row.querySelector('td.sensorCol');
      if (oldCell) oldCell.remove();

      /* 2. create a brand-new cell and append it as the last column */
      const sensorCell = row.insertCell();
      sensorCell.className = 'sensorCol';

      /* 3. populate it with the 5×2 table */
      let innerHtml = '';
      const rowsForSite = siteToRows[siteKey] || [];
      if (rowsForSite.length) {
        innerHtml = '<table border="1" cellpadding="4" cellspacing="0">';
        rowsForSite.slice(0, 5).forEach(([k, v]) => {
          innerHtml += `<tr><td>${k}</td><td>${v}</td></tr>`;
        });
        innerHtml += '</table>';
      }
      sensorCell.innerHTML = innerHtml;
  });
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