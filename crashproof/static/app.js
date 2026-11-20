const byId = id => document.getElementById(id);
let key, lastRequest, selected, lastSnapshot, renderedOrder;
let refreshing = false;
let submitting = false;
const money = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });
const time = new Intl.DateTimeFormat(undefined, { timeStyle: 'medium' });
const pretty = value => value.replaceAll('_', ' ');

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function newKey() {
  key = 'web-' + crypto.randomUUID();
  byId('request-key').textContent = key;
}

async function request(path, options) {
  const response = await fetch(path, { cache: 'no-store', ...options, signal: AbortSignal.timeout(12000) });
  const data = await response.json();
  if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : 'Request failed (' + response.status + ')');
  return data;
}

function message(value, error = false) {
  const notice = byId('form-message');
  notice.textContent = value;
  notice.classList.toggle('error', error);
}

async function submit(payload, requestKey) {
  if (submitting) return;
  submitting = true;
  byId('submit').disabled = byId('replay').disabled = true;
  try {
    const data = await request('/api/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': requestKey },
      body: JSON.stringify(payload)
    });
    lastRequest = { payload, requestKey };
    selected = data.order.id;
    renderDetail(data.order);
    message((data.created ? 'Order saved. ' : 'Identical request deduplicated. ') + data.dispatch);
    await refresh();
  } catch (error) {
    message(error.message, true);
  } finally {
    submitting = false;
    byId('submit').disabled = false;
    byId('replay').disabled = !lastRequest;
  }
}

async function health() {
  try {
    const data = await request('/api/health');
    const ready = data.temporal === 'connected';
    byId('health-label').textContent = ready ? 'Temporal server connected' : 'Temporal unavailable';
    byId('health-dot').className = 'dot ' + (ready ? 'good' : 'bad');
    byId('queue-label').textContent = ready ? 'Worker runs separately' : 'Start the local Temporal server';
  } catch {
    byId('health-label').textContent = 'API unavailable';
    byId('health-dot').className = 'dot bad';
  }
}

function selectOrder(id) {
  selected = id;
  for (const row of byId('orders-body').rows) {
    row.classList.toggle('selected', row.dataset.order === id);
    row.querySelector('button').setAttribute('aria-pressed', String(row.dataset.order === id));
  }
  loadDetail();
}

function renderOrders(data) {
  const tiles = { orders: 'orders', completed: 'completed', dedup: 'deduplicated_charges', refunds: 'refunds' };
  for (const [id, metric] of Object.entries(tiles)) {
    byId('metric-' + id).textContent = data.metrics[metric].toLocaleString();
  }
  const previousFocus = document.activeElement?.dataset?.order;
  const rows = data.orders.map(order => {
    const row = element('tr');
    row.dataset.order = order.id;
    row.classList.toggle('selected', selected === order.id);
    const title = element('td');
    title.append(element('strong', order.id.slice(0, 14)), element('small', order.customer));
    const amount = element('td', money.format(order.total_cents / 100));
    const status = element('td');
    status.append(element('span', pretty(order.status), 'status ' + order.status));
    const action = element('td');
    const button = element('button', '↗');
    button.dataset.order = order.id;
    button.setAttribute('aria-label', 'Inspect order ' + order.id);
    button.setAttribute('aria-pressed', String(selected === order.id));
    button.addEventListener('click', () => selectOrder(order.id));
    action.append(button);
    row.append(title, amount, status, action);
    return row;
  });
  byId('orders-body').replaceChildren(...rows);
  byId('orders-empty').hidden = data.orders.length > 0;
  if (previousFocus) rows.find(row => row.dataset.order === previousFocus)?.querySelector('button').focus({ preventScroll: true });
  byId('inventory').replaceChildren(...data.inventory.map(item => {
    const stock = element('div');
    stock.append(element('strong', item.available.toLocaleString() + ' '), document.createTextNode(item.sku));
    return stock;
  }));
}

function renderRoute(order) {
  const events = new Set(order.events.map(event => event.kind));
  const entered = new Set(order.events.filter(event => event.kind === 'state_changed').map(event => event.detail.to));
  const visited = {
    request: true,
    reserve: events.has('inventory_reserved') || entered.has('reserving'),
    charge: events.has('payment_attempt') || entered.has('charging'),
    fulfill: events.has('order_fulfilled') || entered.has('fulfilling'),
    compensate: entered.has('compensating')
  };
  const stages = {
    accepted: 'request', queued: 'request', reserving: 'reserve', awaiting_approval: 'reserve',
    charging: 'charge', fulfilling: 'fulfill', completed: 'fulfill',
    compensating: 'compensate', compensated: 'compensate', failed: 'compensate', manual_review: 'compensate'
  };
  const active = stages[order.status];
  for (const station of document.querySelectorAll('[data-stage]')) {
    const stage = station.dataset.stage;
    station.dataset.state = stage === active ? 'current' : visited[stage] ? 'visited' : 'waiting';
  }
  const path = Object.keys(visited).filter(stage => visited[stage]).map(stage => ({
    request: 'Accepted', reserve: 'Reservation', charge: 'Payment', fulfill: 'Fulfillment', compensate: 'Compensation'
  })[stage]);
  byId('route-mode').textContent = order.id.slice(0, 14);
  byId('route-status').textContent = path.join(' → ') + ' · ' + pretty(order.status);
}

function renderDetail(order) {
  // Avoid replacing the evidence panel when its data has not changed.
  const next = JSON.stringify(order);
  if (next === renderedOrder) return;
  renderedOrder = next;
  renderRoute(order);
  byId('detail-panel').hidden = false;
  byId('detail-title').textContent = 'Workflow ' + order.id.slice(0, 14);
  byId('timeline').replaceChildren(...order.events.map(event => {
    const item = element('div', undefined, 'event');
    item.append(element('strong', pretty(event.kind)), element('small', time.format(new Date(event.at))),
      element('pre', JSON.stringify(event.detail, null, 2)));
    return item;
  }));
  const pairs = [
    ['Order state', pretty(order.status)],
    ['Amount', money.format(order.total_cents / 100)],
    ['Payment', order.payment?.state || 'No charge'],
    ['Reservation', order.reservation?.state || 'None'],
    ['Scenario', pretty(order.fault)]
  ];
  byId('ledger').replaceChildren(...pairs.flatMap(([label, value]) => [element('dt', label), element('dd', value)]));
  byId('approve').hidden = byId('reject').hidden = order.status !== 'awaiting_approval';
  byId('crash-note').hidden = !(order.fault === 'worker_crash' && order.payment && order.status === 'charging');
}

async function loadDetail() {
  const orderId = selected;
  if (!orderId) return;
  try {
    const order = await request('/api/orders/' + orderId);
    if (selected === orderId) renderDetail(order);
  } catch (error) {
    if (selected === orderId) message(error.message, true);
  }
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try {
    const data = await request('/api/dashboard');
    const snapshot = JSON.stringify(data);
    if (snapshot !== lastSnapshot) {
      renderOrders(data);
      lastSnapshot = snapshot;
    }
    await loadDetail();
  } catch (error) {
    message(error.message, true);
  } finally {
    refreshing = false;
  }
}

async function approve(approved) {
  const orderId = selected;
  if (!orderId) return;
  try {
    await request('/api/orders/' + orderId + '/approval', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ approved })
    });
    await refresh();
  } catch (error) {
    message(error.message, true);
  }
}

async function download(endpoint, label) {
  const orderId = selected;
  if (!orderId) return;
  try {
    const data = await request('/api/orders/' + orderId + '/' + endpoint);
    const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], {type: 'application/json'}));
    const link = element('a');
    link.href = url;
    link.download = orderId + '-' + label + '.json';
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (error) {
    message(error.message, true);
  }
}

byId('order-form').addEventListener('submit', event => {
  event.preventDefault();
  submit({
    customer: byId('customer').value,
    sku: byId('sku').value,
    quantity: Number(byId('quantity').value),
    fault: byId('fault').value,
    requires_approval: byId('approval').checked
  }, key);
});
byId('new-key').addEventListener('click', newKey);
byId('replay').addEventListener('click', () => {
  if (lastRequest) submit(lastRequest.payload, lastRequest.requestKey);
});
byId('approve').addEventListener('click', () => approve(true));
byId('reject').addEventListener('click', () => approve(false));
byId('history').addEventListener('click', () => download('history', 'temporal-history'));
byId('spans').addEventListener('click', () => download('spans', 'otel-spans'));
for (const button of document.querySelectorAll('[data-scenario]')) {
  button.addEventListener('click', () => {
    byId('fault').value = button.dataset.scenario;
    newKey();
    byId('fault').focus();
  });
}
newKey();
health();
refresh();
setInterval(() => { if (!document.hidden) refresh(); }, 2000);
setInterval(() => { if (!document.hidden) health(); }, 15000);
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
