"use strict";

/*
 * Web client for the Food Delivery Platform.
 *
 * nginx serves this page and forwards each API path (/customers, /orders and so
 * on) to the matching service, so every request below is a plain path on the
 * same address. The page never talks to Kafka directly. It calls the services'
 * REST APIs and polls the Admin Service, whose view of every order is built
 * from the Kafka events.
 */

// ---------- Fixed data ----------

const SERVICES = [
  { name: "Customer", health: "/customers/health" },
  { name: "Restaurant", health: "/restaurant/health" },
  { name: "Order", health: "/orders/health" },
  { name: "Payment", health: "/payments/health" },
  { name: "Delivery", health: "/delivery/health" },
  { name: "Notification", health: "/notifications/health" },
  { name: "Admin", health: "/admin/health" }
];

// The stations of the order rail, with the request or Kafka event that moves an order into each one
const STATIONS = [
  { status: "CREATED", label: "Created", cause: "POST /orders" },
  { status: "CONFIRMED", label: "Confirmed", cause: "payments.completed" },
  { status: "PREPARING", label: "Preparing", cause: "restaurant.order.accepted" },
  { status: "READY", label: "Ready", cause: "restaurant.order.ready" },
  { status: "OUT_FOR_DELIVERY", label: "Out for delivery", cause: "delivery.assigned" },
  { status: "DELIVERED", label: "Delivered", cause: "delivery.completed" },
  { status: "CANCELLED", label: "Cancelled", cause: "orders.cancelled" }
];

const TABS = ["customer", "restaurant", "driver", "admin"];
const CARD_LIMIT = 500;          // the Payment Service declines anything above this
const TICKETS_PER_STATION = 3;
const POLL_MS = 2000;
const HEALTH_MS = 5000;

// ---------- State ----------

const state = {
  tab: TABS.includes(recall("tab")) ? recall("tab") : "customer",
  customerId: recall("customerId"),
  restaurantId: recall("restaurantId") || "rest-001",
  customer: null,            // the signed-in customer, from the Customer Service
  restaurant: null,          // the chosen restaurant and its menu, from the Restaurant Service
  restaurantMissing: false,  // true when the Restaurant Service says the ID does not exist
  cart: {},                  // dish name -> quantity
  orders: [],                // every order as the Admin Service sees it
  adminDown: false
};

// The items of an order never change, so they are fetched from the Order Service once
const itemsCache = new Map();

// ---------- Small helpers ----------

function recall(key) {
  try {
    return localStorage.getItem("fdp." + key);
  } catch {
    return null;
  }
}

function remember(key, value) {
  try {
    if (value == null) localStorage.removeItem("fdp." + key);
    else localStorage.setItem("fdp." + key, value);
  } catch {
    // Storage is blocked: the page still works, it just forgets on reload
  }
}

const $ = (selector) => document.querySelector(selector);
const enc = encodeURIComponent;

function esc(value) {
  const entities = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
  return String(value ?? "").replace(/[&<>"']/g, (c) => entities[c]);
}

const money = (amount) => "N$" + Number(amount || 0).toFixed(2);
const ticketNo = (orderId) => "#" + String(orderId).slice(-6);
const statusLabel = (status) => (STATIONS.find((s) => s.status === status) || { label: status }).label;
const itemSummary = (items) => (items || []).map((i) => `${i.quantity} ${i.itemName}`).join(", ");

function duration(seconds) {
  const s = Math.round(Number(seconds) || 0);
  return s >= 60 ? `${Math.floor(s / 60)} min ${s % 60} s` : `${s} s`;
}

function statusPill(status) {
  return `<span class="pill" data-status="${esc(status)}">${esc(statusLabel(status))}</span>`;
}

// Replaces an element's content only when it changed, so the page does not
// rebuild buttons under the mouse every time it polls
const painted = new WeakMap();
function paint(element, html) {
  if (painted.get(element) === html) return false;
  element.innerHTML = html;
  painted.set(element, html);
  return true;
}

// ---------- Talking to the services ----------

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

async function api(method, path, body) {
  const options = { method };
  if (body !== undefined) {
    options.headers = { "Content-Type": "application/json" };
    options.body = JSON.stringify(body);
  }

  let response;
  try {
    response = await fetch(path, options);
  } catch {
    throw new ApiError(0, "The page can't reach the server. Check that the web-client container is running.");
  }

  const text = await response.text();
  let data = text;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    // Plain-text replies, such as "Drivers seeded", stay as text
  }

  if (!response.ok) {
    let message = `The request failed with status ${response.status}.`;
    if (data && typeof data === "object" && data.message) {
      message = data.message;
    } else if (response.status === 502 || response.status === 504) {
      message = "That service isn't running. Check it with docker compose ps.";
    }
    throw new ApiError(response.status, message);
  }
  return data;
}

let toastTimer;
function notify(message, kind = "ok") {
  const toast = $("#toast");
  toast.textContent = message;
  toast.dataset.kind = kind;
  toast.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { toast.hidden = true; }, 5000);
}

// Runs a button's action, keeping the button disabled until the action finishes
async function run(button, action) {
  if (button) button.disabled = true;
  try {
    await action();
  } catch (error) {
    notify(error.message, "error");
  } finally {
    if (button) button.disabled = false;
  }
}

// ---------- Service health ----------

async function checkHealth() {
  const up = await Promise.all(SERVICES.map((s) => api("GET", s.health).then(() => true, () => false)));
  paint($("#health"), SERVICES.map((s, i) => `
    <li class="${up[i] ? "up" : "down"}">${esc(s.name)}<span class="sr">${up[i] ? " is running" : " is not responding"}</span></li>`).join(""));
}

// ---------- The order rail ----------

async function loadOrders() {
  try {
    state.orders = await api("GET", "/admin/orders");
    state.adminDown = false;
  } catch {
    state.adminDown = true;
  }
}

function renderRail() {
  const rail = $("#rail");
  if (state.adminDown) {
    paint(rail, `<p class="rail-note">The order rail reads from the Admin Service, which isn't responding.</p>`);
    return;
  }

  // Remember where every ticket is before redrawing, so moved tickets can glide to their new station
  const before = new Map();
  for (const el of rail.querySelectorAll(".ticket")) before.set(el.dataset.id, el.getBoundingClientRect());

  const html = STATIONS.map((station) => {
    const here = state.orders.filter((o) => o.status === station.status).reverse();
    const hidden = here.length - TICKETS_PER_STATION;
    const firstHint = station.status === "CREATED" && state.orders.length === 0
      ? `<li class="rail-empty">Orders appear here the moment they're placed.</li>` : "";
    return `
      <section class="station" data-status="${station.status}">
        <h2>${station.label} <span class="count">${here.length}</span></h2>
        <p class="cause">${esc(station.cause)}</p>
        <ol>${firstHint}${here.slice(0, TICKETS_PER_STATION).map(ticketHtml).join("")}</ol>
        ${hidden > 0 ? `<p class="more">${hidden} more</p>` : ""}
      </section>`;
  }).join("");

  if (paint(rail, html)) animateMoves(rail, before);
}

function ticketHtml(order) {
  const where = [order.restaurantId, order.driverId].filter(Boolean).map(esc).join(", ");
  return `
    <li class="ticket" data-id="${esc(order.orderId)}" title="Order ${esc(order.orderId)}">
      <span class="ticket-no">${ticketNo(order.orderId)}</span>
      <span class="ticket-amount">${money(order.totalAmount)}</span>
      <span class="ticket-where">${where}</span>
    </li>`;
}

// Slides each ticket from its old station to its new one (the "FLIP" technique)
function animateMoves(rail, before) {
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  for (const el of rail.querySelectorAll(".ticket")) {
    const from = before.get(el.dataset.id);
    const to = el.getBoundingClientRect();
    if (!from) {
      el.animate([{ opacity: 0, transform: "translateY(-8px)" }, { opacity: 1, transform: "none" }],
        { duration: 320, easing: "ease-out" });
    } else if (from.left !== to.left || from.top !== to.top) {
      el.animate([{ transform: `translate(${from.left - to.left}px, ${from.top - to.top}px)` }, { transform: "none" }],
        { duration: 550, easing: "cubic-bezier(0.2, 0.7, 0.2, 1)" });
    }
  }
}

// ---------- Customer ----------

function useCustomer(customer) {
  state.customer = customer;
  state.customerId = customer ? customer.customerId : null;
  remember("customerId", state.customerId);
}

async function loadCustomer() {
  if (!state.customerId) return;
  try {
    state.customer = await api("GET", `/customers/${enc(state.customerId)}`);
  } catch (error) {
    if (error.status === 404) useCustomer(null);
    else notify(`Couldn't load your account: ${error.message}`, "error");
  }
}

function renderAccount() {
  const c = state.customer;
  if (!c) {
    paint($("#account"), `
      <h2>Your account</h2>
      <form class="stack" id="register-form">
        <label>Name <input name="name" required autocomplete="name"></label>
        <label>Email <input name="email" type="email" required autocomplete="email"></label>
        <label>Phone <input name="phone" type="tel" required autocomplete="tel"></label>
        <button type="submit">Create account</button>
      </form>
      <form class="inline" id="signin-form">
        <label>Already have an account? Customer ID <input name="customerId" required autocomplete="off"></label>
        <button type="submit" class="secondary">Use this account</button>
      </form>`);
    return;
  }

  const addresses = c.addresses && c.addresses.length
    ? `<ul class="addresses">${c.addresses.map((a) =>
        `<li><strong>${esc(a.label)}</strong> ${esc(a.street)}, ${esc(a.city)}</li>`).join("")}</ul>`
    : `<p class="muted">No delivery addresses yet.</p>`;

  paint($("#account"), `
    <h2>Your account</h2>
    <p class="who"><strong>${esc(c.name)}</strong><br>${esc(c.email)}<br>${esc(c.phone)}</p>
    <p class="muted small">Customer ID <code>${esc(c.customerId)}</code></p>
    <h3>Delivery addresses</h3>
    ${addresses}
    <form class="stack" id="address-form">
      <div class="pair">
        <label>Label <input name="label" required placeholder="Home"></label>
        <label>City <input name="city" required placeholder="Windhoek"></label>
      </div>
      <label>Street <input name="street" required placeholder="12 Independence Avenue"></label>
      <button type="submit" class="secondary">Add address</button>
    </form>
    <button type="button" class="link" id="switch-account">Use a different account</button>`);
}

$("#account").addEventListener("submit", (event) => {
  event.preventDefault();
  const form = event.target;
  const button = form.querySelector("button[type=submit]");
  const data = Object.fromEntries(new FormData(form));

  if (form.id === "register-form") {
    run(button, async () => {
      const customer = await api("POST", "/customers", {
        name: data.name.trim(), email: data.email.trim(), phone: data.phone.trim()
      });
      useCustomer(customer);
      renderAccount();
      renderMenu();
      refresh();
      notify(`Account created for ${customer.name}`);
    });
  } else if (form.id === "signin-form") {
    run(button, async () => {
      const customer = await api("GET", `/customers/${enc(data.customerId.trim())}`);
      useCustomer(customer);
      renderAccount();
      renderMenu();
      refresh();
      notify(`Now using ${customer.name}'s account`);
    });
  } else if (form.id === "address-form") {
    run(button, async () => {
      await api("POST", `/customers/${enc(state.customerId)}/addresses`, {
        label: data.label.trim(), street: data.street.trim(), city: data.city.trim()
      });
      await loadCustomer();
      renderAccount();
      notify("Address added");
    });
  }
});

$("#account").addEventListener("click", (event) => {
  if (event.target.id !== "switch-account") return;
  useCustomer(null);
  renderAccount();
  renderMenu();
  refresh();
});

// ---------- Restaurant data, shared by the Customer and Restaurant tabs ----------

function useRestaurant(restaurantId) {
  if (restaurantId !== state.restaurantId) state.cart = {};
  state.restaurantId = restaurantId;
  remember("restaurantId", restaurantId);
  $("#restaurant-picker").elements.restaurantId.value = restaurantId;
}

async function loadRestaurant() {
  try {
    state.restaurant = await api("GET", `/restaurant/restaurants/${enc(state.restaurantId)}`);
    state.restaurantMissing = false;
  } catch (error) {
    state.restaurant = null;
    state.restaurantMissing = error.status === 404;
  }
}

async function reloadRestaurant() {
  await loadRestaurant();
  renderMenu();
  renderRestaurantDetails();
  renderMenuTable();
}

function openBadge(r) {
  return r.openNow
    ? `<span class="badge open">Open now, ${esc(r.openingTime)} to ${esc(r.closingTime)}</span>`
    : `<span class="badge closed">Closed, opens at ${esc(r.openingTime)}</span>`;
}

// ---------- Customer: ordering ----------

function renderMenu() {
  const box = $("#menu");
  const r = state.restaurant;
  if (!r) {
    paint(box, state.restaurantMissing
      ? `<p class="muted">There's no restaurant with ID <code>${esc(state.restaurantId)}</code>. Register it on the Restaurant tab.</p>`
      : `<p class="muted">The Restaurant Service isn't answering.</p>`);
    return;
  }

  // Keep keyboard focus on the same stepper button after redrawing
  const focused = document.activeElement;
  const refocus = focused && focused.classList.contains("step")
    ? `[data-dish="${CSS.escape(focused.dataset.dish)}"][data-change="${focused.dataset.change}"]` : null;

  const dishes = r.menu.map((dish) => {
    const qty = state.cart[dish.itemName] || 0;
    return `
      <li class="dish">
        <span class="dish-name">${esc(dish.itemName)}</span>
        <span class="dish-price">${money(dish.price)}</span>
        <span class="dish-stock">${dish.stock > 0 ? `${dish.stock} left` : "Sold out"}</span>
        <span class="stepper">
          <button type="button" class="step" data-dish="${esc(dish.itemName)}" data-change="-1"
            aria-label="One less ${esc(dish.itemName)}"${qty === 0 ? " disabled" : ""}>&minus;</button>
          <output>${qty}</output>
          <button type="button" class="step" data-dish="${esc(dish.itemName)}" data-change="1"
            aria-label="One more ${esc(dish.itemName)}">+</button>
        </span>
      </li>`;
  }).join("");

  const total = r.menu.reduce((sum, d) => sum + (state.cart[d.itemName] || 0) * Number(d.price), 0);
  const count = Object.values(state.cart).reduce((sum, q) => sum + q, 0);

  // Say up front what the services will do with this order
  const hints = [];
  if (!state.customer) hints.push("Create an account or use an existing one to place an order.");
  if (count > 0 && !r.openNow) hints.push(`${r.name} is closed, so the kitchen will reject this order.`);
  if (total > CARD_LIMIT) hints.push(`This is over the N$${CARD_LIMIT} card limit, so the payment will be declined.`);
  if (r.menu.some((d) => (state.cart[d.itemName] || 0) > d.stock)) {
    hints.push("This is more than the kitchen has in stock, so it will reject the order and refund the payment.");
  }

  paint(box, `
    <div class="restaurant-head"><h3>${esc(r.name)}</h3> ${openBadge(r)}</div>
    ${r.menu.length ? `<ul class="dishes">${dishes}</ul>`
      : `<p class="muted">${esc(r.name)} has no dishes yet. Add some on the Restaurant tab.</p>`}
    <div class="order-foot">
      <p class="total">Total <strong>${money(total)}</strong></p>
      <button type="button" id="place-order"${!state.customer || count === 0 ? " disabled" : ""}>Place order</button>
    </div>
    ${hints.map((h) => `<p class="hint">${esc(h)}</p>`).join("")}`);

  if (refocus) {
    const again = box.querySelector(refocus);
    if (again && !again.disabled) again.focus();
  }
}

$("#restaurant-picker").addEventListener("submit", (event) => {
  event.preventDefault();
  const id = event.target.elements.restaurantId.value.trim();
  run(event.submitter, async () => {
    useRestaurant(id);
    await reloadRestaurant();
  });
});

$("#menu").addEventListener("click", (event) => {
  const step = event.target.closest(".step");
  if (step) {
    const dish = step.dataset.dish;
    const next = Math.max(0, (state.cart[dish] || 0) + Number(step.dataset.change));
    if (next === 0) delete state.cart[dish];
    else state.cart[dish] = next;
    renderMenu();
    return;
  }
  if (event.target.id === "place-order") run(event.target, placeOrder);
});

async function placeOrder() {
  const r = state.restaurant;
  // Prices come from the restaurant's own menu
  const items = r.menu
    .filter((d) => state.cart[d.itemName] > 0)
    .map((d) => ({ itemName: d.itemName, quantity: state.cart[d.itemName], price: Number(d.price) }));

  const order = await api("POST", "/orders", {
    customerId: state.customerId, restaurantId: r.restaurantId, items
  });
  state.cart = {};
  renderMenu();
  notify(`Order ${ticketNo(order.orderId)} placed`);
  setTimeout(refresh, 400);
  // The kitchen takes the dishes out of stock once it accepts the order
  setTimeout(async () => { await loadRestaurant(); renderMenu(); }, 2500);
}

async function renderMyOrders() {
  const box = $("#my-orders");
  if (!state.customer) {
    paint(box, `<p class="muted">Your orders appear here once you're using an account.</p>`);
    return;
  }

  let history;
  try {
    history = await api("GET", `/customers/${enc(state.customerId)}/orders`);
  } catch (error) {
    paint(box, `<p class="muted">${esc(error.message)}</p>`);
    return;
  }
  if (!history.length) {
    paint(box, `<p class="muted">No orders yet. Pick some dishes and place your first order.</p>`);
    return;
  }

  // The Customer Service keeps the history; the Order Service owns each order's live status
  const recent = history.slice(-8).reverse();
  const live = await Promise.all(recent.map((h) => api("GET", `/orders/${enc(h.orderId)}`).catch(() => h)));
  paint(box, `
    <table>
      <thead><tr><th scope="col">Order</th><th scope="col">Items</th><th scope="col" class="num">Total</th><th scope="col">Status</th></tr></thead>
      <tbody>${live.map((o) => `
        <tr>
          <td class="ticket-ref" title="${esc(o.orderId)}">${ticketNo(o.orderId)}</td>
          <td>${esc(itemSummary(o.items))}</td>
          <td class="num">${money(o.totalAmount)}</td>
          <td>${statusPill(o.status)}</td>
        </tr>`).join("")}
      </tbody>
    </table>`);
}

// ---------- Restaurant ----------

function renderRestaurantDetails() {
  const r = state.restaurant;
  const picker = `
    <form class="inline" id="restaurant-load">
      <label>Restaurant ID <input name="restaurantId" value="${esc(state.restaurantId)}" required autocomplete="off"></label>
      <button type="submit" class="secondary">Show restaurant</button>
    </form>`;

  if (!r) {
    paint($("#restaurant-details"), `
      <h2>Restaurant</h2>
      ${picker}
      ${state.restaurantMissing ? `
        <p>There's no restaurant with ID <code>${esc(state.restaurantId)}</code> yet. Register it:</p>
        <form class="stack" id="restaurant-register">
          <label>Name <input name="name" required placeholder="Kapana Corner"></label>
          <div class="pair">
            <label>Kitchen opens <input name="openingTime" type="time" required value="08:00"></label>
            <label>Kitchen closes <input name="closingTime" type="time" required value="22:00"></label>
          </div>
          <button type="submit">Register restaurant</button>
        </form>`
        : `<p class="muted">The Restaurant Service isn't answering.</p>`}`);
    return;
  }

  paint($("#restaurant-details"), `
    <h2>Restaurant</h2>
    ${picker}
    <div class="restaurant-head"><h3>${esc(r.name)}</h3> ${openBadge(r)}</div>
    <form class="stack" id="restaurant-hours">
      <div class="pair">
        <label>Kitchen opens <input name="openingTime" type="time" required value="${esc(r.openingTime)}"></label>
        <label>Kitchen closes <input name="closingTime" type="time" required value="${esc(r.closingTime)}"></label>
      </div>
      <button type="submit" class="secondary">Save kitchen hours</button>
    </form>`);
}

$("#restaurant-details").addEventListener("submit", (event) => {
  event.preventDefault();
  const form = event.target;
  const button = form.querySelector("button[type=submit]");
  const data = Object.fromEntries(new FormData(form));

  if (form.id === "restaurant-load") {
    run(button, async () => {
      useRestaurant(data.restaurantId.trim());
      await reloadRestaurant();
      refresh();
    });
  } else if (form.id === "restaurant-register") {
    run(button, async () => {
      await api("POST", "/restaurant/restaurants", {
        restaurantId: state.restaurantId, name: data.name.trim(),
        openingTime: data.openingTime, closingTime: data.closingTime
      });
      await reloadRestaurant();
      notify(`${data.name.trim()} registered`);
    });
  } else if (form.id === "restaurant-hours") {
    run(button, async () => {
      await api("PUT", `/restaurant/restaurants/${enc(state.restaurantId)}/hours`, {
        openingTime: data.openingTime, closingTime: data.closingTime
      });
      await reloadRestaurant();
      notify("Kitchen hours saved");
    });
  }
});

function renderMenuTable() {
  const box = $("#menu-table");
  const r = state.restaurant;
  if (!r) {
    paint(box, `<p class="muted">Show or register a restaurant first.</p>`);
    return;
  }
  paint(box, r.menu.length ? `
    <table>
      <thead><tr><th scope="col">Dish</th><th scope="col" class="num">Price</th><th scope="col" class="num">Stock</th><th scope="col"><span class="sr">Edit</span></th></tr></thead>
      <tbody>${r.menu.map((d) => `
        <tr>
          <td>${esc(d.itemName)}</td>
          <td class="num">${money(d.price)}</td>
          <td class="num">${esc(d.stock)}</td>
          <td class="num"><button type="button" class="link edit-dish" data-dish="${esc(d.itemName)}"
            data-price="${esc(d.price)}" data-stock="${esc(d.stock)}">Edit<span class="sr"> ${esc(d.itemName)}</span></button></td>
        </tr>`).join("")}
      </tbody>
    </table>`
    : `<p class="muted">No dishes yet. Add the first one below.</p>`);
}

$("#menu-table").addEventListener("click", (event) => {
  const edit = event.target.closest(".edit-dish");
  if (!edit) return;
  const form = $("#dish-form");
  form.elements.itemName.value = edit.dataset.dish;
  form.elements.price.value = Number(edit.dataset.price).toFixed(2);
  form.elements.stock.value = edit.dataset.stock;
  form.elements.stock.focus();
});

$("#dish-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const form = event.target;
  const name = form.elements.itemName.value.trim();
  run(event.submitter, async () => {
    if (!state.restaurant) throw new Error("Show or register a restaurant first.");
    await api("PUT", `/restaurant/restaurants/${enc(state.restaurantId)}/menu/${enc(name)}`, {
      price: Number(form.elements.price.value),
      stock: parseInt(form.elements.stock.value, 10)
    });
    form.reset();
    await loadRestaurant();
    renderMenuTable();
    renderMenu();
    notify(`${name} saved`);
  });
});

async function itemsOf(orderId) {
  if (!itemsCache.has(orderId)) {
    const order = await api("GET", `/orders/${enc(orderId)}`);
    itemsCache.set(orderId, order.items);
  }
  return itemsCache.get(orderId);
}

async function renderKitchen() {
  const box = $("#kitchen");
  if (state.adminDown) {
    paint(box, `<p class="muted">The kitchen queue reads from the Admin Service, which isn't responding.</p>`);
    return;
  }
  const mine = state.orders.filter((o) =>
    o.restaurantId === state.restaurantId && (o.status === "PREPARING" || o.status === "READY"));
  if (!mine.length) {
    paint(box, `<p class="muted">Nothing to cook right now. Paid orders for <code>${esc(state.restaurantId)}</code> appear here.</p>`);
    return;
  }

  const rows = await Promise.all(mine.map(async (o) => {
    const items = await itemsOf(o.orderId).catch(() => []);
    const action = o.status === "PREPARING"
      ? `<button type="button" class="mark-ready" data-order="${esc(o.orderId)}">Mark ready<span class="sr"> ${ticketNo(o.orderId)}</span></button>`
      : `<span class="muted small">Waiting for a driver</span>`;
    return `
      <li class="queue-row">
        <span class="ticket-ref" title="${esc(o.orderId)}">${ticketNo(o.orderId)}</span>
        <span>${esc(itemSummary(items))}</span>
        ${statusPill(o.status)}
        ${action}
      </li>`;
  }));
  paint(box, `<ul class="queue">${rows.join("")}</ul>`);
}

$("#kitchen").addEventListener("click", (event) => {
  const button = event.target.closest(".mark-ready");
  if (!button) return;
  const id = button.dataset.order;
  run(button, async () => {
    await api("POST", `/restaurant/orders/${enc(id)}/ready`);
    notify(`Order ${ticketNo(id)} marked ready`);
    setTimeout(refresh, 400);
  });
});

// ---------- Driver ----------

function renderDriver() {
  if (state.adminDown) {
    const note = `<p class="muted">This list reads from the Admin Service, which isn't responding.</p>`;
    paint($("#deliveries"), note);
    paint($("#waiting"), note);
    return;
  }

  const onRoad = state.orders.filter((o) => o.status === "OUT_FOR_DELIVERY");
  paint($("#deliveries"), onRoad.length ? `
    <ul class="queue">${onRoad.map((o) => `
      <li class="queue-row">
        <span class="ticket-ref" title="${esc(o.orderId)}">${ticketNo(o.orderId)}</span>
        <span>${esc(o.driverId)}, from ${esc(o.restaurantId)}</span>
        <span class="num">${money(o.totalAmount)}</span>
        <button type="button" class="complete" data-order="${esc(o.orderId)}">Complete delivery<span class="sr"> ${ticketNo(o.orderId)}</span></button>
      </li>`).join("")}
    </ul>`
    : `<p class="muted">No deliveries on the road.</p>`);

  const ready = state.orders.filter((o) => o.status === "READY");
  const stuck = ready.some((o) => o.readyAt && Date.now() - o.readyAt > 5000);
  paint($("#waiting"), ready.length ? `
    <ul class="queue">${ready.map((o) => `
      <li class="queue-row">
        <span class="ticket-ref" title="${esc(o.orderId)}">${ticketNo(o.orderId)}</span>
        <span>From ${esc(o.restaurantId)}</span>
        <span class="num">${money(o.totalAmount)}</span>
      </li>`).join("")}
    </ul>
    ${stuck ? `<p class="hint">These orders were ready while no driver was free. The Delivery Service doesn't retry yet, so add drivers before the kitchen marks orders ready.</p>` : ""}`
    : `<p class="muted">Every ready order has a driver.</p>`);
}

$("#seed-drivers").addEventListener("click", (event) => {
  run(event.currentTarget, async () => {
    await api("POST", "/delivery/seedDrivers");
    notify("Drivers added to the pool");
  });
});

$("#deliveries").addEventListener("click", (event) => {
  const button = event.target.closest(".complete");
  if (!button) return;
  const id = button.dataset.order;
  run(button, async () => {
    await api("POST", `/delivery/orders/${enc(id)}/complete`);
    notify(`Delivery completed for order ${ticketNo(id)}`);
    setTimeout(refresh, 400);
  });
});

// ---------- Admin ----------

async function renderAdmin() {
  let overview, restaurants, delivery;
  try {
    [overview, restaurants, delivery] = await Promise.all([
      api("GET", "/admin/reports/overview"),
      api("GET", "/admin/reports/restaurants"),
      api("GET", "/admin/reports/delivery")
    ]);
  } catch (error) {
    paint($("#stats"), `<div><dt>Reports</dt><dd class="unavailable">${esc(error.message)}</dd></div>`);
    return;
  }

  paint($("#stats"), [
    ["Orders", overview.totalOrders],
    ["Delivered", overview.delivered],
    ["Cancelled", overview.cancelled],
    ["In progress", overview.inProgress],
    ["Revenue", money(overview.totalRevenue)]
  ].map(([label, value]) => `<div><dt>${label}</dt><dd>${esc(value)}</dd></div>`).join(""));

  paint($("#restaurant-report"), restaurants.length ? `
    <table>
      <thead><tr>
        <th scope="col">Restaurant</th><th scope="col" class="num">Orders</th><th scope="col" class="num">Delivered</th>
        <th scope="col" class="num">Cancelled</th><th scope="col" class="num">Revenue</th>
        <th scope="col" class="num">Average order</th><th scope="col" class="num">Average cooking time</th>
      </tr></thead>
      <tbody>${restaurants.map((r) => `
        <tr>
          <td>${esc(r.restaurantId)}</td>
          <td class="num">${esc(r.totalOrders)}</td>
          <td class="num">${esc(r.delivered)}</td>
          <td class="num">${esc(r.cancelled)}</td>
          <td class="num">${money(r.revenue)}</td>
          <td class="num">${money(r.averageOrderValue)}</td>
          <td class="num">${Number(r.averagePrepSeconds) > 0 ? duration(r.averagePrepSeconds) : "None yet"}</td>
        </tr>`).join("")}
      </tbody>
    </table>`
    : `<p class="muted">No orders yet.</p>`);

  paint($("#delivery-report"), `
    <dl class="figures">
      <div><dt>Completed</dt><dd>${esc(delivery.deliveriesCompleted)}</dd></div>
      <div><dt>On the road</dt><dd>${esc(delivery.activeDeliveries)}</dd></div>
      <div><dt>Average delivery time</dt><dd>${delivery.deliveriesCompleted > 0 ? duration(delivery.averageDeliverySeconds) : "None yet"}</dd></div>
    </dl>
    ${delivery.drivers.length ? `
      <table>
        <thead><tr><th scope="col">Driver</th><th scope="col" class="num">Deliveries</th><th scope="col" class="num">Average time</th></tr></thead>
        <tbody>${delivery.drivers.map((d) => `
          <tr><td>${esc(d.driverId)}</td><td class="num">${esc(d.deliveriesCompleted)}</td><td class="num">${duration(d.averageDeliverySeconds)}</td></tr>`).join("")}
        </tbody>
      </table>` : ""}`);
}

// ---------- Tabs and the refresh loop ----------

function selectTab(name, moveFocus = false) {
  state.tab = name;
  remember("tab", name);
  for (const tab of document.querySelectorAll("[role=tab]")) {
    const selected = tab.dataset.tab === name;
    tab.setAttribute("aria-selected", String(selected));
    tab.tabIndex = selected ? 0 : -1;
    if (selected && moveFocus) tab.focus();
  }
  for (const panel of document.querySelectorAll("[role=tabpanel]")) panel.hidden = panel.dataset.panel !== name;
}

function showTab(name, moveFocus = false) {
  selectTab(name, moveFocus);
  refresh();
}

const tablist = $("[role=tablist]");
tablist.addEventListener("click", (event) => {
  const tab = event.target.closest("[role=tab]");
  if (tab) showTab(tab.dataset.tab);
});
tablist.addEventListener("keydown", (event) => {
  if (event.key !== "ArrowRight" && event.key !== "ArrowLeft") return;
  const step = event.key === "ArrowRight" ? 1 : -1;
  const next = (TABS.indexOf(state.tab) + step + TABS.length) % TABS.length;
  showTab(TABS[next], true);
});

// Pulls fresh data for the rail and the open tab. Calls that arrive while one
// is still running are folded into a single follow-up run.
let refreshing = false;
let refreshAgain = false;
async function refresh() {
  if (refreshing) {
    refreshAgain = true;
    return;
  }
  refreshing = true;
  try {
    await loadOrders();
    renderRail();
    if (state.tab === "customer") {
      await renderMyOrders();
    } else if (state.tab === "restaurant") {
      await loadRestaurant();
      renderMenuTable();
      await renderKitchen();
    } else if (state.tab === "driver") {
      renderDriver();
    } else if (state.tab === "admin") {
      await renderAdmin();
    }
  } catch (error) {
    console.error(error);
  } finally {
    refreshing = false;
    if (refreshAgain) {
      refreshAgain = false;
      refresh();
    }
  }
}

async function start() {
  $("#restaurant-picker").elements.restaurantId.value = state.restaurantId;
  selectTab(state.tab);
  checkHealth();
  await Promise.all([loadCustomer(), loadRestaurant()]);
  renderAccount();
  renderMenu();
  renderRestaurantDetails();
  renderMenuTable();
  refresh();
  setInterval(refresh, POLL_MS);
  setInterval(checkHealth, HEALTH_MS);
}

start();
