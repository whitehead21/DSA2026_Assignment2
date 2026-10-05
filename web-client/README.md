# Web client

A browser front end for the platform. Open http://localhost:8080 once `docker compose up -d --build` has finished.

- **Order rail**, the dark band at the top: every order is a ticket in the station for its current status, and each station is labelled with the Kafka event that moves an order into it. Place an order and watch its ticket travel from Created to Delivered.
- **Service status:** a green or red dot for each of the seven services.
- **Customer tab:** create an account, add delivery addresses, order from a restaurant's menu and follow your orders.
- **Restaurant tab:** register a restaurant, set its kitchen hours, manage dishes and stock, and mark orders ready.
- **Driver tab:** add drivers to the pool and complete deliveries.
- **Admin tab:** totals, restaurant statistics and delivery performance.

## A demo in five steps

1. **Driver:** add drivers to the pool.
2. **Restaurant:** register `rest-001` and add a few dishes with prices and stock.
3. **Customer:** create an account, pick some dishes and place the order. The ticket moves to Preparing on its own, because payment and the kitchen react to Kafka events.
4. **Restaurant:** mark the order ready. A driver is assigned straight away.
5. **Driver:** complete the delivery.

To show the cancellation paths, order more than N$500 (the payment is declined), or more of a dish than the kitchen has in stock (the kitchen rejects the paid order and the payment is refunded).

## How it works

The `web-client` container runs nginx. It serves the page and forwards each API path to its service: `/customers` to customer-service:8081, `/orders` to order-service:8083, `/admin` to admin-service:8087 and so on. The browser only ever talks to one address, so the services need no CORS setup. nginx looks the services up through Docker's DNS every 10 seconds, so it still finds a service after its container is rebuilt.

The page only uses the services' REST APIs and never talks to Kafka itself. The rail and the role tabs poll the Admin Service every 2 seconds, and the Admin Service builds its view of every order from the Kafka events. A customer's own orders come from the Customer Service (the history) and the Order Service (the live status).

| File | Purpose |
|---|---|
| `index.html` | Page structure: the rail and the four role tabs |
| `app.js` | Calls the services, polls for changes and draws the page |
| `styles.css` | Layout and styling |
| `nginx.conf` | Serves the page and forwards API calls to the services |
| `Dockerfile` | Builds the nginx image |
| `fonts/` | Barlow and Barlow Condensed (SIL Open Font License), bundled so the page works without internet access |
