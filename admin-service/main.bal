import ballerina/http;
import ballerina/log;
import ballerinax/kafka;
import ballerinax/mongodb;

// ---------- Data types ----------

// Admin saves every order event it receives into its own "orderEvents"
// collection, an append-only event log. Reports are worked out from that
// log when they are requested, so Admin never reads another service's
// database, and it does not matter in which order the events arrive.
type OrderEvent record {
    string orderId;
    string eventType;          // the Kafka topic the event came from
    int eventTime;             // Kafka event timestamp, epoch milliseconds
    string restaurantId = "";  // only filled in for orders.created
    string customerId = "";    // only filled in for orders.created
    decimal totalAmount = 0;   // only filled in for orders.created
    string driverId = "";      // only filled in for delivery.assigned
};

type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    decimal totalAmount;
};

// The current picture of one order, rebuilt from its events.
// A time of 0 means that step has not happened yet.
type OrderSummary record {|
    string orderId;
    string restaurantId = "";
    string customerId = "";
    decimal totalAmount = 0;
    string status = "CREATED";
    string driverId = "";
    int acceptedAt = 0;
    int readyAt = 0;
    int assignedAt = 0;
    int deliveredAt = 0;
|};

type Overview record {|
    int totalOrders;
    int delivered;
    int cancelled;
    int inProgress;
    decimal totalRevenue;
    int restaurants;
    int customers;
|};

type RestaurantStats record {|
    string restaurantId;
    int totalOrders;
    int delivered;
    int cancelled;
    int inProgress;
    decimal revenue;
    decimal averageOrderValue;
    decimal averagePrepSeconds;
|};

type DriverStats record {|
    string driverId;
    int deliveriesCompleted;
    decimal averageDeliverySeconds;
|};

type DeliveryReport record {|
    int deliveriesAssigned;
    int deliveriesCompleted;
    int activeDeliveries;
    decimal averageDeliverySeconds;
    DriverStats[] drivers;
|};

// ---------- Configuration and clients ----------

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string kafkaBootstrap = "localhost:9092";

final mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${mongoHost}:${mongoPort}`
});

function eventsCollection() returns mongodb:Collection|error {
    mongodb:Database adminDb = check mongoClient->getDatabase("admindb");
    mongodb:Collection events = check adminDb->getCollection("orderEvents");
    return events;
}

// Replays the event log, oldest first, to get the latest state of every order
function buildOrderSummaries() returns OrderSummary[]|error {
    mongodb:Collection events = check eventsCollection();
    stream<OrderEvent, error?> result = check events->find();
    OrderEvent[] allEvents = check from OrderEvent e in result
        order by e.eventTime ascending
        select e;

    map<OrderSummary> summaries = {};
    foreach OrderEvent e in allEvents {
        OrderSummary summary = summaries[e.orderId] ?: {orderId: e.orderId};
        match e.eventType {
            "orders.created" => {
                summary.restaurantId = e.restaurantId;
                summary.customerId = e.customerId;
                summary.totalAmount = e.totalAmount;
            }
            "payments.completed" => {
                summary.status = "CONFIRMED";
            }
            "orders.cancelled" => {
                summary.status = "CANCELLED";
            }
            "restaurant.order.accepted" => {
                summary.status = "PREPARING";
                summary.acceptedAt = e.eventTime;
            }
            "restaurant.order.ready" => {
                summary.status = "READY";
                summary.readyAt = e.eventTime;
            }
            "delivery.assigned" => {
                summary.status = "OUT_FOR_DELIVERY";
                summary.driverId = e.driverId;
                summary.assignedAt = e.eventTime;
            }
            "delivery.completed" => {
                summary.status = "DELIVERED";
                summary.deliveredAt = e.eventTime;
            }
        }
        summaries[e.orderId] = summary;
    }

    // Leave out orders that were created before Admin was running
    return from OrderSummary s in summaries
        where s.restaurantId != ""
        select s;
}

// Average of durations given in milliseconds, returned in seconds
function averageSeconds(int[] durationsMs) returns decimal {
    if durationsMs.length() == 0 {
        return 0;
    }
    int total = 0;
    foreach int duration in durationsMs {
        total += duration;
    }
    decimal averageMs = <decimal>total / <decimal>durationsMs.length();
    return (averageMs / 1000).round(1);
}

// ---------- REST API (read-only reports) ----------

service /admin on new http:Listener(8087) {

    resource function get health() returns string {
        return "Admin Service is alive";
    }

    // Every order with its current status, as Admin sees it
    resource function get orders() returns OrderSummary[]|error {
        return buildOrderSummaries();
    }

    // Platform-wide totals
    resource function get reports/overview() returns Overview|error {
        OrderSummary[] orders = check buildOrderSummaries();
        map<boolean> restaurants = {};
        map<boolean> customers = {};
        Overview overview = {
            totalOrders: 0, delivered: 0, cancelled: 0, inProgress: 0,
            totalRevenue: 0, restaurants: 0, customers: 0
        };

        foreach OrderSummary o in orders {
            overview.totalOrders += 1;
            restaurants[o.restaurantId] = true;
            customers[o.customerId] = true;
            if o.status == "DELIVERED" {
                overview.delivered += 1;
                overview.totalRevenue += o.totalAmount;
            } else if o.status == "CANCELLED" {
                overview.cancelled += 1;
            } else {
                overview.inProgress += 1;
            }
        }
        overview.restaurants = restaurants.length();
        overview.customers = customers.length();
        return overview;
    }

    // Orders, revenue and average preparation time per restaurant
    resource function get reports/restaurants() returns RestaurantStats[]|error {
        OrderSummary[] orders = check buildOrderSummaries();
        map<RestaurantStats> statsByRestaurant = {};
        map<int[]> prepTimes = {};

        foreach OrderSummary o in orders {
            RestaurantStats stats = statsByRestaurant[o.restaurantId] ?: {
                restaurantId: o.restaurantId, totalOrders: 0, delivered: 0, cancelled: 0,
                inProgress: 0, revenue: 0, averageOrderValue: 0, averagePrepSeconds: 0
            };
            stats.totalOrders += 1;
            if o.status == "DELIVERED" {
                stats.delivered += 1;
                stats.revenue += o.totalAmount;
            } else if o.status == "CANCELLED" {
                stats.cancelled += 1;
            } else {
                stats.inProgress += 1;
            }
            statsByRestaurant[o.restaurantId] = stats;

            // Preparation time: from the restaurant accepting the order to it being ready
            if o.acceptedAt > 0 && o.readyAt > 0 {
                int[] times = prepTimes[o.restaurantId] ?: [];
                times.push(o.readyAt - o.acceptedAt);
                prepTimes[o.restaurantId] = times;
            }
        }

        foreach RestaurantStats stats in statsByRestaurant {
            if stats.delivered > 0 {
                stats.averageOrderValue = (stats.revenue / <decimal>stats.delivered).round(2);
            }
            int[] times = prepTimes[stats.restaurantId] ?: [];
            stats.averagePrepSeconds = averageSeconds(times);
        }
        return statsByRestaurant.toArray();
    }

    // Delivery times, overall and per driver
    resource function get reports/delivery() returns DeliveryReport|error {
        OrderSummary[] orders = check buildOrderSummaries();
        int assigned = 0;
        int[] allTimes = [];
        map<int[]> timesByDriver = {};

        foreach OrderSummary o in orders {
            if o.assignedAt == 0 {
                continue; // no driver assigned yet
            }
            assigned += 1;

            // Delivery time: from the driver being assigned to the order being delivered
            if o.deliveredAt > 0 {
                int duration = o.deliveredAt - o.assignedAt;
                allTimes.push(duration);
                int[] driverTimes = timesByDriver[o.driverId] ?: [];
                driverTimes.push(duration);
                timesByDriver[o.driverId] = driverTimes;
            }
        }

        DriverStats[] drivers = [];
        foreach var [driver, times] in timesByDriver.entries() {
            DriverStats driverStats = {
                driverId: driver,
                deliveriesCompleted: times.length(),
                averageDeliverySeconds: averageSeconds(times)
            };
            drivers.push(driverStats);
        }

        DeliveryReport report = {
            deliveriesAssigned: assigned,
            deliveriesCompleted: allTimes.length(),
            activeDeliveries: assigned - allTimes.length(),
            averageDeliverySeconds: averageSeconds(allTimes),
            drivers: drivers
        };
        return report;
    }
}

// ---------- Kafka consumer ----------

// One consumer subscribed to seven topics. Every event is saved to the
// event log, and its topic name records what kind of event it was.
listener kafka:Listener adminListener = check new (kafkaBootstrap, {
    groupId: "admin-service-group",
    topics: [
        "orders.created", "payments.completed", "orders.cancelled",
        "restaurant.order.accepted", "restaurant.order.ready",
        "delivery.assigned", "delivery.completed"
    ]
});

service on adminListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        mongodb:Collection events = check eventsCollection();

        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            string messageContent = check string:fromBytes(rec.value);
            OrderEvent orderEvent;

            if topic == "orders.created" {
                OrderCreatedEvent created = check messageContent.fromJsonStringWithType(OrderCreatedEvent);
                orderEvent = {
                    orderId: created.orderId,
                    eventType: topic,
                    eventTime: rec.timestamp,
                    restaurantId: created.restaurantId,
                    customerId: created.customerId,
                    totalAmount: created.totalAmount
                };
            } else {
                json eventJson = check messageContent.fromJsonString();
                string orderId = check eventJson.orderId;
                orderEvent = {orderId: orderId, eventType: topic, eventTime: rec.timestamp};

                if topic == "delivery.assigned" {
                    string driverId = check eventJson.driverId;
                    orderEvent.driverId = driverId;
                }
            }

            check events->insertOne(orderEvent);
            log:printInfo("Admin stored " + topic + " event for order " + orderEvent.orderId);
        }
    }
}
