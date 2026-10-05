import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/mongodb;
import ballerinax/kafka;

type OrderItem record {
    string itemName;
    int quantity;
    decimal price;
};

type CreateOrderRequest record {
    string customerId;
    string restaurantId;
    OrderItem[] items;
};

type Order record {
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string status;
};

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;

mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${mongoHost}:${mongoPort}`
});

configurable string kafkaBootstrap = "localhost:9092";

kafka:Producer orderProducer = check new (kafkaBootstrap);

// The order lifecycle, step by step. An order only ever moves forward, so a
// late or repeated event can never send it back a step. DELIVERED and
// CANCELLED are final.
final string[] LIFECYCLE = ["CREATED", "CONFIRMED", "PREPARING", "READY", "OUT_FOR_DELIVERY", "DELIVERED"];

function ordersCollection() returns mongodb:Collection|error {
    mongodb:Database orderDb = check mongoClient->getDatabase("orderdb");
    mongodb:Collection orderCollection = check orderDb->getCollection("orders");
    return orderCollection;
}

// Moves an order to newStatus, but only if it is still at an earlier step.
// Returns true if the order moved, false if the event was ignored.
function advanceStatus(string orderId, string newStatus) returns boolean|error {
    string[] earlierSteps = [];
    if newStatus == "CANCELLED" {
        // An order can only be cancelled before the kitchen starts on it
        earlierSteps = ["CREATED", "CONFIRMED"];
    } else {
        foreach string step in LIFECYCLE {
            if step == newStatus {
                break;
            }
            earlierSteps.push(step);
        }
    }

    mongodb:Collection orderCollection = check ordersCollection();
    mongodb:UpdateResult result = check orderCollection->updateOne(
        {orderId: orderId, status: {"$in": earlierSteps}},
        {set: {status: newStatus}}
    );

    if result.matchedCount == 0 {
        log:printWarn("Ignored " + newStatus + " for order " + orderId
            + ": order not found, or it is already past that step");
        return false;
    }
    log:printInfo("Order " + orderId + " updated to " + newStatus);
    return true;
}

// Publishes an event with the orderId as its Kafka key, so all events for
// one order land in the same partition and stay in order
function publishEvent(string topic, string orderId, json event) returns error? {
    kafka:BytesProducerRecord producerRecord = {
        topic: topic,
        key: orderId.toBytes(),
        value: event.toJsonString().toBytes()
    };
    check orderProducer->send(producerRecord);
    log:printInfo("Published " + topic + " for order " + orderId);
}

service /orders on new http:Listener(8083) {

    resource function get health() returns string {
        return "Order Service is alive";
    }

    // Place a new order
    resource function post .(CreateOrderRequest req) returns Order|http:BadRequest|error {
        if req.items.length() == 0 {
            return <http:BadRequest>{body: {message: "An order needs at least one item"}};
        }

        decimal total = 0;
        foreach OrderItem item in req.items {
            if item.quantity <= 0 {
                return <http:BadRequest>{body: {message: "Every item needs a quantity of at least 1"}};
            }
            total += item.price * <decimal>item.quantity;
        }

        Order newOrder = {
            orderId: uuid:createType1AsString(),
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: req.items,
            totalAmount: total,
            status: "CREATED"
        };

        mongodb:Collection orderCollection = check ordersCollection();
        check orderCollection->insertOne(newOrder);
        log:printInfo("Order created and saved: " + newOrder.orderId);

        check publishEvent("orders.created", newOrder.orderId, newOrder.toJson());
        return newOrder;
    }

    // Look up one order and its current status
    resource function get [string orderId]() returns Order|http:NotFound|error {
        mongodb:Collection orderCollection = check ordersCollection();
        Order? found = check orderCollection->findOne({orderId: orderId});
        if found is () {
            return <http:NotFound>{body: {message: "Order not found"}};
        }
        return found;
    }
}

// payments.completed: confirm the order, then tell the restaurant (once only)
listener kafka:Listener paymentCompletedListener = check new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["payments.completed"]
});

service on paymentCompletedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json eventJson = check messageContent.fromJsonString();
            string orderId = check eventJson.orderId;

            if check advanceStatus(orderId, "CONFIRMED") {
                mongodb:Collection orderCollection = check ordersCollection();
                Order? confirmedOrder = check orderCollection->findOne({orderId: orderId});
                if confirmedOrder is Order {
                    check publishEvent("orders.confirmed", orderId, confirmedOrder.toJson());
                }
            }
        }
    }
}

// payments.failed: cancel the order and announce it on orders.cancelled
listener kafka:Listener paymentFailedListener = check new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["payments.failed"]
});

service on paymentFailedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json eventJson = check messageContent.fromJsonString();
            string orderId = check eventJson.orderId;

            json|error reasonField = eventJson.reason;
            string reason = reasonField is string ? reasonField : "Payment failed";

            if check advanceStatus(orderId, "CANCELLED") {
                check publishEvent("orders.cancelled", orderId,
                    {orderId: orderId, status: "CANCELLED", reason: reason});
            }
        }
    }
}

// restaurant.order.accepted: the kitchen has started on the order
listener kafka:Listener restaurantAcceptedListener = check new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["restaurant.order.accepted"]
});

service on restaurantAcceptedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json eventJson = check messageContent.fromJsonString();
            string orderId = check eventJson.orderId;
            _ = check advanceStatus(orderId, "PREPARING");
        }
    }
}

// restaurant.order.ready: the food is ready for pickup
listener kafka:Listener restaurantReadyListener = check new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["restaurant.order.ready"]
});

service on restaurantReadyListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json eventJson = check messageContent.fromJsonString();
            string orderId = check eventJson.orderId;
            _ = check advanceStatus(orderId, "READY");
        }
    }
}

// delivery.assigned: a driver has the order
listener kafka:Listener deliveryAssignedListener = check new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["delivery.assigned"]
});

service on deliveryAssignedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json eventJson = check messageContent.fromJsonString();
            string orderId = check eventJson.orderId;
            _ = check advanceStatus(orderId, "OUT_FOR_DELIVERY");
        }
    }
}

// delivery.completed: the order has been delivered
listener kafka:Listener deliveryCompletedListener = check new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["delivery.completed"]
});

service on deliveryCompletedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json eventJson = check messageContent.fromJsonString();
            string orderId = check eventJson.orderId;
            _ = check advanceStatus(orderId, "DELIVERED");
        }
    }
}
// restaurant.order.rejected: the kitchen cannot make the order, so cancel it
listener kafka:Listener restaurantRejectedListener = check new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["restaurant.order.rejected"]
});

service on restaurantRejectedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json eventJson = check messageContent.fromJsonString();
            string orderId = check eventJson.orderId;
            string reason = check eventJson.reason;

            if check advanceStatus(orderId, "CANCELLED") {
                check publishEvent("orders.cancelled", orderId,
                    {orderId: orderId, status: "CANCELLED", reason: "Restaurant rejected the order: " + reason});
            }
        }
    }
}