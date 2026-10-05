import ballerina/http;
import ballerina/log;
import ballerinax/kafka;

configurable string kafkaBootstrap = "localhost:9092";

kafka:Producer notificationProducer = check new (kafkaBootstrap);

service /notifications on new http:Listener(8086) {

    resource function get health() returns string {
        return "Notification Service is alive";
    }
}

// Listener for orders.created
listener kafka:Listener orderCreatedListener = check new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: ["orders.created"]
});

service on orderCreatedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json orderEvent = check messageContent.fromJsonString();

            string orderId = check orderEvent.orderId;
            string customerId = check orderEvent.customerId;
            decimal totalAmount = check orderEvent.totalAmount;

            log:printInfo("SMS to customer " + customerId + ": Your order " + orderId + " has been created. Total: N$" + totalAmount.toString());
            log:printInfo("Email to customer " + customerId + ": Order confirmation for order " + orderId);
        }
    }
}

// Listener for payments.completed
listener kafka:Listener paymentCompletedListener = check new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: ["payments.completed"]
});

service on paymentCompletedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json paymentEvent = check messageContent.fromJsonString();

            string orderId = check paymentEvent.orderId;
            string paymentId = check paymentEvent.paymentId;

            log:printInfo("SMS: Payment for order " + orderId + " confirmed. Payment ID: " + paymentId);
            log:printInfo("Email: Payment receipt for order " + orderId + " sent successfully");
        }
    }
}

// Listener for restaurant.order.ready
listener kafka:Listener restaurantReadyListener = check new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: ["restaurant.order.ready"]
});

service on restaurantReadyListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json readyEvent = check messageContent.fromJsonString();

            string orderId = check readyEvent.orderId;

            log:printInfo("SMS: Your order " + orderId + " is ready for pickup/delivery");
            log:printInfo("Email: Order " + orderId + " status update: Ready for delivery");
        }
    }
}

// Listener for delivery.assigned
listener kafka:Listener deliveryAssignedListener = check new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: ["delivery.assigned"]
});

service on deliveryAssignedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json assignedEvent = check messageContent.fromJsonString();

            string orderId = check assignedEvent.orderId;
            string driverId = check assignedEvent.driverId;

            log:printInfo("SMS: Driver " + driverId + " has been assigned to deliver your order " + orderId);
            log:printInfo("Email: Your order " + orderId + " is on its way. Driver: " + driverId);
            log:printInfo("SMS to driver " + driverId + ": You have been assigned order " + orderId + ". Please proceed to pickup");
            log:printInfo("Email to driver " + driverId + ": New delivery assignment: order " + orderId);
        }
    }
}

// Listener for delivery.completed
listener kafka:Listener deliveryCompletedListener = check new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: ["delivery.completed"]
});

service on deliveryCompletedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json completedEvent = check messageContent.fromJsonString();

            string orderId = check completedEvent.orderId;

            log:printInfo("SMS: Your order " + orderId + " has been delivered. Thank you!");
            log:printInfo("Email: Order " + orderId + " delivery confirmation");
        }
    }
}
// ---------- Restaurant and cancellation alerts ----------

type NotifiedItem record {
    string itemName;
    int quantity;
};

type ConfirmedOrderEvent record {
    string orderId;
    string restaurantId;
    decimal totalAmount;
    NotifiedItem[] items;
};

// orders.confirmed: tell the restaurant a paid order is coming in
listener kafka:Listener orderConfirmedListener = check new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: ["orders.confirmed"]
});

service on orderConfirmedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            ConfirmedOrderEvent confirmed = check messageContent.fromJsonStringWithType(ConfirmedOrderEvent);

            string[] itemLines = from NotifiedItem item in confirmed.items
                select item.quantity.toString() + " x " + item.itemName;
            string itemSummary = string:'join(", ", ...itemLines);

            log:printInfo("SMS to restaurant " + confirmed.restaurantId + ": New paid order " + confirmed.orderId
                + ": " + itemSummary + ". Total N$" + confirmed.totalAmount.toString());
            log:printInfo("Email to restaurant " + confirmed.restaurantId + ": Order " + confirmed.orderId
                + " is confirmed and waiting to be prepared");
        }
    }
}

// orders.cancelled: tell the customer why their order was cancelled
listener kafka:Listener orderCancelledListener = check new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: ["orders.cancelled"]
});

service on orderCancelledListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json cancelledEvent = check messageContent.fromJsonString();

            string orderId = check cancelledEvent.orderId;
            string reason = check cancelledEvent.reason;

            log:printInfo("SMS: Your order " + orderId + " has been cancelled. Reason: " + reason);
            log:printInfo("Email: Cancellation notice for order " + orderId + ". Any payment for it is refunded");
        }
    }
}
