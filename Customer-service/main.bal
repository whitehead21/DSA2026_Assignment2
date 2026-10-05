import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

// ---------- Data types ----------

// A delivery address. Stored inside the customer document because a
// customer has only a few addresses and they are always read together.
type Address record {
    string addressId;
    string label;
    string street;
    string city;
};

type Customer record {
    string customerId;
    string name;
    string email;
    string phone;
    Address[] addresses;
};

// Request bodies
type NewCustomer record {
    string name;
    string email;
    string phone;
};

type CustomerUpdate record {
    string name;
    string phone;
};

type NewAddress record {
    string label;
    string street;
    string city;
};

type OrderItem record {
    string itemName;
    int quantity;
    decimal price;
};

// One document per order in the orderHistory collection. Kept separate
// from the customer document because order history keeps growing.
type OrderHistoryEntry record {
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string status;
};

// The parts of the orders.created event this service needs
type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
};

// ---------- Configuration and clients ----------

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string kafkaBootstrap = "localhost:9092";

final mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${mongoHost}:${mongoPort}`
});

function customersCollection() returns mongodb:Collection|error {
    mongodb:Database customerDb = check mongoClient->getDatabase("customerdb");
    mongodb:Collection customers = check customerDb->getCollection("customers");
    return customers;
}

function historyCollection() returns mongodb:Collection|error {
    mongodb:Database customerDb = check mongoClient->getDatabase("customerdb");
    mongodb:Collection history = check customerDb->getCollection("orderHistory");
    return history;
}

// ---------- REST API ----------

service /customers on new http:Listener(8081) {

    resource function get health() returns string {
        return "Customer Service is alive";
    }

    // Register a new customer account
    resource function post .(NewCustomer req) returns Customer|http:BadRequest|http:Conflict|error {
        if !req.email.includes("@") {
            return <http:BadRequest>{body: {message: "Email address is not valid"}};
        }

        mongodb:Collection customers = check customersCollection();
        Customer? existing = check customers->findOne({email: req.email});
        if existing is Customer {
            return <http:Conflict>{body: {message: "A customer with this email already exists"}};
        }

        Customer newCustomer = {
            customerId: uuid:createType1AsString(),
            name: req.name,
            email: req.email,
            phone: req.phone,
            addresses: []
        };
        check customers->insertOne(newCustomer);

        log:printInfo("Customer registered: " + newCustomer.customerId);
        return newCustomer;
    }

    // View a customer's profile and saved addresses
    resource function get [string customerId]() returns Customer|http:NotFound|error {
        mongodb:Collection customers = check customersCollection();
        Customer? customer = check customers->findOne({customerId: customerId});
        if customer is () {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }
        return customer;
    }

    // Update a customer's name and phone number
    resource function put [string customerId](CustomerUpdate req) returns json|http:NotFound|error {
        mongodb:Collection customers = check customersCollection();
        mongodb:UpdateResult result = check customers->updateOne(
            {customerId: customerId},
            {set: {name: req.name, phone: req.phone}}
        );
        if result.matchedCount == 0 {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }

        log:printInfo("Customer updated: " + customerId);
        return {customerId: customerId, message: "Customer updated"};
    }

    // Add a delivery address to a customer
    resource function post [string customerId]/addresses(NewAddress req) returns Address|http:NotFound|error {
        mongodb:Collection customers = check customersCollection();
        Customer? customer = check customers->findOne({customerId: customerId});
        if customer is () {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }

        Address newAddress = {
            addressId: uuid:createType1AsString(),
            label: req.label,
            street: req.street,
            city: req.city
        };
        Address[] addresses = [...customer.addresses, newAddress];

        _ = check customers->updateOne(
            {customerId: customerId},
            {set: {addresses: addresses.toJson()}}
        );

        log:printInfo("Address added for customer " + customerId);
        return newAddress;
    }

    // A customer's order history, built from Kafka events
    resource function get [string customerId]/orders() returns OrderHistoryEntry[]|error {
        mongodb:Collection history = check historyCollection();
        stream<OrderHistoryEntry, error?> result = check history->find({customerId: customerId});
        return from OrderHistoryEntry entry in result select entry;
    }
}

// ---------- Kafka consumers ----------

// orders.created: add the new order to the customer's history
listener kafka:Listener orderCreatedListener = check new (kafkaBootstrap, {
    groupId: "customer-service-group",
    topics: ["orders.created"]
});

service on orderCreatedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            OrderCreatedEvent orderEvent = check messageContent.fromJsonStringWithType(OrderCreatedEvent);

            OrderHistoryEntry entry = {
                orderId: orderEvent.orderId,
                customerId: orderEvent.customerId,
                restaurantId: orderEvent.restaurantId,
                items: orderEvent.items,
                totalAmount: orderEvent.totalAmount,
                status: "CREATED"
            };
            mongodb:Collection history = check historyCollection();
            check history->insertOne(entry);

            log:printInfo("Order " + entry.orderId + " added to history of customer " + entry.customerId);
        }
    }
}

// delivery.completed: mark the order as delivered in the history
listener kafka:Listener deliveryCompletedListener = check new (kafkaBootstrap, {
    groupId: "customer-service-group",
    topics: ["delivery.completed"]
});

service on deliveryCompletedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json completedEvent = check messageContent.fromJsonString();
            string orderId = check completedEvent.orderId;
            check updateHistoryStatus(orderId, "DELIVERED");
        }
    }
}

// orders.cancelled: mark the order as cancelled in the history
listener kafka:Listener paymentFailedListener = check new (kafkaBootstrap, {
    groupId: "customer-service-group",
    topics: ["orders.cancelled"]
});

service on paymentFailedListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json failedEvent = check messageContent.fromJsonString();
            string orderId = check failedEvent.orderId;
            check updateHistoryStatus(orderId, "CANCELLED");
        }
    }
}

function updateHistoryStatus(string orderId, string status) returns error? {
    mongodb:Collection history = check historyCollection();
    mongodb:UpdateResult result = check history->updateOne({orderId: orderId}, {set: {status: status}});
    log:printInfo("History for order " + orderId + " updated to " + status
        + ". Matched: " + result.matchedCount.toString());
}
