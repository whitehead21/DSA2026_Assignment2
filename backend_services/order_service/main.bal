import ballerina/http;
import ballerinax/kafka;

// 1. Define the exact state machine required by the assignment
enum OrderStatus {
    CREATED, CONFIRMED, PREPARING, READY, OUT_FOR_DELIVERY, DELIVERED, CANCELLED
}

type Order record {|
    readonly string id;
    string customerId;
    string restaurantId;
    string[] items;
    float totalAmount;
    OrderStatus status;
|};

table<Order> key(id) ordersTable = table [];

// 2. Configure the Kafka Producer to connect to your Docker instance on port 9092
kafka:Producer orderProducer = check new (kafka:DEFAULT_URL);

service /orders on new http:Listener(8083) {

    // 3. Create an order and emit the Kafka event
    resource function post .(Order newOrder) returns Order|error {
        
        // Enforce the initial state of the machine
        newOrder.status = CREATED;
        ordersTable.add(newOrder);

        // Emit event to the "orders.created" Kafka topic 
        string message = "Order " + newOrder.id + " created for customer " + newOrder.customerId;
        check orderProducer->send({
            topic: "orders.created",
            value: message.toBytes()
        });

        return newOrder;
    }

    // 4. Progress the Order State Machine
    resource function put [string id]/status(OrderStatus newStatus) returns Order|http:NotFound|error {
        Order? existingOrder = ordersTable[id];
        if existingOrder is () {
            return http:NOT_FOUND;
        }
        
        existingOrder.status = newStatus;
        
        // Emit a status update event
        string message = "Order " + id + " transitioned to " + newStatus;
        check orderProducer->send({
            topic: "orders.updated",
            value: message.toBytes()
        });

        return existingOrder;
    }
}
