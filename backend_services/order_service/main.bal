import ballerina/http;
import ballerinax/kafka;

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

kafka:Producer orderProducer = check new (kafka:DEFAULT_URL);

service /orders on new http:Listener(8083) {

    // Create an order with dynamic Surge Pricing
    resource function post .(Order newOrder) returns Order|error {
        
        // --- SURGE PRICING LOGIC (BONUS FEATURE) ---
        // If an order contains more than 2 items, simulate high demand and apply a 25% surge multiplier
        float surgeMultiplier = 1.0;
        string surgeNote = "Standard Pricing";
        
        if newOrder.items.length() > 2 {
            surgeMultiplier = 1.25; 
            newOrder.totalAmount = newOrder.totalAmount * surgeMultiplier;
            surgeNote = "Surge Pricing Applied (25% High Demand Multiplier)";
        }
        // -------------------------------------------

        newOrder.status = CREATED;
        ordersTable.add(newOrder);

        // Emit event to the "orders.created" Kafka topic including the surge info
        string message = string `Order ${newOrder.id} created. Total: ${newOrder.totalAmount} (${surgeNote})`;
        check orderProducer->send({
            topic: "orders.created",
            value: message.toBytes()
        });

        return newOrder;
    }

    // Progress the Order State Machine
    resource function put [string id]/status(OrderStatus newStatus) returns Order|http:NotFound|error {
        Order? existingOrder = ordersTable[id];
        if existingOrder is () {
            return http:NOT_FOUND;
        }
        
        existingOrder.status = newStatus;
        
        string message = "Order " + id + " transitioned to " + newStatus;
        check orderProducer->send({
            topic: "orders.updated",
            value: message.toBytes()
        });

        return existingOrder;
    }
}
