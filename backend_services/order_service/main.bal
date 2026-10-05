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

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /orders on new http:Listener(8083) {

    resource function post .(Order newOrder) returns Order|error {
        float surgeMultiplier = 1.0;
        string surgeNote = "Standard Pricing";
        
        if newOrder.items.length() > 2 {
            surgeMultiplier = 1.25; 
            newOrder.totalAmount = newOrder.totalAmount * surgeMultiplier;
            surgeNote = "Surge Pricing Applied (+25% High Demand Multiplier)";
        }

        newOrder.status = CREATED;
        ordersTable.add(newOrder);

        string message = string `Order ${newOrder.id} created. Total: ${newOrder.totalAmount} (${surgeNote})`;
        check orderProducer->send({
            topic: "orders.created",
            value: message.toBytes()
        });

        return newOrder;
    }
}
