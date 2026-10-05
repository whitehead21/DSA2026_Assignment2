import ballerina/http;

type Delivery record {|
    string orderId;
    string driverId;
    string status; // ASSIGNED, EN_ROUTE, DELIVERED
|};

service /deliveries on new http:Listener(8085) {
    resource function post assign(Delivery delivery) returns Delivery|error {
        delivery.status = "ASSIGNED";
        return delivery;
    }
}
