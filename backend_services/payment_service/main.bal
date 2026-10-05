import ballerina/http;

type Payment record {|
    string orderId;
    float amount;
    string status; // PENDING, COMPLETED, FAILED
|};

service /payments on new http:Listener(8084) {
    resource function post process(Payment payment) returns Payment|error {
        payment.status = "COMPLETED"; // Simulating processing
        return payment;
    }
}
