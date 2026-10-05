import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerina/uuid;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;
import ballerinax/kafka;

configurable string dbHost = "localhost";
configurable int dbPort = 5432;
configurable string dbUser = "payments_user";
configurable string dbPassword = "payments_pass";
configurable string dbName = "payments_db";

// Simulated card limit: payments above this amount are declined
configurable decimal cardLimit = 500;

postgresql:Client paymentDb = check new (
    host = dbHost,
    port = dbPort,
    username = dbUser,
    password = dbPassword,
    database = dbName
);

configurable string kafkaBootstrap = "localhost:9092";
kafka:Producer paymentProducer = check new (kafkaBootstrap);

service /payments on new http:Listener(8084) {

    resource function get health() returns string {
        return "Payment Service is alive";
    }
}

listener kafka:Listener orderCreatedListener = check new (kafkaBootstrap, {
    groupId: "payment-service-group",
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

            string paymentId = uuid:createType1AsString();

            // Simulated payment processing: decline anything above the card limit
            boolean approved = totalAmount <= cardLimit;
            string status = approved ? "COMPLETED" : "FAILED";

            sql:ParameterizedQuery insertQuery = `
                INSERT INTO payments (payment_id, order_id, customer_id, amount, status)
                VALUES (${paymentId}::uuid, ${orderId}, ${customerId}, ${totalAmount}, ${status})
            `;
            _ = check paymentDb->execute(insertQuery);

            json paymentEvent;
            string topic;
            if approved {
                topic = "payments.completed";
                paymentEvent = {orderId: orderId, paymentId: paymentId, status: status};
            } else {
                topic = "payments.failed";
                paymentEvent = {orderId: orderId, paymentId: paymentId, status: status,
                    reason: "Card declined: amount is above the N$" + cardLimit.toString() + " limit"};
            }

            kafka:BytesProducerRecord producerRecord = {
                topic: topic,
                key: orderId.toBytes(),
                value: paymentEvent.toJsonString().toBytes()
            };
            check paymentProducer->send(producerRecord);

            log:printInfo("Payment " + status + " for order " + orderId + ", paymentId: " + paymentId
                + ". Published " + topic);
        }
    }
}
// orders.cancelled: refund the payment if the customer was charged.
// This is the compensating step when the restaurant rejects a paid order.
listener kafka:Listener orderCancelledListener = check new (kafkaBootstrap, {
    groupId: "payment-service-group",
    topics: ["orders.cancelled"]
});

service on orderCancelledListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string messageContent = check string:fromBytes(rec.value);
            json cancelledEvent = check messageContent.fromJsonString();
            string orderId = check cancelledEvent.orderId;

            sql:ParameterizedQuery refundQuery = `
                UPDATE payments SET status = 'REFUNDED'
                WHERE order_id = ${orderId} AND status = 'COMPLETED'
            `;
            sql:ExecutionResult result = check paymentDb->execute(refundQuery);

            int? refunded = result.affectedRowCount;
            if refunded is int && refunded > 0 {
                log:printInfo("Payment for order " + orderId + " refunded");
            } else {
                log:printInfo("Order " + orderId + " cancelled, nothing to refund");
            }
        }
    }
}
