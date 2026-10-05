import ballerina/http;
import ballerina/log;
import ballerinax/redis;
import ballerinax/kafka;

configurable string redisHost = "localhost";
configurable int redisPort = 6379;
configurable string kafkaBootstrap = "localhost:9092";

type ReadyEvent record {
    string orderId;
};

redis:Client redisClient = check new ({
    connection: {
        host: redisHost,
        port: redisPort
    }
});

kafka:Producer deliveryProducer = check new (kafkaBootstrap);

service /delivery on new http:Listener(8085) {

    resource function post seedDrivers() returns string|error {
        _ = check redisClient->sAdd("drivers:available", ["driver-001", "driver-002", "driver-003"]);
        return "Drivers seeded";
    }

    resource function get health() returns string {
        return "Delivery Service is alive";
    }

    resource function post orders/[string orderId]/complete() returns json|http:Conflict|error {
        string key = "delivery:" + orderId;

      
        // The Redis library fails on a missing key instead of returning (),
        // so trap the call and treat any failure as "no active delivery".
        string|error currentStatus = trap redisClient->hGet(key, "status");
        if !(currentStatus is string && currentStatus == "ASSIGNED") {
            return <http:Conflict>{body: {orderId: orderId, message: "No active delivery to complete"}};
        }

        string? driverId = check redisClient->hGet(key, "driverId");

        _ = check redisClient->hSet(key, "status", "DELIVERED");

        if driverId is string {
            _ = check redisClient->sAdd("drivers:available", [driverId]);
        }

        log:printInfo("Delivery completed for order " + orderId);

        json completedEvent = {orderId: orderId, status: "DELIVERED"};
        kafka:BytesProducerRecord producerRecord = {
            topic: "delivery.completed",
            key: orderId.toBytes(),
            value: completedEvent.toJsonString().toBytes()
        };
        check deliveryProducer->send(producerRecord);

        log:printInfo("Published delivery.completed for order " + orderId);

        return {orderId: orderId, status: "DELIVERED"};
    }
}

listener kafka:Listener readyListener = check new (kafkaBootstrap, {
    groupId: "delivery-service-group",
    topics: ["restaurant.order.ready"]
});

service on readyListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string message = check string:fromBytes(rec.value);
            ReadyEvent ready = check message.fromJsonStringWithType(ReadyEvent);

            string[]? popped = check redisClient->sPop("drivers:available", 1);

            if popped is string[] && popped.length() > 0 {
                string driverId = popped[0];

                _ = check redisClient->hSet("delivery:" + ready.orderId, "driverId", driverId);
                _ = check redisClient->hSet("delivery:" + ready.orderId, "status", "ASSIGNED");

                log:printInfo("Assigned driver " + driverId + " to order " + ready.orderId);

                json assignedEvent = {orderId: ready.orderId, driverId: driverId, status: "ASSIGNED"};
                kafka:BytesProducerRecord producerRecord = {
                    topic: "delivery.assigned",
                    key: ready.orderId.toBytes(),
                    value: assignedEvent.toJsonString().toBytes()
                };
                check deliveryProducer->send(producerRecord);

                log:printInfo("Published delivery.assigned for order " + ready.orderId);
            } else {
                log:printInfo("No available drivers for order " + ready.orderId);
            }
        }
    }
}
