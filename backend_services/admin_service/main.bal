import ballerina/http;

service /admin on new http:Listener(8086) {
    resource function get stats() returns map<string> {
        return {
            "totalOrdersToday": "150",
            "activeDrivers": "12",
            "systemStatus": "Healthy"
        };
    }
}
