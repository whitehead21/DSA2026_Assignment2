import ballerina/http;

type RouteRequest record {|
    float startLat;
    float startLon;
    float endLat;
    float endLon;
|};

type RouteResponse record {|
    float distanceKm;
    float estimatedTimeMinutes;
    string optimalPath;
|};

type DriverLocation record {|
    string driverId;
    float latitude;
    float longitude;
    string status;
|};

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /deliveries on new http:Listener(8085) {

    resource function post optimize(RouteRequest req) returns RouteResponse|error {
        float latDiff = req.endLat - req.startLat;
        float lonDiff = req.endLon - req.startLon;
        if latDiff < 0.0 { latDiff = -latDiff; }
        if lonDiff < 0.0 { lonDiff = -lonDiff; }
        
        float distance = (latDiff + lonDiff) * 111.0;
        if distance < 1.0 { distance = 2.5; }
        
        float etaMinutes = (distance / 40.0) * 60.0;

        return {
            distanceKm: distance,
            estimatedTimeMinutes: etaMinutes,
            optimalPath: "Optimized Path via Jackson Kaujeua St -> Independence Ave -> Destination"
        };
    }

    resource function get [string driverId]/location() returns DriverLocation {
        return {
            driverId: driverId,
            latitude: -22.5609 + 0.0123,
            longitude: 17.0658 + 0.0123,
            status: "EN_ROUTE_TO_CUSTOMER"
        };
    }
}
