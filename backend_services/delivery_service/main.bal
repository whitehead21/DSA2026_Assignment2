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

service /deliveries on new http:Listener(8085) {

    // 1. Route Optimization Algorithm (Flat-earth distance & ETA calculation)
    resource function post optimize(RouteRequest req) returns RouteResponse|error {
        float latDiff = req.endLat - req.startLat;
        float lonDiff = req.endLon - req.startLon;
        if latDiff < 0.0 {
            latDiff = -latDiff;
        }
        if lonDiff < 0.0 {
            lonDiff = -lonDiff;
        }
        
        // Approximate 111 km per degree
        float distance = (latDiff + lonDiff) * 111.0;
        if distance < 1.0 {
            distance = 2.5; 
        }
        
        // Assume average delivery speed of 40 km/h in city traffic
        float etaMinutes = (distance / 40.0) * 60.0;

        return {
            distanceKm: distance,
            estimatedTimeMinutes: etaMinutes,
            optimalPath: "Optimized Path via Jackson Kaujeua St -> Independence Ave -> Destination"
        };
    }

    // 2. Driver Location Simulation (Real-time GPS coordinate updates)
    resource function get [string driverId]/location() returns DriverLocation {
        float offset = 0.0123;
        
        // Simulating movement around Windhoek coordinates (-22.5609, 17.0658)
        return {
            driverId: driverId,
            latitude: -22.5609 + offset,
            longitude: 17.0658 + offset,
            status: "EN_ROUTE_TO_CUSTOMER"
        };
    }
}
