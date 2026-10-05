import ballerina/http;

type MenuItem record {|
    readonly string id;
    string name;
    float price;
    boolean isAvailable;
|};

type Restaurant record {|
    readonly string id;
    string name;
    string openingHours;
    MenuItem[] menu;
|};

table<Restaurant> key(id) restaurantsTable = table [];

service /restaurants on new http:Listener(8082) {

    // Retrieve all restaurants
    resource function get .() returns Restaurant[] {
        return restaurantsTable.toArray();
    }

    // Add a new restaurant
    resource function post .(Restaurant restaurant) returns Restaurant|error {
        restaurantsTable.add(restaurant);
        return restaurant;
    }

    // Get a specific restaurant by ID
    resource function get [string id]() returns Restaurant|http:NotFound {
        Restaurant? restaurant = restaurantsTable[id];
        if restaurant is () {
            return http:NOT_FOUND;
        }
        return restaurant;
    }
}
