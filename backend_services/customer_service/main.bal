import ballerina/http;

type Customer record {|
    readonly string id;
    string name;
    string email;
    string deliveryAddress;
|};

// Temporary in-memory storage until MongoDB is connected
table<Customer> key(id) customersTable = table [];

service /customers on new http:Listener(8081) {
    
    // Retrieve all customers
    resource function get .() returns Customer[] {
        return customersTable.toArray();
    }

    // Register a new customer
    resource function post .(Customer customer) returns Customer|error {
        customersTable.add(customer);
        return customer;
    }

    // Get a specific customer by ID
    resource function get [string id]() returns Customer|http:NotFound {
        Customer? customer = customersTable[id];
        if customer is () {
            return http:NOT_FOUND;
        }
        return customer;
    }
}
