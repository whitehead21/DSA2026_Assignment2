import ballerina/http;

type Customer record {|
    readonly string id;
    string name;
    string email;
    string deliveryAddress;
|};

table<Customer> key(id) customersTable = table [];

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /customers on new http:Listener(8081) {

    resource function post .(Customer newCustomer) returns Customer|error {
        customersTable.add(newCustomer);
        return newCustomer;
    }

    resource function get [string id]() returns Customer|http:NotFound|error {
        Customer? cust = customersTable[id];
        if cust is () {
            return http:NOT_FOUND;
        }
        return cust;
    }
}
