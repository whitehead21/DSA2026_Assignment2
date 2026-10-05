import ballerina/http;
import ballerinax/mongodb;

type Customer record {|
    string id;
    string name;
    string email;
    string deliveryAddress;
|};

// Connect to the MongoDB container
mongodb:Client mongoClient = check new ({
    connection: "mongodb://mongodb:27017"
});

service /customers on new http:Listener(8081) {
    
    // Register a new customer into MongoDB
    resource function post .(Customer customer) returns Customer|error {
        mongodb:Database db = check mongoClient->getDatabase("FoodDeliveryDB");
        mongodb:Collection coll = check db->getCollection("Customers");
        
        check coll->insertOne(customer);
        return customer;
    }

    // Retrieve a customer by ID from MongoDB
    resource function get [string id]() returns Customer|http:NotFound|error {
        mongodb:Database db = check mongoClient->getDatabase("FoodDeliveryDB");
        mongodb:Collection coll = check db->getCollection("Customers");
        
        // Find the document and infer the type as Customer
        Customer|error? result = coll->findOne({"id": id});
        
        if result is Customer {
            return result;
        }
        return http:NOT_FOUND;
    }
}
