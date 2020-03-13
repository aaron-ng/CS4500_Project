# Introduction
The EU2 system is distributed system that will be used for large scale data analysis. The system will allow queries on the data. It will be written in CwC, with some C++ as needed. Each node in the distributed system will hold its own data. Data will be loaded from the SoR file format.

# Architecture 

- There is a central server that all of the nodes talk to to figure out what nodes are connected and where they are reachable 
- Each node runs an instance of an Application class. This is where the application specific logic lives
- Each node also runs an instance of the KVStore. This talks to the central server to get the list of all of the nodes. If the application requests data that is not on the node that the KVStore is running on, it will use the data it received from the central server to talk to the node that has the data.

# Implementation

##  Classes

```
/**
 * The distributed key value store. This will connect to the central rendezvous server
 * to register it in the constructor. 
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu 
 */
class KVStore {
    public:
    
        /**
         * Retrieves the dataframe with the given key from the key value store. If the 
         * value does not exist, nullptr is returned
         * @param key The key of the dataframe to return  
         */
        Dataframe* get(const Key& key)

        /**
         * Retrieves the dataframe with the given key from the key value store. If the 
         * value does not exist, this call will wait until it is available
         * @param key The key of the dataframe to return  
         */
        Dataframe* waitAndGet(const Key& key)

        /**
         * Puts the dataframe in the store. If the key references a node that is not 
         * this machine, it is sent across the network.
         * @param dataframe The data to store
         * @param key The key of the dataframe in the store 
         */
        void put(Dataframe* dataframe, const Key& key)
        
        /**
         * Provides the node identifier of the running application. This is determined
         * by the rendezvous server
         */
        size_t this_node() const 
}

/**
 * A key that contains a name and a home node 
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu 
 */
class Key {
    public:
    
        /**
         * Creates a new key
         * @param name The name of the key 
         * @param node the home node of the key
         */
        Key(const char* name, size_t node)

        /** Provides the name of the key */
        const char* getName() const 
        
        /** Provides the home node of the key */
        size_t getNode() const

}

/** 
 * Prints log lines to something
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class Printer {

    /** Prints the string */
    Printer& p(const char* contents);

    /** Prints the int */
    Printer& p(int contents);
    
    /** Prints the float */
    Printer& p(float contents);

    /** Prints the bool */
    Printer& p(bool contents);
    
    /** Prints the string with a newline after */
    Printer& pl(const char* contents);

    /** Prints the int with a newline after */
    Printer& pl(int contents);
    
    /** Prints the float with a newline after */
    Printer& pl(float contents);

    /** Prints the bool with a newline after */
    Printer& pl(bool contents);

}

/**
 * An application that is running on 
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu 
 */
 class Application: public Printer {
    public:

        /** The distributed key store that this application is connected to */
        KVStore kv;

        /**
         * Creates a new application that runs on a distributed key store. 
         * This constructor will rendezvous with the central server.
         * @param idx An unused parameter so far. The client ID will be assigned by the server,
         * but the example code supplies this param
         */
        Application(size_t idx)

        /**
         * Provides the node identifier of the running application. This is determined
         * by the rendezvous server
         */
        size_t this_node() const 

        /** The function that is called after the application setup is complete */
        virtual void _run()

}

/**
 * A frame that can hold columns of data
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class DataFrame: Codable {
    public:
    
        /**
         * Creates a new dataframe from one value. The resulting dataframe will have one column
         * and be stored in the KV store under the given key
         * @param key The key to store the dataframe under
         * @param kv The key value store to put the dataframe in
         * @param value The value to put into the dataframe
         */
        static void fromScalar(const Key& key, KVStore* kv, int value);

        /**
         * Creates a new dataframe from one value. The resulting dataframe will have one column
         * and be stored in the KV store under the given key
         * @param key The key to store the dataframe under
         * @param kv The key value store to put the dataframe in
         * @param value The value to put into the dataframe
         */
        static void fromScalar(const Key& key, KVStore* kv, bool value);
        
        /**
         * Creates a new dataframe from one value. The resulting dataframe will have one column
         * and be stored in the KV store under the given key
         * @param key The key to store the dataframe under
         * @param kv The key value store to put the dataframe in
         * @param value The value to put into the dataframe
         */
        static void fromScalar(const Key& key, KVStore* kv, float value);
        
        /**
         * Creates a new dataframe from one value. The resulting dataframe will have one column
         * and be stored in the KV store under the given key
         * @param key The key to store the dataframe under
         * @param kv The key value store to put the dataframe in
         * @param value The value to put into the dataframe
         */
        static void fromScalar(const Key& key, KVStore* kv, String* value);

        /**
         * Creates a new dataframe from an array of values. The resulting dataframe will have one column
         * and be stored in the KV store under the given key
         * @param key The key to store the dataframe under
         * @param kv The key value store to put the dataframe in
         * @param count The number of items in values
         * @param values The values to put into the dataframe
         */
        static void fromScalar(const Key& key, KVStore* kv, size_t count, int* values);

        /**
         * Creates a new dataframe from an array of values. The resulting dataframe will have one column
         * and be stored in the KV store under the given key
         * @param key The key to store the dataframe under
         * @param kv The key value store to put the dataframe in
         * @param count The number of items in values
         * @param values The values to put into the dataframe
         */
        static void fromScalar(const Key& key, KVStore* kv, size_t count, bool* values);
        
        /**
         * Creates a new dataframe from an array of values. The resulting dataframe will have one column
         * and be stored in the KV store under the given key
         * @param key The key to store the dataframe under
         * @param kv The key value store to put the dataframe in
         * @param count The number of items in values
         * @param values The values to put into the dataframe
         */
        static void fromScalar(const Key& key, KVStore* kv, size_t count, float* values);
        
        /**
         * Creates a new dataframe from an array of values. The resulting dataframe will have one column
         * and be stored in the KV store under the given key
         * @param key The key to store the dataframe under
         * @param kv The key value store to put the dataframe in
         * @param count The number of items in values
         * @param values The values to put into the dataframe
         */
        static void fromScalar(const Key& key, KVStore* kv, size_t count, String** values);

        /** Return the value at the given column and row. Accessing rows or
         *  columns out of bounds, or request the wrong type is undefined.*/
        int get_int(size_t col, size_t row) 
        bool get_bool(size_t col, size_t row)
        float get_float(size_t col, size_t row)
        String*  get_string(size_t col, size_t row)

}

/**
 * A client that is not running on this machine
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class RemoteClient {
    public:

        /**
         * Default constructor
         * @param identification The information for the remote client
         */
        RemoteClient(ClientIdentification& identification)

        /**
         * Constructor for a client that is already connected on a socket
         * @param socket The socket that the client is connected on
         */
        RemoteClient(Socket& socket)

        /**
         * Sends a message to the remote client
         * @param message The message to send
         */
        void send(Codable& message)

        /**
         * Recieves a message from the client. This is a blocking operation
         * @return The message that was sent to this cleint
         */
        Message* recieve()

};

/**
 * A client that connects to the central server
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Client {
    public:

        /**
         * Default constructor
         * @param ip The IP that the client is reachable at
         * @param handler The handler for messages. Owns the handler
         */
        Client(in_addr_t ip, uint16_t port, MessageHandler* handler)

        /**
         * Connects to the central server at the given IP. This will connect, register with the server and start
         * listening for messages from other clients until the server initiates teardown
         * @param serverIP The IP which the server can be reached on
         * @param serverPort The port that the server can be reached on
         */
        void connect(in_addr_t serverIP, uint16_t serverPort) 

        /**
         * Determines if the client is listening to messages and connected to the central server
         */
        bool connected() 

        /**
         * Reads data from the central server if there is any. If the server tears the client down,
         * all of the open sockets are closed. If there is an incoming connection, data is read and the
         * message handler is used to generate a response
         */
        void poll()

        /**
         * Sends a message to a client that is at clientInformation()[clientId]
         * @param clientId The index in clientInformation() to send the message to
         * @param data The data to send to the client
         */
        void send(size_t clientId, Codable& data)

        /**
         * Provides the information of all of the clients connected to the central server, including this one
         */
        const ClientInformation& clientInformation()

};

/**
 * The central registration server
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Server {
    public:

        /**
         * Creates a new central listening server
         * @param serverIP The IP to bind the server to
         * @param serverPort The port to bind the server to
         */
        Server(in_addr_t serverIP, uint16_t serverPort)

        /** Starts listening to incoming connections and serving the list of connected clients to any incoming clients */
        void run()

        /** Closes the server */
        void close()

};

/** 
 * A class that will build a Sor schema from a file 
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Schema {
    public:
    
        /**
         * Loads a dataframe from a file. The first 500 lines will be used to determine the schema
         * @param file
         * The file to read from 
         */ 
        Dataframe* build(FILE* file);

```

# Use cases

```
// An example integration of Application and the KeyStore 
class UseCases : public Application {
public:
  static const size_t SZ = 1000;

  Key main("main",0);
  Key verification("verifify",0);
  Key check("check",0);
 
  UseCases(size_t idx): Application(idx) {}
 
  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }
 
  void producer() {
    double vals[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) { sum += vals[i] = i; }
    
    DataFrame::fromArray(&main, &kv, SZ, vals);
    DataFrame::fromScalar(&check, &kv, sum);
  }
 
  void counter() {
    DataFrame* v = kv.waitAndGet(main);
    size_t sum = 0;
    for (size_t i = 0; i < SZ; ++i) { sum += v->get_double(0,i); }
    
    p("The sum is  ").pln(sum);
    DataFrame::fromScalar(&verify, &kv, sum);
  }
 
  void summarizer() {
    DataFrame* result = kv.waitAndGet(verify);
    DataFrame* expected = kv.waitAndGet(check);
    
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
  }
};

// Example client-server communication

Server server(inet_addr(argv[2]), SERVER_PORT);
    std::thread serverThread(&Server::run, std::ref(server));

    std::cout << "Hit enter to stop server..." << std::endl;

    if (!lastsForever) {
        char c;
        std::cin.read(&c, 1);
        server.close();
    } else { while(true) {} }

    serverThread.join();

Here is an example of a client that will say hello to the newest connected client (including itself)

in_addr_t ip = inet_addr(argv[2]);
    Client c(ip, port, new Echo());

    c.connect(inet_addr("127.0.0.1"), SERVER_PORT);

    int connectedClients = 0;
    while (c.connected()) {
        c.poll();

        int newCount = c.clientInformation().numClients;
        if (newCount != connectedClients) {
            std::cout << newCount << " connected clients" << std::endl;
            connectedClients = newCount;

            // Send the last (assumed newest client) a hello
            const char* hello = "Hello there new guy";
            Data data(hello, strlen(hello) + 1);
            c.send(connectedClients - 1, data);
        }
    }

    std::cout << "Received teardown from server" << std::endl;

class Echo: public MessageHandler {
    public:

        virtual void handleMessage(class Message* message, RemoteClient& client) {
            Deserializer deserializer = message->deserializer();

            Data data;
            data.deserialize(deserializer);
            std::cout << data.getData() << std::endl;
        }

};

// SOR adapter example

FILE* file = fopen("./test.sor", "r");
if (!file) {
    std::cerr << "Could not open file\n";
    return -1;
}

Schema builder;
Dataframe* result = builder.build(file);

```

# Open questions

Will the nodes ever write their data to disk?
If they do write to disk, what format will they use?
Are nodes going to be all distributed a SoR file and told that they are responsible for part of it?

# Status
We have implemented the dataframe adapter to read the SoR file format, we have written the dataframe class and we have written the client-server communication. The only thing that remains to be done for the current requirements is to implement the keystore network communication. This should be aproximately 15 hours of work. 
