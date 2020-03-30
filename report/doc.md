# Introduction
The EU2 system is distributed system that will be used for large scale data analysis. The system will allow queries on the data. It will be written in CwC, with some C++ as needed. Each node in the distributed system will hold its own data. Data will be loaded from the SoR file format.

# Architecture 

- There is a central server that all of the nodes talk to to figure out what nodes are connected and where they are reachable 
- Each node runs an instance of an Application class. This is where the application specific logic lives
- Each node also runs an instance of the KVStore. This talks to the central server to get the list of all of the nodes. If the application requests data that is not on the node that the KVStore is running on, it will use the data it received from the central server to talk to the node that has the data.
- When a dataframe is stored in the KVStore, the columns of it are each stored on different nodes. When it is requested, the columns are pulled from all of the nodes and turned back into a dataframe.

# Implementation

##  Classes

`KVStore`
- Abstracts away the distributed nature of the key store
- Manages concurrency and networking 
- Allows for putting and getting data frames

`Key`
- Stores the home node and the name of an entry in the distributed key store

`Application`
- Manages lifecyle of the KVStore
- Provides a 'main' like run method

`Dataframe`
- Stores columns of data
- Has convience methods to create a dataframe from a single value or an array of values 
- Ensures that columns adhere to a schema
- Allows reading / writing data 

`RemoteClient`
- An abstraction for sending and recieving messages from another node
- Allows sending and recieving messages
- Handles serialization but not deserialization

`Client`
- Rendezvous with the central server
- Enables staying up to date with the currently connected nodes
- Allows for sending messages to other clients
- Allows for recieving messages from other clients

`Server`
- Central rendezvous server for nodes
- Accepts incoming connections and broadcasts IPs and ports of currently connected clients

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
    // Use the current client ID to choose a task
  }
 
  void producer() {
    // Create a dataframe and write it to the store    
    DataFrame::fromArray(&main, &kv, SZ, vals);
    DataFrame::fromScalar(&check, &kv, sum);
  }
 
  void counter() {
    DataFrame* v = kv.waitAndGet(main);
    
    // Do something with the dataframe and write it back to the key store
    DataFrame::fromScalar(&verify, &kv, sum);
  }
 
  void summarizer() {
    // Wait until operation is complete and make sure that it was correct
    DataFrame* result = kv.waitAndGet(verify);
    DataFrame* expected = kv.waitAndGet(check);
    
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
  }
};
```

SOR files can be loaded by using an instance of the `Schema` class and using the build method.

# Open questions
There are no open questions at this time.

# Status
We have implemented the dataframe adapter to read the SoR file format, we have written the dataframe class and we have written the client-server communication. The only thing that remains to be done for the current requirements is to implement the keystore network communication. This should be aproximately 15 hours of work. 
