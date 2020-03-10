# Introduction

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
```

# Use cases

## TODO CHANGE SOME NAMES :))))))

```
class Demo : public Application {
public:
  Key main("main",0);
  Key verify("verif",0);
  Key check("ck",0);
 
  Demo(size_t idx): Application(idx) {}
 
  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }
 
  void producer() {
    size_t SZ = 100*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DataFrame::fromArray(&main, &kv, SZ, vals);
    DataFrame::fromScalar(&check, &kv, sum);
  }
 
  void counter() {
    DataFrame* v = kv.waitAndGet(main);
    size_t sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
    p("The sum is  ").pln(sum);
    DataFrame::fromScalar(&verify, &kv, sum);
  }
 
  void summarizer() {
    DataFrame* result = kv.waitAndGet(verify);
    DataFrame* expected = kv.waitAndGet(check);
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
  }
};
```

# Open questions

# Status
