#pragma once

#include "object.h"
#include "string.h"
#include "map.h"

/**
 * A key that contains a name and a home node
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class Key: public Object {
public:
    String* _name;
    size_t _node;

    /**
     * Creates a new key
     * @param name The name of the key
     * @param node the home node of the key
     */
    Key(const char* name, size_t node) {
        _name = new String(name);
        _node = node;
    }

    /** Provides the name of the key */
    const char* getName() const {
        return _name->c_str();
    }

    /** Provides the home node of the key */
    size_t getNode() const {
        return _node;
    }

    /** Compute the hash code (subclass responsibility) */
    virtual size_t hash_me() { return _name->hash() + _node; };

    /** Subclasses should redefine */
    virtual bool equals(Object* other) {
        Key* givenKey = dynamic_cast<Key*>(other);
        if (givenKey == nullptr) {
            return false;
        }

        return _name->equals(givenKey->_name) && _node == givenKey->getNode();
    }

};

/**
 * The distributed key value store. This will connect to the central rendezvous server
 * to register it in the constructor.
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class KVStore {
public:
    Map _map;

    /**
     * Retrieves the dataframe with the given key from the key value store. If the
     * value does not exist, nullptr is returned
     * @param key The key of the dataframe to return
     */
    class DataFrame* get(Key& key) {
        return (class DataFrame*)_map.get(&key);
    }

    /**
     * Retrieves the dataframe with the given key from the key value store. If the
     * value does not exist, this call will wait until it is available
     * @param key The key of the dataframe to return
     */
    class DataFrame* waitAndGet(Key& key) {
        while (!_map.contains_key(&key)) {
            // TODO for networking
        }
        return (class DataFrame*)_map.get(&key);
    }

    /**
     * Puts the dataframe in the store. If the key references a node that is not
     * this machine, it is sent across the network.
     * @param dataframe The data to store
     * @param key The key of the dataframe in the store
     */
    void put(class DataFrame* dataframe, Key& key) {
        _map.put(&key, (Object*) dataframe);
    }

    /**
     * Provides the node identifier of the running application. This is determined
     * by the rendezvous server
     */
    size_t this_node() const {
        return 0; // TODO Node ID for server
    }
};
