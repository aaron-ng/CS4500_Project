#pragma once

#include "object.h"
#include "string.h"
#include "map.h"
#include "utils/dataframe_description.h"
#include "utils/key.h"

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
