#pragma once

// Language: C++

#include <atomic>

#include "../../utils/instructor-provided/object.h"
#include "../../utils/instructor-provided/string.h"
#include "../../utils/datastructures/map.h"
#include "../../utils/key.h"
#include "../kbstore.h"

/**
 * The distributed key value store. This will connect to the central rendezvous server
 * to register it in the constructor.
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class KVStore {
public:

    /** The mapping of keys to descriptions */
    Map _map;

    /** Statuses of keys that are used for waitAndGet. This prevents deadlocks */
    Map _statuses;

    /** Mutex for _statuses */
    std::mutex _statusMutex;

    /** The store for raw bytes under keys */
    KBStore _byteStore;

    /** References to all of the KV stores. TODO REMOVE */
    std::vector<KVStore*> _stores;


    ~KVStore();

    /**
     * Retrieves the dataframe with the given key from the key value store. If the
     * value does not exist, nullptr is returned
     * @param key The key of the dataframe to return
     */
    class DataFrame* get(Key& key);

    /**
     * Retrieves the dataframe with the given key from the key value store. If the
     * value does not exist, this call will wait until it is available
     * @param key The key of the dataframe to return
     */
    class DataFrame* waitAndGet(Key& key);

    /**
     * Puts the dataframe in the store. If the key references a node that is not
     * this machine, it is sent across the network.
     * @param dataframe The data to store
     * @param key The key of the dataframe in the store
     */
    void put(class DataFrame* dataframe, Key& key);

    /**
     * Provides the node identifier of the running application. This is determined
     * by the rendezvous server
     */
    size_t this_node() const;

    /**
     * Generates a description of the dataframe in the distributed key store.
     * @param dataframe The dataframe to generate the description of
     * @param key The key the dataframe will be stored under
     */
    class DataframeDescription* _descFrom(class DataFrame* dataframe, Key& key) const;

    /**
     * Provides the key for the column in the byte stores
     * @param key The key that the dataframe is stored under
     * @param column The column index
     * @return The key for the column in a dataframe stored under the given key
     */
    String* _keyFor(const Key& key, size_t column) const;

    /**
     * An object wrapper a std::atomic that says if a key is ready
     * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
     */
    class Ready: public Object {
        public:
            std::atomic<bool> isReady;

            /** Default constructor */
            Ready() : isReady(false) {}
    };
};
