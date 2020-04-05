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

    /** The store for raw bytes under keys */
    KBStore _byteStore;

    /**
     * Default constructor
     * @param ip The IP that the client is reachable at
     * @param handler The handler for messages. Owns the handler
     * @param serverIP The IP of the rendezvous server
     * @param serverPort The port of the rendezvous server
     */
    KVStore(in_addr_t ip, uint16_t port, in_addr_t serverIP, uint16_t serverPort);

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
     * @param chunk The chunk index
     * @param node The node for the key
     * @return The key for the column in a dataframe stored under the given key
     */
    Key* _keyFor(const Key& key, size_t column, size_t chunk, size_t node) const;

    /**
     * Creates a new dataframe using the dataframe description
     * @param desc The description of the dataframe to use to build the new one
     * @return A new dataframe
     */
    DataFrame* _dataframeFrom(ByteArray* desc);

};
