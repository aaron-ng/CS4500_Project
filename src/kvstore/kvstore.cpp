#include "kvstore.h"
#include "../dataframe/modified_dataframe.h"
#include "../utils/dataframe_description.h"

static KBStore byteStores[NUM_KV_STORES];
static KVStore stores[NUM_KV_STORES];

KVStore::~KVStore() {
    Object** descs = _map.values();
    for (size_t i = 0; i < _map.get_size(); i++) {
        delete descs[i];
    }
}

/**
 * Retrieves the dataframe with the given key from the key value store. If the
 * value does not exist, nullptr is returned
 * @param key The key of the dataframe to return
 */
DataFrame* KVStore::get(Key& key) {
    DataframeDescription* desc = (DataframeDescription*)stores[key.getNode()]._map.get(&key);

    Schema s("");
    DataFrame* dataFrame = new DataFrame(s);
    for (size_t i = 0; i < desc->numColumns; i++) {
        Column* newColumn = allocateColumnOfType(desc->columns[i]->type);

        Key* columnKey = desc->columns[i]->location;
        ByteArray* bytes = byteStores[columnKey->getNode()].get(*columnKey);

        Deserializer deserializer( bytes->length, bytes->contents);
        newColumn->deserialize(deserializer);
        dataFrame->add_column(newColumn, nullptr);
    }

    return dataFrame;
}

/**
 * Retrieves the dataframe with the given key from the key value store. If the
 * value does not exist, this call will wait until it is available
 * @param key The key of the dataframe to return
 */
DataFrame* KVStore::waitAndGet(Key& key) {
    // TODO use networking
    while (!stores[key.getNode()]._map.contains_key(&key)) {

    }
    return get(key);
}

/**
 * Puts the dataframe in the store. If the key references a node that is not
 * this machine, it is sent across the network.
 * @param dataframe The data to store
 * @param key The key of the dataframe in the store
 */
void KVStore::put(DataFrame* dataframe, Key& key) {
    DataframeDescription* desc = _descFrom(dataframe, key);

    // TODO CHANGE FOR NETWORKING
    for (size_t i = 0; i < dataframe->ncols(); i++) {
        size_t node = i % NUM_KV_STORES;

        String* name = _keyFor(key, i);
        Key newKey(name->c_str(), node);
        delete name;

        Serializer serializer;
        dataframe->getColumn(i)->serialize(serializer);
        byteStores[node].put(serializer.getUnownedBuffer(), serializer.getSize(), newKey);
    }

    // TODO use networking to put
    stores[key.getNode()]._map.put(new Key(key.getName(), key.getNode()), desc);
}

/**
 * Provides the node identifier of the running application. This is determined
 * by the rendezvous server
 */
size_t KVStore::this_node() const {
    return 0; // TODO Node ID for server
}

/** Generates a description of a dataframe that can be serialized */
DataframeDescription* KVStore::_descFrom(DataFrame* dataframe, Key& key) const {
    // Generate column descriptions
    size_t columns = dataframe->ncols();
    ColumnDescription** descriptions = new ColumnDescription*[columns];

    for (size_t i = 0; i < columns; i++) {
        size_t node = i % NUM_KV_STORES;

        String* name = _keyFor(key, i);
        Key newKey(name->c_str(), node);
        delete name;

        descriptions[i] = new ColumnDescription(newKey, (ColumnType)dataframe->get_schema().col_type(i));
    }

    return new DataframeDescription(new String(dataframe->get_schema().types()), columns, descriptions);
}

// Names up until 1024 including the column numbers are supported. No actual checking is done
char keyBuffer[1024];
String* KVStore::_keyFor(const Key& key, size_t column) const {
    sprintf(keyBuffer, "%s-%zu", key.getName(), column);
    return new String(keyBuffer);
}
