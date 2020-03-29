// Language C++

#include "kvstore.h"
#include "../../dataframe/dataframe.h"
#include "../dataframe_description.h"

KVStore::~KVStore() {
    std::vector<Entry*>& entries = _map.entrySet();
    for (size_t i = 0; i < entries.size(); i++) {
        delete entries[i]->key;
        delete entries[i]->value;
    }

    std::vector<Entry*>& statusEntries = _statuses.entrySet();
    for (size_t i = 0; i < statusEntries.size(); i++) {
        delete statusEntries[i]->key;
        delete statusEntries[i]->value;
    }
}

/**
 * Retrieves the dataframe with the given key from the key value store. If the
 * value does not exist, nullptr is returned
 * @param key The key of the dataframe to return
 */
DataFrame* KVStore::get(Key& key) {
    DataframeDescription* desc = (DataframeDescription*)_stores[key.getNode()]->_map.get(&key);

    Schema s(desc->schema->c_str());
    DataFrame* dataFrame = new DataFrame(s);

    for (size_t i = 0; i < desc->numColumns; i++) {
        Key* columnKey = desc->columns[i]->location;
        ByteArray* bytes = _stores[columnKey->getNode()]->_byteStore.get(*columnKey);

        Deserializer deserializer( bytes->length, bytes->contents);
        dataFrame->getColumn(i)->deserialize(deserializer);
    }

    return dataFrame;
}

/**
 * Retrieves the dataframe with the given key from the key value store. If the
 * value does not exist, this call will wait until it is available
 * @param key The key of the dataframe to return
 */
DataFrame* KVStore::waitAndGet(Key& key) {

    // This has some complicated logic to prevent deadlocks. Anything that waitAndGet is called on will have an
    // item in the status map on its home node. The status mutex is locked immediately so that until the status
    // object is acquired, the status cannot be updated if it already exists and cannot be missed if it doesn't.

    // If we didn't have this extra map, we would be checking the main map over and over to see if it contained the
    // key. Its possible that this could stop whatever is writing the frame to the store from acquiring the mutex and
    // thus there will be a deadlock. By using this method below, there is one lock and unlock so this problem doesn't
    // exist.

    KVStore& store = *_stores[key.getNode()];
    store._statusMutex.lock();

    if (store._map.contains_key(&key)) {
        store._statusMutex.unlock();
        return get(key);
    }

    Ready* ready = dynamic_cast<Ready*>(store._statuses.get(&key));
    if (!ready) {
        ready = new Ready();
        store._statuses.put(key.clone(), ready);
    }

    store._statusMutex.unlock();
    while (ready->isReady == false) {}

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
        size_t node = i % _stores.size();

        String* name = _keyFor(key, i);
        Key newKey(name->c_str(), node);
        delete name;

        Serializer serializer;
        dataframe->getColumn(i)->serialize(serializer);
        _stores[node]->_byteStore.put(serializer.getUnownedBuffer(), serializer.getSize(), newKey);
    }

    // TODO use networking to put
    KVStore& store = *_stores[key.getNode()];
    store._statusMutex.lock();

    store._map.put(key.clone(), desc);
    Ready* ready = dynamic_cast<Ready*>(store._statuses.get(&key));
    if (ready) { ready->isReady = true; }

    store._statusMutex.unlock();
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
        size_t node = i % _stores.size();

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
