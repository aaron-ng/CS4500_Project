// Language C++

#include "kvstore.h"
#include "../../dataframe/dataframe.h"
#include "../dataframe_description.h"

KVStore::KVStore(in_addr_t ip, uint16_t port, in_addr_t serverIP, uint16_t serverPort): _byteStore(ip, port, serverIP, serverPort) {}

/**
 * Retrieves the dataframe with the given key from the key value store. If the
 * value does not exist, nullptr is returned
 * @param key The key of the dataframe to return
 */
DataFrame* KVStore::get(Key& key) {
    return _dataframeFrom(_byteStore.get(key));
}

/**
 * Retrieves the dataframe with the given key from the key value store. If the
 * value does not exist, this call will wait until it is available
 * @param key The key of the dataframe to return
 */
DataFrame* KVStore::waitAndGet(Key& key) {
    return _dataframeFrom(_byteStore.waitAndGet(key));
}

/**
 * Puts the dataframe in the store. If the key references a node that is not
 * this machine, it is sent across the network.
 * @param dataframe The data to store
 * @param key The key of the dataframe in the store
 */
void KVStore::put(DataFrame* dataframe, Key& key) {
    size_t stores = _byteStore.nodes();
    DataframeDescription* description = _descFrom(dataframe, key, stores);

    for (uint64_t i = 0; i < dataframe->getColumn(0)->numChunks(); i++) {
        putDataframeChunk(key, dataframe, i, stores);
    }

    putDataframeDesc(key, description);

    delete description;
}

/**
 * Provides the node identifier of the running application. This is determined
 * by the rendezvous server
 */
size_t KVStore::this_node() const { return _byteStore.this_node(); }

/** Generates a description of a dataframe that can be serialized */
DataframeDescription* KVStore::_descFrom(DataFrame* dataframe, Key& key, size_t stores) {
    // Generate column descriptions
    size_t columns = dataframe->ncols();
    ColumnDescription** descriptions = new ColumnDescription*[columns];

    for (size_t i = 0; i < columns; i++) {
        Column* column = dataframe->getColumn(i);
        size_t numChunks = column->numChunks();
        Key** chunkKeys = new Key*[numChunks];

        for (size_t chunk = 0; chunk < numChunks; chunk++) {
            chunkKeys[chunk] = _keyFor(key, i, chunk, stores);
        }

        descriptions[i] = new ColumnDescription(chunkKeys, numChunks, column->size(), (ColumnType)dataframe->get_schema().col_type(i));
    }

    return new DataframeDescription(new String(dataframe->get_schema().types()), columns, descriptions);
}

Key* KVStore::_keyFor(const Key& key, size_t column, size_t chunk, size_t nodes) {
    sprintf(_keyBuffer, "%s-%zu-%zu", key.getName(), column, chunk);
    return new Key(_keyBuffer, chunk % nodes);
}

DataFrame *KVStore::_dataframeFrom(ByteArray* bytes) {
    if (!bytes) { return nullptr; }

    Deserializer deserializer(bytes->length, bytes->contents);
    DataframeDescription desc;
    desc.deserialize(deserializer);

    Schema schema("");
    DataFrame* dataframe = new DataFrame(schema);

    for (size_t i = 0; i < desc.numColumns; i++) {
        ColumnDescription* colDesc = desc.columns[i];
        Key** keyCopies = new Key*[colDesc->chunks];
        for (size_t chunk = 0; chunk < colDesc->chunks; chunk++) {
            keyCopies[chunk] = (Key*)colDesc->keys[chunk]->clone();
        }

        Column* newColumn = allocateChunkedColumnOfType(colDesc->type, keyCopies, colDesc->chunks, _byteStore, colDesc->totalLength);
        dataframe->add_column(newColumn, nullptr);
    }

    delete bytes;
    return dataframe;
}

void KVStore::putDataframeChunk(const Key& key, DataFrame* dataframe, size_t chunk, size_t nodes, long int serializedChunk) {
    for (size_t col = 0; col < dataframe->ncols(); col++) {
        Column* column = dataframe->getColumn(col);

        Key* chunkKey = _keyFor(key, col, chunk, nodes);
        Serializer serializer;
        column->serializeChunk(serializer, serializedChunk == -1 ? chunk : serializedChunk);

        _byteStore.put(serializer.getBuffer(), serializer.getSize(), *chunkKey);
        delete chunkKey;
    }
}

void KVStore::putDataframeDesc(Key &key, DataframeDescription *desc) {
    Serializer serializer;
    desc->serialize(serializer);

    _byteStore.put(serializer.getBuffer(), serializer.getSize(), key);
}
