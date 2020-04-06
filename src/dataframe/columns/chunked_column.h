// Language C++

#pragma once

#include "../../ea2/kbstore.h"
#include "../../utils/key.h"
#include "../../utils/datastructures/element_column.h"

/**
 * A column that will retrieve chunks of data from a KB store
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ChunkedColumn {
    public:

        /** An ordered list of the keys for the individual chunks */
        Key** _keys;

        /** The number of chunks in the column */
        size_t _chunkCount;

        /** The cached chunks */
        Element** _chunks;

        /** The store to retrieve data from */
        KBStore& _kbstore;

        /**
         * Creates a new column that will load chunks of data from different keys
         * @param keys The list of keys to use for chunks
         * @param chunkCount The number of chunks
         * @param kbstore The kbstore to load the chunks from
         */
        ChunkedColumn(Key **keys, size_t chunkCount, KBStore &kbstore) : _keys(keys), _chunkCount(chunkCount),
                                                                         _kbstore(kbstore) {
            _chunks = new Element*[_chunkCount];
            memset(_chunks, '\0', sizeof(Element*) * _chunkCount);
        }

        virtual ~ChunkedColumn() {
            for (size_t i = 0; i < _chunkCount; i++) {
                delete[] _chunks[i];
                delete _keys[i];
            }

            delete[] _chunks;
            delete[] _keys;
        }

        /**
         * Deserializes a single chunk that was loaded from the KBStore
         * @param deserializer the chunk to deserialize
         * @return The deserialized chunk
         */
        virtual Element* deserializeChunk(Deserializer& deserializer) = 0;

        /**
         * Gets the element at the given index. If the chunk is not already loaded, it will be loaded from the KBStore
         * @param idx The index of the item to get
         * @return The element at the given index
         */
        Element _get(size_t idx) {
            size_t chunk = idx / Column::CHUNK_SIZE;
            if (!_chunks[chunk]) {
                ByteArray* data = _kbstore.waitAndGet(*_keys[chunk]);
                Deserializer deserializer(data->length, data->contents);

                _chunks[chunk] = deserializeChunk(deserializer);
                delete data;
            }

            return _chunks[chunk][idx % Column::CHUNK_SIZE];
        }

        bool isKeyLocal(Key* key) {
            return key->getNode() == _kbstore.this_node();
        }

};

/**
 * A chunked column that will do deserialization just by reading the raw bytes into the elements
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ChunkedRawElementColumn: public ChunkedColumn {

    public:

        /**
         * Creates a new column that will load chunks of data from different keys
         * @param keys The list of keys to use for chunks
         * @param chunkCount The number of chunks
         * @param kbstore The kbstore to load the chunks from
         */
        ChunkedRawElementColumn(Key **keys, size_t chunkCount, KBStore &kbstore) : ChunkedColumn(keys, chunkCount, kbstore) {}

        /**
         * Deserializes a single chunk that was loaded from the KBStore
         * @param deserializer the chunk to deserialize
         * @return The deserialized chunk
         */
        virtual Element* deserializeChunk(Deserializer &deserializer) {
            uint64_t size = deserializer.read_uint64();
            Element* elements = new Element[size];

            for (size_t i = 0; i < size; i++) {
                elements[i] = deserializer.read_element();
            }

            return elements;
        }

};

/**
 * An IntColumn that will load chunks of data from a kbstore
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ChunkedIntColumn: public ChunkedRawElementColumn, public IntColumn {

    public:

        /** The total number of elements inside of the column */
        size_t _totalSize;

        /**
         * Creates a new column that will load chunks of data from different keys
         * @param keys The list of keys to use for chunks
         * @param chunkCount The number of chunks
         * @param kbstore The kbstore to load the chunks from
         * @param totalSize The number of elements inside of the entire column
         */
        ChunkedIntColumn(Key **keys, size_t chunkCount, KBStore &kbstore, size_t totalSize) :
            ChunkedRawElementColumn(keys, chunkCount,kbstore),
            _totalSize(totalSize) {}

        /**
         * Returns the element at the given index
         * @param idx The index of the element to return
         */
        virtual int get(size_t idx) { return _get(idx).i; }

        /**
         * Does nothing since all of the chunks are stored on thet network potentially
         */
        virtual void push_back(int val) { /* Do nothing */ }

        /**
         * Does nothing since all of the chunks are stored on thet network potentially
         */
        virtual void set(size_t idx, int val) { /* Do nothing */ }

        /**
         * Provides the size of the column
         */
        virtual size_t size() override { return _totalSize; }

};

/**
 * An BoolColumn that will load chunks of data from a kbstore
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ChunkedBoolColumn: public ChunkedRawElementColumn, public BoolColumn {

    public:

        /** The total number of elements inside of the column */
        size_t _totalSize;

        /**
         * Creates a new column that will load chunks of data from different keys
         * @param keys The list of keys to use for chunks
         * @param chunkCount The number of chunks
         * @param kbstore The kbstore to load the chunks from
         * @param totalSize The number of elements inside of the entire column
         */
        ChunkedBoolColumn(Key **keys, size_t chunkCount, KBStore &kbstore, size_t totalSize) :
                ChunkedRawElementColumn(keys, chunkCount,kbstore),
                _totalSize(totalSize) {}

        /**
         * Returns the element at the given index
         * @param idx The index of the element to return
         */
        virtual bool get(size_t idx) { return _get(idx).b; }

        /**
         * Does nothing since all of the chunks are stored on thet network potentially
         */
        virtual void push_back(bool val) { /* Do nothing */ }

        /**
         * Does nothing since all of the chunks are stored on thet network potentially
         */
        virtual void set(size_t idx, bool val) { /* Do nothing */ }

        /**
         * Provides the size of the column
         */
        virtual size_t size() override { return _totalSize; }

};

/**
 * An DoubleColumn that will load chunks of data from a kbstore
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ChunkedDoubleColumn: public ChunkedRawElementColumn, public DoubleColumn {

    public:

        /** The total number of elements inside of the column */
        size_t _totalSize;

        /**
         * Creates a new column that will load chunks of data from different keys
         * @param keys The list of keys to use for chunks
         * @param chunkCount The number of chunks
         * @param kbstore The kbstore to load the chunks from
         * @param totalSize The number of elements inside of the entire column
         */
        ChunkedDoubleColumn(Key **keys, size_t chunkCount, KBStore &kbstore, size_t totalSize) :
                ChunkedRawElementColumn(keys, chunkCount,kbstore),
                _totalSize(totalSize) {}

        /**
         * Returns the element at the given index
         * @param idx The index of the element to return
         */
        virtual double get(size_t idx) { return _get(idx).f; }

        /**
         * Does nothing since all of the chunks are stored on thet network potentially
         */
        virtual void push_back(double val) { /* Do nothing */ }

        /**
         * Does nothing since all of the chunks are stored on thet network potentially
         */
        virtual void set(size_t idx, double val) { /* Do nothing */ }

        /**
         * Provides the size of the column
         */
        virtual size_t size() override { return _totalSize; }

};

class ChunkedStringColumn: public ChunkedColumn, public StringColumn {
    public:

        /** The total number of elements inside of the column */
        size_t _totalSize;

        /**
         * Creates a new column that will load chunks of data from different keys
         * @param keys The list of keys to use for chunks
         * @param chunkCount The number of chunks
         * @param kbstore The kbstore to load the chunks from
         * @param totalSize The number of elements inside of the entire column
         */
        ChunkedStringColumn(Key **keys, size_t chunkCount, KBStore &kbstore, size_t totalSize) :
            ChunkedColumn(keys, chunkCount,kbstore),
            _totalSize(totalSize) {}

        virtual ~ChunkedStringColumn() {
            for (size_t i = 0; i < _chunkCount; i++) {
                if (_chunks[i]) {
                    for (size_t idx = 0; idx < CHUNK_SIZE && i * CHUNK_SIZE + idx < _totalSize; idx++) {
                        delete _chunks[i][idx].s;
                    }
                }
            }
        }

        virtual String* get(size_t idx) { return _get(idx).s; }

        /**
         * Does nothing since all of the chunks are stored on thet network potentially
         */
        virtual void push_back(String *val) { /* Do nothing */ }

        /**
         * Does nothing since all of the chunks are stored on thet network potentially
         */
        virtual void set(size_t idx, String *val) { /* Do nothing */ }

        /**
         * Provides the size of the column
         */
        virtual size_t size() { return _totalSize; }

        /**
         * Deserializes a single chunk that was loaded from the KBStore
         * @param deserializer the chunk to deserialize
         * @return The deserialized chunk
         */
        Element *deserializeChunk(Deserializer &deserializer) override {
            uint64_t size = deserializer.read_uint64();
            Element* elements = new Element[size];

            for (size_t i = 0; i < size; i++) {
                elements[i].s = deserializer.read_string();
            }

            return elements;
        }

};