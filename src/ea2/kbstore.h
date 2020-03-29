#pragma once

// Language: C++

#include "../utils/key.h"

/**
 * A byte array with a length and contents
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class ByteArray: public Object {
    public:

        /** The contents of the buffer */
        const char* contents;

        /** The length of the buffer */
        size_t length;

        /** Default constructor */
        ByteArray(const char *contents, size_t length) : contents(contents), length(length) {}

        virtual ~ByteArray() {
            delete[] contents;
        }

};

/**
 * A key to byte array store. All arrays are owned by the store
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class KBStore {
    public:
        /** The map where all of the bytes are stored */
        Map _map;

        ~KBStore() {
            std::vector<Entry*>& entries = _map.entrySet();
            for (size_t i = 0; i < _map.get_size(); i++) {
                delete entries[i]->key;
                delete entries[i]->value;
            }
        }

        /**
         * Retrieves the buffer with the given key from the store. If the
         * value does not exist, nullptr is returned. The buffer is owned by this store.
         * @param key The key of the buffer to return
         */
        class ByteArray* get(Key& key) {
            return (ByteArray*)_map.get(&key);
        }

        /** Returns true if the store contains a value for the given key */
        bool contains(Key& key) { return _map.contains_key(&key); }

        /**
         * Puts a series of bytes inside of the store
         * @param contents The buffer to put into the store
         * @param length The length of the bytes in the buffer
         * @param key The key to store the buffer under
         */
        void put(const char *contents, size_t length, Key& key) {
            _map.put(key.clone(), new ByteArray(contents, length));
        }
};