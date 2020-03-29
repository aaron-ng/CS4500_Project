//
// Created by Jiawen Liu on 1/24/20.
//

// Language: C++

#ifndef MAP_H
#define MAP_H

#pragma once
#include "../instructor-provided/object.h"

#include <vector>
#include <mutex>

/**
 * An entry in a map
 */
class Entry: public Object {
public:

    /** The object used as the key */
    Object* key;

    /** The value of the entry */
    Object* value;

    /** The hash value of the key */
    size_t hash;

    /**
     * Creates a new entry
     * @param key The key of the entry
     * @param value The value of the entry
     */
    Entry(Object *key, Object *value) : key(key), value(value) {
        hash = key->hash();
    }
};

/**
 * An implementation of map that will use the hash code of objects to sort them into buckets.
 * If there are multiple elements in a bucket, equals() is used
 */
class Map : public Object {
public:

    /** The array that stores all of the buckets */
    std::vector<std::vector<Entry*>> _array;

    /** The collection of entries in the map */
    std::vector<Entry*> _entrySet;

    /** Mutex to make this map thread safe */
    std::mutex _mutex;

    /**
     * Default constructor that constructs an empty Map with
    */
    Map() {
        for (int i = 0; i < 16; i++) {
            _array.push_back(std::vector<Entry*>());
        }
    }

public:
    /**
     * Returns the number of key-value pairs in this map.
     */
    int get_size() { return _entrySet.size(); }

    /**
     * Put the given key value pair into the map
     * If the key is already associated with a value, the new value should overwrite the previous one
     * @return  val
     */
    void put(Object* key, Object* val) {
        _mutex.lock();

        _resizeIfNeeded();

        Entry* entry = _getEntry(key);
        if (entry) {
            entry->value = val;
            _mutex.unlock();

            return;
        }

        Entry* newEntry = new Entry(key, val);
        _entrySet.push_back(newEntry);
        _put(newEntry);

        _mutex.unlock();
    }

    /**
     * Resizes the map's internal storage if needed. The array is resized if the load factor is less than or
     * equal to the number of entries in the map plus one.
     * The load factor is calculated by 0.75 * capacity
     */
    void _resizeIfNeeded() {
        size_t resizeThreshold = (float)_array.size() * 0.75;
        if (resizeThreshold <= (_entrySet.size() + 1)) {
            for (size_t i = 0; i < _array.size(); i++) {
                _array[i].empty();
            }

            size_t size = _array.size();
            for (size_t i = 0; i < size; i++) {
                _array.push_back(std::vector<Entry*>());
            }

            for (size_t i = 0; i < _entrySet.size(); i++) {
                _put(_entrySet[i]);
            }
        }
    }

    /**
     * Puts the entry into the correct bucket. This does not add it to the entries array
     * @param entry The entry to add into a bucket
     */
    void _put(Entry* entry) {
        size_t bucketIndex = entry->hash % _array.size();
        _array[bucketIndex].push_back(entry);
    }

    /**
     * Returns the value of which the specified key is mapped to, or nullptr if this map does not contain the given key
     * @param key   the key whose associated value is to be returned
     * @return  the value mapped to the given key, or nullptr if the key is not found
     */
    Object* get(Object* key) {
        _mutex.lock();

        Entry* entry = _getEntry(key);
        _mutex.unlock();

        return entry ? entry->value : nullptr;
    }

    /**
     * Provides the entry for the object of the given key. If it is not found, nullptr is returned
     * @param key The key to get the entry of
     * @return The entry that represents the key
     */
    Entry* _getEntry(Object* key) {
        size_t hash = key->hash();
        size_t bucketIndex = hash % _array.size();

        std::vector<Entry*>& bucketArr =_array[bucketIndex];
        for (int i = 0; i < bucketArr.size(); i++) {
            Entry* entry = bucketArr[i];
            if (entry->key->equals(key)) {
                return entry;
            }
        }

        return nullptr;
    }

    /**
     * Returns true if this map contains the given key
     * @param key The key whose presence in this map is to be tested
     * @return  true if this map contains a mapping for the specified key, otherwise false
     */
    bool contains_key(Object* key) { return get(key) != nullptr; }

    /** Returns all of the entries in the map. These are of type Entry. Modifying this is undefined behavior. This method is potentially not thread safe */
    std::vector<Entry*>& entrySet() {
        return _entrySet;
    }
};
#endif
