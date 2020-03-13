#pragma once

/** A union that is used to store all of the kinds of data that columns need to support
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
union Element {
    int i;
    float f;
    bool b;
    class ServerClientInfo* s;
};

/**
 * A column that stores elements in chunks such that the elements are never copied when resizing the storage.
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ElementColumn {
public:
    /** The number of elements inside each chunk */
    static const size_t _CHUNK_SIZE = 512;

    /** The number of elements inside of the column */
    size_t _size = 0;

    /** The number of chunks inside of the column */
    size_t _numChunks = 1;

    /** The storage for all of the chunks */
    Element** _storage = nullptr;

    /**
     * Creates a column that stores elements
     */
    ElementColumn() {
        _storage = new Element*[1];
        _storage[0] = new Element[_CHUNK_SIZE];
    }

    ~ElementColumn() {
        for (size_t i = 0; i < _numChunks; i++) {
            delete[] _storage[i];
        }

        delete _storage;
    }

    /**
     * Allocates the space for a new chunk if all of the existing chunks are full
     */
    void _addColumnIfNeeded() {
        if (_size / _CHUNK_SIZE == _numChunks) {
            Element** newStorage = new Element*[_numChunks + 1];
            newStorage[_numChunks] = new Element[_CHUNK_SIZE];
            memcpy(newStorage, _storage, sizeof(Element*) * _numChunks);
            _storage = newStorage;
            _numChunks++;
        }
    }

    /**
     * Grows the column by one element
     * @return The memory address of the new element
     */
    Element* grow() {
        _addColumnIfNeeded();
        _size++;
        return get(_size - 1);
    }

    /**
     * Provides the memory location of the element at the given index. Index out of bounds leads to undefined behavior
     * @param i The index of the element to retrieve
     * @return The memory location of the given element
     */
    Element* get(size_t i) const { return &_storage[i / _CHUNK_SIZE][i % _CHUNK_SIZE]; }

    /**
     * Provides the number of elements inside of the column
     * @return The number of elements inside of the column
     */
    size_t size() const { return _size; }

};
