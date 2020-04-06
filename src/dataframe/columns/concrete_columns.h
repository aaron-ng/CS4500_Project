#pragma once

#include "column.h"
#include "../../utils/column_type.h"
#include "../../utils/instructor-provided/object.h"
#include "../../utils/datastructures/element_column.h"
#include "../../utils/serial.h"

/**
 * Returns the number of elements inside the given chunk
 * @param idx The index of the chunk
 * @param totalSize The length of the entire column
 */
inline uint64_t chunkSize(size_t idx, size_t totalSize) {
    size_t remainingElements = totalSize - (Column::CHUNK_SIZE * idx);
    return remainingElements >= Column::CHUNK_SIZE ? Column::CHUNK_SIZE : remainingElements;
}

/**
 * Serializes a single chunk using the raw elements
 * @param serializer The serializer to serialize the chunk into
 * @param idx The index of the chunk to serialize
 */
inline void serializeChunkRawElement(Serializer& serializer, size_t idx, ElementColumn& column) {
    uint64_t elements = chunkSize(idx, column.size());

    serializer.write(elements);
    for (size_t i = 0; i < elements && idx * Column::CHUNK_SIZE + i < column.size(); i++) {
        serializer.write(*column.get(idx * Column::CHUNK_SIZE + i));
    }
}

/*************************************************************************
* IntColumn::
* Holds int values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class FullIntColumn : public IntColumn {
    public:

        /** Storage for the actual elements */
        ElementColumn _elements;

        FullIntColumn() = default;

        FullIntColumn(int n, ...) {
            va_list varArgs;
            va_start(varArgs, n);
            for (int i = 0; i < n; i++) {
                _elements.grow()->i = va_arg(varArgs, int);
            }
            va_end(varArgs);
        }

        /**
         * Gets the integer at the given index. Out of bounds is undefined behavior
         * @param idx The index of the element to get
         * @return The element at the given index
         */
        virtual int get(size_t idx) { return _elements.get(idx)->i; }

        /**
         * Pushes back a new integer onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(int val) { _elements.grow()->i = val; }

        /**
         * Returns this column as an IntColumn
         * @return This column as an IntColumn
         */
        virtual IntColumn* as_int() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        virtual void set(size_t idx, int val) { _elements.get(idx)->i = val; }

        /**
         * Returns the number of elements inside of this column
         * @return The number of elements inside of this column
         */
        virtual size_t size() { return _elements.size(); }

        /**
         * Provides the type of this column
         * @return ColumnType::INT
         */
        virtual char get_type() { return INT; }

        /** Clones this column. The resulting column will have all of the data inside of this column */
        virtual Object* clone() {
            IntColumn* newColumn = new FullIntColumn();
            for (size_t i = 0; i < _elements.size(); i++) { newColumn->push_back(_elements.get(i)->i); }
            return newColumn;
        }

        /**
         * Serialize a single chunk
         * @param serializer The serializer to serialize into
         * @param idx The index of the chunk to serialze
         */
        virtual void serializeChunk(Serializer &serializer, size_t idx) {
            serializeChunkRawElement(serializer, idx, _elements);
        }

};

/*************************************************************************
* BoolColumn::
* Holds bool values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class FullBoolColumn : public BoolColumn {
    public:

        /** Storage for the actual elements */
        ElementColumn _elements;

        FullBoolColumn() = default;

        FullBoolColumn(int n, ...) {
            va_list varArgs;
            va_start(varArgs, n);
            for (int i = 0; i < n; i++) {
                _elements.grow()->b = va_arg(varArgs, int);
            }
            va_end(varArgs);
        }

        /**
         * Gets the bool at the given index. Out of bounds is undefined behavior
         * @param idx The index of the element to get
         * @return The element at the given index
         */
        virtual bool get(size_t idx) { return _elements.get(idx)->b; }

        /**
         * Pushes back a new bool onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(bool val) { _elements.grow()->b = val; }

        /**
         * Returns this column as an BoolColumn
         * @return This column as an BoolColumn
         */
        virtual BoolColumn* as_bool() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        virtual void set(size_t idx, bool val) { _elements.get(idx)->b = val; }

        /**
         * Returns the number of elements inside of this column
         * @return The number of elements inside of this column
         */
        virtual size_t size() { return _elements.size(); }

        /**
         * Provides the type of this column
         * @return ColumnType::BOOL
         */
        virtual char get_type() { return BOOL; }

        /** Clones this column. The resulting column will have all of the data inside of this column */
        virtual Object* clone() {
            BoolColumn* newColumn = new FullBoolColumn();
            for (size_t i = 0; i < _elements.size(); i++) { newColumn->push_back(_elements.get(i)->b); }
            return newColumn;
        }

        /**
         * Serialize a single chunk
         * @param serializer The serializer to serialize into
         * @param idx The index of the chunk to serialze
         */
        virtual void serializeChunk(Serializer &serializer, size_t idx) {
            serializeChunkRawElement(serializer, idx, _elements);
        }

};

/*************************************************************************
* DoubleColumn::
* Holds bool values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class FullDoubleColumn : public DoubleColumn {
    public:

        /** Storage for the actual elements */
        ElementColumn _elements;

        FullDoubleColumn() = default;

        FullDoubleColumn(int n, ...) {
            va_list varArgs;
            va_start(varArgs, n);
            for (int i = 0; i < n; i++) {
                _elements.grow()->f = va_arg(varArgs, double);
            }
            va_end(varArgs);
        }

        /**
         * Gets the double at the given index. Out of bounds is undefined behavior
         * @param idx The index of the element to get
         * @return The element at the given index
         */
        virtual double get(size_t idx) { return _elements.get(idx)->f; }

        /**
         * Pushes back a new double onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(double val) { _elements.grow()->f = val; }

        /**
         * Returns this column as an DoubleColumn
         * @return This column as an DoubleColumn
         */
        virtual DoubleColumn* as_double() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        virtual void set(size_t idx, double val) { _elements.get(idx)->f = val; }

        /**
         * Returns the number of elements inside of this column
         * @return The number of elements inside of this column
         */
        virtual size_t size() { return _elements.size(); }

        /**
         * Provides the type of this column
         * @return ColumnType::DOUBLE
         */
        virtual char get_type() { return DOUBLE; }

        /** Clones this column. The resulting column will have all of the data inside of this column */
        virtual Object* clone() {
            DoubleColumn* newColumn = new FullDoubleColumn();
            for (size_t i = 0; i < _elements.size(); i++) { newColumn->push_back(_elements.get(i)->f); }
            return newColumn;
        }

        /**
         * Serialize a single chunk
         * @param serializer The serializer to serialize into
         * @param idx The index of the chunk to serialze
         */
        virtual void serializeChunk(Serializer &serializer, size_t idx) {
            serializeChunkRawElement(serializer, idx, _elements);
        }

};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class FullStringColumn : public StringColumn {
    public:

        /** Storage for the actual elements */
        ElementColumn _elements;

        FullStringColumn() = default;

        FullStringColumn(int n, ...) {
            va_list varArgs;
            va_start(varArgs, n);
            for (int i = 0; i < n; i++) {
                _elements.grow()->s = va_arg(varArgs, String*);
            }
            va_end(varArgs);
        }

        ~FullStringColumn() {
            for (size_t i = 0; i < _elements.size(); i++) {
                delete _elements.get(i)->s;
            }
        }

        /**
         * Returns this column as an StringColumn
         * @return This column as an StringColumn
         */
        virtual StringColumn* as_string() { return this; }

        /** Returns the string at idx; undefined on invalid idx.*/
        virtual String* get(size_t idx) { return _elements.get(idx)->s; }

        /**
         * Pushes back a new string onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(String* val) { _elements.grow()->s = val; }

        /** Acquire ownership fo the string.  Out of bound idx is undefined. */
        virtual void set(size_t idx, String* val) { _elements.get(idx)->s = val; }

        /**
         * Returns the number of elements inside of this column
         * @return The number of elements inside of this column
         */
        virtual size_t size() { return _elements.size(); }

        /**
         * Provides the type of this column
         * @return ColumnType::STRING
         */
        virtual char get_type() { return STRING; }

        /** Clones this column. The resulting column will have all of the data inside of this column, however since
         *  this column owns the strings inside of it, the strings will all be copied
         */
        virtual Object* clone() {
            StringColumn* newColumn = new FullStringColumn();
            for (size_t i = 0; i < _elements.size(); i++) {
                String* string = _elements.get(i)->s;
                newColumn->push_back(string ? string->clone() : string);
            }
            return newColumn;
        }

        /**
         * Serialize a single chunk
         * @param serializer The serializer to serialize into
         * @param idx The index of the chunk to serialze
         */
        void serializeChunk(Serializer &serializer, size_t idx) override {
            uint64_t elements = chunkSize(idx, _elements.size());

            serializer.write(elements);
            for (size_t i = 0; i < elements && idx * Column::CHUNK_SIZE + i < size(); i++) {
                serializer.write(_elements.get(idx * Column::CHUNK_SIZE + i)->s);
            }
        }
};