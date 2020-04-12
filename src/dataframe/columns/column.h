#pragma once

class IntColumn;
class BoolColumn;
class DoubleColumn;
class StringColumn;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality.
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Column : public Object {
    public:

        /** The number of elements in one chunk */
        static const size_t CHUNK_SIZE = 2500000;

        virtual ~Column() {}

        /** Type converters: Return same column under its actual type, or
         *  nullptr if of the wrong type.  */
        virtual IntColumn* as_int() { return nullptr; }
        virtual BoolColumn*  as_bool() { return nullptr; }
        virtual DoubleColumn* as_double() { return nullptr; }
        virtual StringColumn* as_string() { return nullptr; }

        /** Type appropriate push_back methods. Calling the wrong method is
          * undefined behavior. **/
        virtual void push_back(int val) {}
        virtual void push_back(bool val) {}
        virtual void push_back(double val) {}
        virtual void push_back(String* val) {}

        /** Returns the number of elements in the column. */
        virtual size_t size() = 0;

        /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
        virtual char get_type() { return '\0'; }

        /** Returns the number of chunks inside of this column */
        size_t numChunks() { return size() / CHUNK_SIZE + (size() % CHUNK_SIZE ? 1 : 0); }

        /**
         * Serializes a single chunk
         * @param serializer The serializer to serialize the chunk into
         * @param idx The index of the chunk to serialize
         */
        virtual void serializeChunk(Serializer& serializer, size_t idx) { /** By default do nothing */ }

};

/*************************************************************************
* IntColumn::
* Holds int values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class IntColumn : public Column {
    public:

        /**
         * Gets the integer at the given index. Out of bounds is undefined behavior
         * @param idx The index of the element to get
         * @return The element at the given index
         */
        virtual int get(size_t idx) = 0;

        /**
         * Pushes back a new integer onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(int val) = 0;

        /**
         * Returns this column as an IntColumn
         * @return This column as an IntColumn
         */
        virtual IntColumn* as_int() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        virtual void set(size_t idx, int val) = 0;

        /**
         * Returns the number of elements inside of this column
         * @return The number of elements inside of this column
         */
        virtual size_t size() = 0;

        /**
         * Provides the type of this column
         * @return ColumnType::INT
         */
        virtual char get_type() { return INT; }

};

/*************************************************************************
* BoolColumn::
* Holds bool values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class BoolColumn : public Column {
    public:

        /**
         * Gets the bool at the given index. Out of bounds is undefined behavior
         * @param idx The index of the element to get
         * @return The element at the given index
         */
        virtual bool get(size_t idx) = 0;

        /**
         * Pushes back a new bool onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(bool val) = 0;

        /**
         * Returns this column as an BoolColumn
         * @return This column as an BoolColumn
         */
        virtual BoolColumn* as_bool() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        virtual void set(size_t idx, bool val) = 0;

        /**
         * Returns the number of elements inside of this column
         * @return The number of elements inside of this column
         */
        virtual size_t size() = 0;

        /**
         * Provides the type of this column
         * @return ColumnType::BOOL
         */
        virtual char get_type() { return BOOL; }

};

/*************************************************************************
* DoubleColumn::
* Holds bool values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class DoubleColumn : public Column {
    public:

        /**
         * Gets the double at the given index. Out of bounds is undefined behavior
         * @param idx The index of the element to get
         * @return The element at the given index
         */
        virtual double get(size_t idx) = 0;

        /**
         * Pushes back a new double onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(double val) = 0;

        /**
         * Returns this column as an DoubleColumn
         * @return This column as an DoubleColumn
         */
        virtual DoubleColumn* as_double() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        virtual void set(size_t idx, double val) = 0;

        /**
         * Returns the number of elements inside of this column
         * @return The number of elements inside of this column
         */
        virtual size_t size() = 0;

        /**
         * Provides the type of this column
         * @return ColumnType::DOUBLE
         */
        virtual char get_type() { return DOUBLE; }

};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class StringColumn : public Column {
    public:

        /**
         * Returns this column as an StringColumn
         * @return This column as an StringColumn
         */
        virtual StringColumn* as_string() { return this; }

        /** Returns the string at idx; undefined on invalid idx.*/
        virtual String* get(size_t idx) = 0;

        /**
         * Pushes back a new string onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(String* val) = 0;

        /** Acquire ownership fo the string.  Out of bound idx is undefined. */
        virtual void set(size_t idx, String* val) = 0;

        /**
         * Returns the number of elements inside of this column
         * @return The number of elements inside of this column
         */
        virtual size_t size() = 0;

        /**
         * Provides the type of this column
         * @return ColumnType::STRING
         */
        virtual char get_type() { return STRING; }
};