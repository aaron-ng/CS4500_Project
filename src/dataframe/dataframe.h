#pragma once

#include <iostream>
#include <iomanip>
#include <cstdarg>
#include <thread>

#include "../utils/column_type.h"
#include "../utils/instructor-provided/object.h"
#include "../utils/datastructures/element_column.h"
#include "../utils/instructor-provided/string.h"
#include "../ea2/kvstore/kvstore.h"
#include "../network/shared/messages.h"

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
class Column : public Object, public Codable {
public:
    /** Storage for the actual elements */
    ElementColumn _elements;

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

    /**
     * Writes this column out to a buffer
     * @param serializer The serializer to write data with
     */
    virtual void serialize(Serializer& serializer) {
        serializer.write((uint64_t)_elements.size());
        for (size_t i = 0; i < _elements.size(); i++) {
            serializer.write(*_elements.get(i));
        }
    };

    /**
     * Reads this column from a buffer
     * @param data The deserializer to read data from
     */
    virtual void deserialize(Deserializer& deserializer) {
        uint64_t count = deserializer.read_uint64();
        for (size_t i = 0; i < count; i++) {
            *_elements.grow() = deserializer.read_element();
        }
    };

};

/*************************************************************************
* IntColumn::
* Holds int values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class IntColumn : public Column {
    public:

        IntColumn() = default;

        IntColumn(int n, ...) {
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
        int get(size_t idx) { return _elements.get(idx)->i; }

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
        void set(size_t idx, int val) { _elements.get(idx)->i = val; }

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
            IntColumn* newColumn = new IntColumn();
            for (size_t i = 0; i < _elements.size(); i++) { newColumn->push_back(_elements.get(i)->i); }
            return newColumn;
        }

};

/*************************************************************************
* BoolColumn::
* Holds bool values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class BoolColumn : public Column {
public:

    BoolColumn() = default;

    BoolColumn(int n, ...) {
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
    bool get(size_t idx) { return _elements.get(idx)->b; }

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
    void set(size_t idx, bool val) { _elements.get(idx)->b = val; }

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
        BoolColumn* newColumn = new BoolColumn();
        for (size_t i = 0; i < _elements.size(); i++) { newColumn->push_back(_elements.get(i)->b); }
        return newColumn;
    }

};

/*************************************************************************
* DoubleColumn::
* Holds bool values.
* Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
*/
class DoubleColumn : public Column {
public:

    DoubleColumn() = default;

    DoubleColumn(int n, ...) {
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
    double get(size_t idx) { return _elements.get(idx)->f; }

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
    void set(size_t idx, double val) { _elements.get(idx)->f = val; }

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
        DoubleColumn* newColumn = new DoubleColumn();
        for (size_t i = 0; i < _elements.size(); i++) { newColumn->push_back(_elements.get(i)->f); }
        return newColumn;
    }

};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class StringColumn : public Column {
    public:

        StringColumn() = default;

        StringColumn(int n, ...) {
            va_list varArgs;
            va_start(varArgs, n);
            for (int i = 0; i < n; i++) {
                _elements.grow()->s = va_arg(varArgs, String*);
            }
            va_end(varArgs);
        }

        ~StringColumn() {
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
        String* get(size_t idx) { return _elements.get(idx)->s; }

        /**
         * Pushes back a new string onto the column
         * @param val The value to add to the end of the column
         */
        virtual void push_back(String* val) { _elements.grow()->s = val; }

        /** Acquire ownership fo the string.  Out of bound idx is undefined. */
        void set(size_t idx, String* val) { _elements.get(idx)->s = val; }

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
            StringColumn* newColumn = new StringColumn();
            for (size_t i = 0; i < _elements.size(); i++) {
                String* string = _elements.get(i)->s;
                newColumn->push_back(string ? string->clone() : string);
            }
            return newColumn;
        }

        /**
         * Writes this column out to a buffer
         * @param serializer The serializer to write data with
         */
        virtual void serialize(Serializer& serializer) {
            serializer.write((uint64_t)_elements.size());
            for (size_t i = 0; i < _elements.size(); i++) {
                serializer.write(_elements.get(i)->s);
            }
        };

        /**
         * Reads this column from a buffer
         * @param data The deserializer to read data from
         */
        virtual void deserialize(Deserializer& deserializer) {
            uint64_t count = deserializer.read_uint64();
            for (size_t i = 0; i < count; i++) {
                _elements.grow()->s = deserializer.read_string();
            }
        };
};

/**
 * Creates a new column of the given type. If the type is invalid, nullptr is returned
 * @param type The type of column to create
 * @return A new column of the given type
 */
inline Column* allocateColumnOfType(char type) {
    switch (type) {
        case INT: return new IntColumn();
        case BOOL: return new BoolColumn();
        case DOUBLE: return new DoubleColumn();
        case STRING: return new StringColumn();
        default: return nullptr;
    }
}

/*************************************************************************
* Schema::
* A schema is a description of the contents of a data frame, the schema
* knows the number of columns and number of rows, the type of each column,
* optionally columns and rows can be named by strings.
* The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
*/
class Schema : public Object {
public:

    /** The storage for the column names */
    ElementColumn _columnNames;

    /** The types of all of the columns */
    char* _types = nullptr;

    /** Copying constructor. This will not copy the row names */
    Schema(const Schema& from) {
        _types = duplicate(from._types);
        for (size_t i = 0; i < from._columnNames.size(); i++) {
            String* name = from._columnNames.get(i)->s;
            _columnNames.grow()->s = name ? name->clone() : name;
        }
    }

    /** Create an empty schema **/
    Schema() {
        _types = new char[1];
        _types[0] = '\0';
    }

    /** Create a schema from a string of types. A string that contains
      * characters other than those identifying the four type results in
      * undefined behavior. The argument is external, a nullptr argument is
      * undefined. **/
    Schema(const char* types) {
        _types = duplicate(types);
        for (size_t i = 0; i < strlen(types); i++) {
            _columnNames.grow()->s = nullptr;
        }
    }

    ~Schema() {
        delete [] _types;

        for (size_t i = 0; i < _columnNames.size(); i++) {
            if (_columnNames.get(i)) {
                delete _columnNames.get(i)->s;
            }
        }
    }

    /** Add a column of the given type and name (can be nullptr), name
      * is external. Names are expectd to be unique, duplicates result
      * in undefined behavior. */
    void add_column(char typ, String* name) {
        _columnNames.grow()->s = name ? name->clone() : name;

        char* newTypes = new char[_columnNames.size() + 1];
        strcpy(newTypes, _types);
        newTypes[_columnNames.size() - 1] = typ;
        newTypes[_columnNames.size()] = '\0';

        delete[] _types;
        _types = newTypes;
    }

    /** Return name of column at idx; nullptr indicates no name given.
      *  An idx >= width is undefined.*/
    String* col_name(size_t idx) { return _columnNames.get(idx)->s; }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) { return _types[idx]; }

    /** Given a column name return its index, or -1. If nullptr is given, the result will always be -1 */
    int col_idx(const char* name) {
        if (!name) { return -1; }
        for (size_t i = 0; i < _columnNames.size(); i++) {
            String* nameI = _columnNames.get(i)->s;
            if (nameI && !strcmp(nameI->c_str(), name)) { return i; }
        }
        return -1;
    }

    /** The number of columns */
    size_t width() { return _columnNames.size(); }

    /** Gets all of the types in the schema as a char array */
    const char* types() const { return _types; }
};

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object {
public:

    /** The index of this row */
    size_t _idx = 0;

    /** The entries that are inside of the row */
    Element* _entries;

    /** The schema for this row */
    Schema _schema;

    /** Build a row following a schema. */
    Row(Schema& scm) : _schema(scm) {
        _entries = new Element[scm.width()];
    }

    ~Row() { delete[] _entries; }

    /** Setters: set the given column with the given value. Setting a column with
      * a value of the wrong type is undefined. */
    void set(size_t col, int val) { _entries[col].i = val; }
    void set(size_t col, double val) { _entries[col].f = val; }
    void set(size_t col, bool val) { _entries[col].b = val; }
    /** Acquire ownership of the string. */
    void set(size_t col, String* val) { _entries[col].s = val; }

    /** Set/get the index of this row (ie. its position in the dataframe. This is
     *  only used for informational purposes, unused otherwise */
    void set_idx(size_t idx) { _idx = idx; }
    size_t get_idx() { return _idx; }

    /** Getters: get the value at the given column. If the column is not
      * of the requested type, the result is undefined. */
    int get_int(size_t col) { return _entries[col].i; }
    bool get_bool(size_t col) { return _entries[col].b; }
    double get_double(size_t col) { return _entries[col].f; }
    String* get_string(size_t col) { return _entries[col].s; }

    /** Number of fields in the row. */
    size_t width() { return _schema.width(); }

    /** Type of the field at the given position. An idx >= width is  undefined. */
    char col_type(size_t idx) { return _schema.col_type(idx); }

};

/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object {
public:
    /** This method is called once per row. The row object is on loan and
        should not be retained as it is likely going to be reused in the next
        call. The return value is used in filters to indicate that a row
        should be kept. */
    virtual bool accept(Row& r) = 0;

    /** Once traversal of the data frame is complete the rowers that were
        split off will be joined.  There will be one join per split. The
        original object will be the last to be called join on. The join method
        is reponsible for cleaning up memory. */
    virtual void join_delete(Rower* other) { delete other; };
};

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object {
public:

    /** The schema of the dataframe */
    Schema _schema;

    /** The collection of column objects */
    ElementColumn _columns;

    /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
    DataFrame(DataFrame& df) : DataFrame(df._schema) {}

    /** Create a data frame from a schema and columns. All columns are created
      * empty. */
    DataFrame(Schema& schema) : _schema(schema) {
        for (size_t i = 0; i < _schema.width(); i++) {
            Column* column = allocateColumnOfType(_schema.col_type(i));
            _columns.grow()->c = column;
        }
    }

    ~DataFrame() {
        for (size_t i = 0; i < _columns.size(); i++) {
            delete _columns.get(i)->c;
        }
    }

    /** Returns the dataframe's schema. Modifying the schema after a dataframe
      * has been created in undefined. */
    Schema& get_schema() { return _schema; }

    /** Adds a column this dataframe, updates the schema, the new column
      * is external, and appears as the last column of the dataframe, the
      * name is optional and external. A nullptr colum is undefined.
      * If there are already columns that have rows and the new column has a different number of rows, it is discarded
      */
    void add_column(Column* col, String* name) {
        if (!name || _schema.col_idx(name->c_str()) == -1) {
            if (_schema.width() == 0 || col->size() == nrows()) {
                _schema.add_column(col->get_type(), name);
                _columns.grow()->c = dynamic_cast<Column*>(col->clone());
            }
        }
    }

    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds, or request the wrong type is undefined.*/
    int get_int(size_t col, size_t row) { return getColumn(col)->as_int()->get(row); }
    bool get_bool(size_t col, size_t row) { return getColumn(col)->as_bool()->get(row); }
    double get_double(size_t col, size_t row) { return getColumn(col)->as_double()->get(row); }
    String*  get_string(size_t col, size_t row) { return getColumn(col)->as_string()->get(row); }

    /** Return the offset of the given column name or -1 if no such col. */
    int get_col(String& col) { return _schema.col_idx(col.c_str()); }

    /** Set the value at the given column and row to the given value.
      * If the column is not  of the right type or the indices are out of
      * bound, the result is undefined. */
    void set(size_t col, size_t row, int val) { getColumn(col)->as_int()->set(row, val); }
    void set(size_t col, size_t row, bool val) { getColumn(col)->as_bool()->set(row, val); }
    void set(size_t col, size_t row, double val) { getColumn(col)->as_double()->set(row, val); }
    void set(size_t col, size_t row, String* val) { getColumn(col)->as_string()->set(row, val); }

    /** Set the fields of the given row object with values from the columns at
      * the given offset.  If the row is not form the same schema as the
      * dataframe, results are undefined.
      */
    void fill_row(size_t idx, Row& row) {
        for (size_t i = 0; i < _schema.width(); i++) {
            switch (_schema.col_type(i)) {
                case INT:
                    set(i, idx, row.get_int(i));
                    break;
                case BOOL:
                    set(i, idx, row.get_bool(i));
                    break;
                case DOUBLE:
                    set(i, idx, row.get_double(i));
                    break;
                case STRING:
                    set(i, idx, row.get_string(i));
                    break;
                default:
                    continue;
            }
        }
    }

    /** Add a row at the end of this dataframe. The row is expected to have
     *  the right schema and be filled with values, otherwise undedined.  */
    void add_row(Row& row) {
        for (size_t i = 0; i < _schema.width(); i++) {
            Column* col = getColumn(i);
            switch (_schema.col_type(i)) {
                case INT:
                    col->as_int()->push_back(row.get_int(i));
                    break;
                case BOOL:
                    col->as_bool()->push_back(row.get_bool(i));
                    break;
                case DOUBLE:
                    col->as_double()->push_back(row.get_double(i));
                    break;
                case STRING:
                    col->as_string()->push_back(row.get_string(i));
                    break;
                default:
                    continue;
            }
        }
    }

    /** The number of rows in the dataframe. */
    size_t nrows() { return getColumn(0)->size(); }

    /** The number of columns in the dataframe.*/
    size_t ncols() { return _columns.size(); }

    /**
     * Creates a new dataframe from one value. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param value The value to put into the dataframe
     */
    static void fromScalar(Key* key, KVStore* kv, int value) {
        _fromScalar(key, kv, value, INT);
    }

    /**
     * Creates a new dataframe from one value. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param value The value to put into the dataframe
     */
    static void fromScalar(Key* key, KVStore* kv, bool value) {
        _fromScalar(key, kv, value, BOOL);
    }

    /**
     * Creates a new dataframe from one value. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param value The value to put into the dataframe
     */
    static void fromScalar(Key* key, KVStore* kv, double value) {
        _fromScalar(key, kv, value, DOUBLE);
    }

    /**
     * Creates a new dataframe from one value. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param value The value to put into the dataframe
     */
    static void fromScalar(Key* key, KVStore* kv, String* value) {
        // Copy the string because the dataframe will own it and delete it
        String* copy = value->clone();
        _fromScalar(key, kv, copy, STRING);
    }

    /**
     * Creates a new dataframe from one value. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param value The value to put into the dataframe
     * @param c The type to use for the schema
     */
    template <typename T>
    static void _fromScalar(Key* key, KVStore* kv, T value, ColumnType c) {
        const char charSchema[2] = {(char)c, '\0'};
        Schema schema(charSchema);
        DataFrame dataFrame(schema);
        Row row(schema);
        row.set(0, value);

        dataFrame.add_row(row);
        kv->put(&dataFrame, *key);
    }

    /**
     * Creates a new dataframe from an array of values. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param count The number of items in values
     * @param values The values to put into the dataframe
     */
    static void fromArray(Key* key, KVStore* kv, size_t count, int* values) {
        _fromArray(key, kv, count, values, INT);
    }

    /**
     * Creates a new dataframe from an array of values. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param count The number of items in values
     * @param values The values to put into the dataframe
     */
    static void fromArray(Key* key, KVStore* kv, size_t count, bool* values) {
        _fromArray(key, kv, count, values, BOOL);
    }

    /**
     * Creates a new dataframe from an array of values. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param count The number of items in values
     * @param values The values to put into the dataframe
     */
    static void fromArray(Key* key, KVStore* kv, size_t count, double* values) {
        _fromArray(key, kv, count, values, DOUBLE);
    }

    /**
     * Creates a new dataframe from an array of values. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param count The number of items in values
     * @param values The values to put into the dataframe
     */
    static void fromArray(Key* key, KVStore* kv, size_t count, String** values) {
        // Copy the values since the dataframe will own the strings and delete them once its in the store
        String** copy = new String*[count];
        for (size_t i = 0; i < count; i++) {
            copy[i] = values[i]->clone();
        }

        _fromArray(key, kv, count, copy, STRING);
        delete[] copy;
    }

    /**
     * Creates a new dataframe from an array of values. The resulting dataframe will have one column
     * and be stored in the KV store under the given key
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param count The number of items in values
     * @param values The values to put into the dataframe
     * @param c The type to use for the schema
     */
    template <typename T>
    static void _fromArray(Key* key, KVStore* kv, size_t count, T* values, ColumnType c) {
        const char charSchema[2] = {(char)c, '\0'};
        Schema schema(charSchema);
        DataFrame dataFrame(schema);
        Row row(schema);

        for (int i = 0; i < count; i++) {
            row.set(0, values[i]);
            dataFrame.add_row(row);
        }
        kv->put(&dataFrame, *key);
    }

    /**
     * Fills a row with the data that is contained in this data frame at the given row
     * @param row The row to fill with data
     * @param idx The index of the row who's data should be put inside the row
     */
    void _fillRow(Row& row, size_t idx) {
        row.set_idx(idx);

        // Fill the row with data
        for (size_t i = 0; i < _schema.width(); i++) {
            Column* col = getColumn(i);
            switch (_schema.col_type(i)) {
                case INT: {
                    IntColumn* intColumn = col->as_int();
                    if (intColumn) { row.set(i, intColumn->get(idx)); }
                    break;
                }
                case BOOL: {
                    BoolColumn* boolColumn = col->as_bool();
                    if (boolColumn) { row.set(i, boolColumn->get(idx)); }
                    break;
                }
                case DOUBLE: {
                    DoubleColumn* doubleColumn = col->as_double();
                    if (doubleColumn) { row.set(i, doubleColumn->get(idx)); }
                    break;
                }
                case STRING: {
                    StringColumn* stringColumn = col->as_string();
                    if (stringColumn) { row.set(i, stringColumn->get(idx)); }
                    break;
                }
                default:
                    continue;
            }
        }
    }

    /**
     * Provides the column at the given index. Modifying the column is undefined behavior
     * @param index The index of the given column
     * @return The element in _columns at the given index as a column
     */
    Column* getColumn(size_t index) { return _columns.get(index)->c; }

};
