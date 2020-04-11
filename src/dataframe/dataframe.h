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
#include "columns/concrete_columns.h"
#include "columns/chunked_column.h"
#include "../adapter/sor/dataadapter.h"
#include "utils.h"

/**
 * Creates a new column of the given type. If the type is invalid, nullptr is returned
 * @param type The type of column to create
 * @return A new column of the given type
 */
inline Column* allocateColumnOfType(char type) {
    switch (type) {
        case INT: return new FullIntColumn();
        case BOOL: return new FullBoolColumn();
        case DOUBLE: return new FullDoubleColumn();
        case STRING: return new FullStringColumn();
        default: return nullptr;
    }
}

/**
 * Creates a new column that will load chunks of data from different keys of the given type
 * @param type The type of column to create
 * @param keys The list of keys to use for chunks
 * @param chunkCount The number of chunks
 * @param kbstore The kbstore to load the chunks from
 * @param totalSize The number of elements inside of the entire column
 */
inline Column* allocateChunkedColumnOfType(char type, Key** keys, size_t chunkCount, KBStore &kbstore, size_t totalSize) {
    switch (type) {
        case INT: return new ChunkedIntColumn(keys, chunkCount, kbstore, totalSize);
        case BOOL: return new ChunkedBoolColumn(keys, chunkCount, kbstore, totalSize);
        case DOUBLE: return new ChunkedDoubleColumn(keys, chunkCount, kbstore, totalSize);
        case STRING: return new ChunkedStringColumn(keys, chunkCount, kbstore, totalSize);
        default: return nullptr;
    }
}

/*******************************************************************************
 * Writer class to write over each row
 * Written by ng.h@husky.neu.edu & pazol.l@husky.neu.edu
 */
class Writer {
public:
    /** Reads next word and stores it in the row. Actually read the word.
        While reading the word, we may have to re-fill the buffer  */
    virtual void visit(Row & r) {}

    /** Returns true when there are no more words to read.  There is nothing
       more to read if we are at the end of the buffer and the file has
       all been read.     */
    virtual bool done() { return false; }
};

/**
 * Reader class to read over each row
 * Written by ng.h@husky.neu.edu & pazol.l@husky.neu.edu
 */
class Reader {
public:
    /** Reads the next word */
    virtual bool visit(Row & r) { return false; }
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
                _columns.grow()->c = dynamic_cast<Column*>(col);
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
     * Creates a dataframe from a vistor and uploads it to the KV store.
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     * @param charSchema The schema of the dataframe
     * @param writer The visitor to use to create the dataframe
     */
    static void fromVisitor(Key* key, KVStore* kv, const char* charSchema, Writer* writer) {
        Schema schema(charSchema);
        Row row(schema);
        fromLambda(key, kv, charSchema, [&](DataFrame* df) {
            writer->visit(row);
            df->add_row(row);
        }, [&]{ return !writer->done(); });
    }

    /**
     * Creates a dataframe from an SOR file and puts it in the store
     * @param name The name of the file to read in
     * @param key The key to store the dataframe under
     * @param kv The key value store to put the dataframe in
     */
    static void fromFile(const char* name, Key* key, KVStore* kv) {
        FILE* file = fopen(name, "r");
        if (!file) {
            std::cout << "File not found: " << name << std::endl;
            exit(15);
        }

        DataAdapter adapter;
        Schema schema = adapter.determineSchema(file);
        Row row(schema);

        bool more = true;
        fromLambda(key, kv, schema._types, [&](DataFrame* df){
            more = adapter.read(row, file, schema);
            df->add_row(row);
        }, [&] { return more; });
    }

    /**
     * Creates a dataframe using a lambda to populate single rows
     * @param key The key to store the dataframe under
     * @param kv The key value store to store the dataframe in
     * @param schema The schema of the dataframe
     * @param populate A lambda that adds a single row to the given dataframe
     * @param hasMore A lambda that returns true if there is more data to read
     */
    static void fromLambda(Key* key, KVStore* kv, const char* schema, std::function<void(DataFrame*)> populate, std::function<bool()> hasMore) {
        Schema s(schema);
        DataFrame* dataFrame = new DataFrame(s);

        size_t nodes = kv->_byteStore.nodes();
        size_t chunks = 0;
        size_t rows = 0;

        while (hasMore()) {
            populate(dataFrame);
            rows++;

            if (dataFrame->nrows() == Column::CHUNK_SIZE) {
                kv->putDataframeChunk(*key, dataFrame, chunks, nodes, 0);
                chunks++;

                delete dataFrame;
                dataFrame = new DataFrame(s);
            }
        }

        if (dataFrame->nrows()) {
            kv->putDataframeChunk(*key, dataFrame, chunks, nodes, 0);
            chunks++;
        }

        delete dataFrame;

        // Generate the description
        size_t columns = s.width();
        ColumnDescription** descriptions = new ColumnDescription*[columns];

        for (size_t i = 0; i < columns; i++) {
            Key** chunkKeys = new Key*[chunks];

            for (size_t chunk = 0; chunk < chunks; chunk++) { chunkKeys[chunk] = kv->_keyFor(*key, i, chunk, nodes); }
            descriptions[i] = new ColumnDescription(chunkKeys, chunks, rows, (ColumnType)s.col_type(i));
        }

        DataframeDescription* desc = new DataframeDescription(new String(schema), columns, descriptions);
        kv->putDataframeDesc(*key, desc);
        delete desc;
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

    /** Visit rows in order */
    void map(Reader& r) {
        Row row(_schema);
        for (size_t idx = 0; idx < nrows(); idx++) {
            _fillRow(row, idx);
            r.visit(row);
        }
    }

    /** Visits rows in order if they are stored on this machine */
    void local_map(Reader& r) {
        Column* column = getColumn(0);
        ChunkedColumn* chunkedCol = dynamic_cast<ChunkedColumn*>(column);

        Row row(get_schema());
        if (chunkedCol != nullptr) {
            for (size_t i = 0; i < chunkedCol->_chunkCount; i++) {
                Key* currKey = chunkedCol->_keys[i];
                if (chunkedCol->isKeyLocal(currKey)) {
                    for (size_t idx = 0; idx < Column::CHUNK_SIZE && i * Column::CHUNK_SIZE + idx < column->size(); idx++) {
                        _fillRow(row, i * Column::CHUNK_SIZE + idx);
                        r.visit(row);
                    }
                }
            }
        }
    }

};