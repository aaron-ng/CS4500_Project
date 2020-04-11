#pragma once

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