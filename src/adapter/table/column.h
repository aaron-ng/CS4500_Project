#include <vector>

#include "../values/valuetypes.h"
#include "../values/value.h"

class Column {
    private:

        /** The values inside of the column */
        std::vector<Value*> values;

    public:

        /** The type of values that are stored inside of this column */
        const ValueType valueType;

        /**
         * Creates a new column that will store values of the given type
         * @param valueType The type of value that this column will store
         * @param reserve The number of spaces in the column to reserve
         */
        Column(ValueType valueType, size_t reserve = 0) : valueType(valueType) {
            values.reserve(reserve);
        }

        ~Column() {
            for (Value* value : values) { delete value; }
        }

        /**
         * Appends a new row to the column. This column will own the value and will delete it when the column deallocates
         * @param value The vale to append
         */
        void appendRow(Value* value) {
            values.emplace_back(value);
        }

        /**
         * Gets the value at the given row. Access is bounds checked
         * @param row The index of the row to provide the value for
         * @return The value at the given row
         */
        Value* get(size_t row) {
            if (row >= values.size() || row < 0) { throw std::runtime_error("Out of bounds"); }
            return values[row];
        }

        /**
         * Provides the number of entries in the column
         * @return The number of entries in the column
         */
        size_t count() const { return values.size(); }

        /**
         * Provides the types of values stored in this column
         * @return The type of value stored in this column
         */
        ValueType getValueType() const { return valueType; }

};