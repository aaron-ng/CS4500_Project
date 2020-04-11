#pragma once

#include <string.h>

#include "valueproducer.h"
#include "../../utils/column_type.h"
#include "../../utils/instructor-provided/string.h"
#include "../../dataframe/utils.h"

/** A value producer for a string */
class StringProducer: public ValueProducer {
    public:

        /**
         * Attempts to produce a value from the given string. If a value cannot be produce the behavior is undefined.
         * @param str The string to parse a value from
         * @param row The row to add the value to
         * @param idx The index in the row to add the value in
         * @return The resulting value or nullptr if a value could not be parsed
         */
        virtual void produce(const std::string& str, Row& row, size_t idx) override {
            String* string = nullptr;

            if (!str.empty()) {
                if (strHasQuotes(str)) {
                    string = new String(str.substr(1, str.length() - 2).c_str());
                } else if (str.find(' ') == std::string::npos) {
                    string = new String(str.c_str());
                } else {
                    // EMPTY
                }
            }

            row.set(idx, string);
        }

        /**
         * Returns true if a string value can be produced. If it has spaces it must have quotes.
         * @param str The string to check if it can be converted to a valid string
         * @return true if a string value can be created, false otherwise
         */
        virtual bool canProduce(const std::string& str) override {
            bool hasQuotes = strHasQuotes(str);
            bool hasSpace = str.find(' ') != std::string::npos;
            return !str.empty() && (!hasSpace || (hasSpace && hasQuotes));
        }

        /** This class produces string values */
        ColumnType producedType() const override { return STRING; }

    private:

        /**
         * Determines if a string has opening and closing quotes
         * @param str The string to check
         * @return true if the string has open and closing quotes, false otherwise
         */
        bool strHasQuotes(const std::string& str) const { return str.length() >= 2 && str[0] == '\"' && str[str.length() - 1] == '\"'; }
};

/**
 * Returns true if the given number has a sign and only digits. A decimal is allowed if allowDecimals is true
 * @param str The string to determine if it is a valid numner
 * @param allowDecimals true if decimals should be allowed
 * @return true if the string is a valid number, false otherwise
 */
inline bool isValidNumber(const std::string& str, bool allowDecimals = false) {
    if (str.size() < 1) {
        return false;
    }
    
    for (char c : str) {
        if (!isdigit(c) && c != '-' && c != '+' && (c != '.' || !allowDecimals)) { return false; }
    }
    return true;
}

/** A class that produces a DoubleValue */
class DoubleProducer : public ValueProducer {
    public:

        /**
         * Attempts to produce a value from the given string. If a value cannot be produce the behavior is undefined.
         * @param str The string to parse a value from
         * @param row The row to add the value to
         * @param idx The index in the row to add the value in
         * @return The resulting value or nullptr if a value could not be parsed
         */
        virtual void produce(const std::string& str, Row& row, size_t idx) override {
            if (!isValidNumber(str, true)) { /* EMPTY */ }
            try { row.set(idx, atof(str.c_str())); }
            catch (std::exception e) { /* EMPTY */ }
        }

        /**
         * Determines whether a float can be produced from the given string
         * @param str The string to check
         * @return true if a float can be produced, false otherwise
         */
        virtual bool canProduce(const std::string& str) override {
            if (!isValidNumber(str, true)) { return false; }
            try { atof(str.c_str()); return true; }
            catch (std::exception e) { return false; }
        }

        /** This class produces float values */
        ColumnType producedType() const override { return DOUBLE; }
};

/** A class that produces an IntValue */
class IntProducer : public ValueProducer {
    public:

        /**
         * Attempts to produce a value from the given string. If a value cannot be produce the behavior is undefined.
         * @param str The string to parse a value from
         * @param row The row to add the value to
         * @param idx The index in the row to add the value in
         * @return The resulting value or nullptr if a value could not be parsed
         */
        virtual void produce(const std::string& str, Row& row, size_t idx) override {
            if (!isValidNumber(str, false)) { /* EMPTY */ }
            try { return  row.set(idx, atoi(str.c_str())); }
            catch (std::exception e) { /* EMPTY */ }
        }

        /**
         * Determines whether a int can be produced from the given string
         * @param str The string to check
         * @return true if a int can be produced, false otherwise
         */
        virtual bool canProduce(const std::string& str) override {
            if (!isValidNumber(str, false)) { return false; }
            try { atoi(str.c_str()); return true; }
            catch (std::exception e) { return false; }
        }

        /** This class produces int values */
        ColumnType producedType() const override { return INT; }
};

/** A class that produces a BoolValue */
class BoolProducer: public ValueProducer {
    public:

        /**
         * Attempts to produce a value from the given string. If a value cannot be produce the behavior is undefined.
         * @param str The string to parse a value from
         * @param row The row to add the value to
         * @param idx The index in the row to add the value in
         * @return The resulting value or nullptr if a value could not be parsed
         */
        virtual void produce(const std::string& str, Row& row, size_t idx) override {
            if (str == "0") { row.set(idx, false); }
            if (str == "1") { row.set(idx, true); }
            // EMPTY
        }

        /**
         * Determines whether a int can be produced from the given string
         * @param str The string to check
         * @return true if a int can be produced, false otherwise
         */
        virtual bool canProduce(const std::string& str) override { return str == "0" || str == "1"; }

        /** This class produces bool values */
        ColumnType producedType() const override { return BOOL; }
};