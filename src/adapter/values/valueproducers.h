#pragma once

#include <string.h>

#include "valueproducer.h"
#include "values.h"
#include "value.h"

/** A value producer for a string */
class StringProducer: public ValueProducer {
    public:

        /**
         * Produces a new string or nullptr if it cannot be produced. If there are spaces in the string, quotes must be the first
         * and last char of str.
         * @param str The string to produce a new string from. This assumes there is no leading or trailing whitespace.
         * @return A StringValue or nullptr if one could not be parsed
         */
        Value* produce(const std::string& str) override {
            if (!str.empty()) {
                if (strHasQuotes(str)) {
                    return new StringValue(str.substr(1, str.length() - 2));
                } else if (str.find(' ') == std::string::npos) {
                    return new StringValue(str);
                } else {
                    return new EmptyValue();
                }
            }

            return nullptr;
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
        ValueType producedType() const override { return STRING; }

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
bool isValidNumber(const std::string& str, bool allowDecimals = false) {
    if (str.size() < 1) {
        return false;
    }
    
    for (char c : str) {
        if (!isdigit(c) && c != '-' && c != '+' && (c != '.' || !allowDecimals)) { return false; }
    }
    return true;
}

/** A class that produces a FloatValue */
class FloatProducer : public ValueProducer {
    public:

        /**
         * Produces a new float value if possible. Floats can have a leading positive and negative sign,
         * one decimal with no spaces. If a float could not be parsed, nullptr is returned
         * @param str The string to parse the float from
         * @return the parsed float value, nullptr otherwise
         */
        Value* produce(const std::string& str) override {
            if (!isValidNumber(str, true)) { return nullptr; }
            try { return new FloatValue(atof(str.c_str())); }
            catch (std::exception e) { return nullptr; }
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
        ValueType producedType() const override { return FLOAT; }
};

/** A class that produces an IntValue */
class IntProducer : public ValueProducer {
    public:

        /**
         * Produces a new int value if possible. Ints can have a leading positive and negative sign and no spaces.
         * If a int could not be parsed, nullptr is returned
         * @param str The string to parse the int from
         * @return the parsed int value, nullptr otherwise
         */
        Value* produce(const std::string& str) override {
            if (!isValidNumber(str, false)) { return nullptr; }
            try { return new IntValue(atoi(str.c_str())); }
            catch (std::exception e) { return nullptr; }
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
        ValueType producedType() const override { return INT; }
};

/** A class that produces a BoolValue */
class BoolProducer: public ValueProducer {
    public:

        /**
         * Produces a new bool value if the given string is 0 or 1, otherwise returns nullptr
         * @param str The string to produce a bool from
         * @return A BoolValue or nullptr if the string was not a bool
         */
        Value* produce(const std::string& str) override {
            if (str == "0") { return new BoolValue(0); }
            if (str == "1") { return new BoolValue(1); }
            return nullptr;
        }

        /**
         * Determines whether a int can be produced from the given string
         * @param str The string to check
         * @return true if a int can be produced, false otherwise
         */
        virtual bool canProduce(const std::string& str) override { return str == "0" || str == "1"; }

        /** This class produces bool values */
        ValueType producedType() const override { return BOOL; }
};