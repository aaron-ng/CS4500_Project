#pragma once

#include "value.h"

class StringValue : public Value {
    private:

        /** The buffer where the contents of the string is stored */
        const char* buffer = nullptr;

    public:

        /**
         * Creates a new string value. A copy of str will be made
         * @param str The contents of the string
         */
        StringValue(const std::string& str) {
            char* _buffer = new char[str.length() + 1];
            strcpy(_buffer, str.c_str());
            this->buffer = _buffer;
        }

        virtual ~StringValue() {
            delete[] buffer;
        }

        // Value methods

        /**
         * @return true since this string represents a value
         */
        virtual bool isEmpty() { return false; }

        /**
         * Prints the string to the stream
         * @param stream The stream to print to
         */
        virtual void print(std::basic_ostream<char>& stream) {
            stream << "\"" << buffer << "\"";
        }

        const char* get() {
            return buffer;
        }
};

/**
 * Implementation of value that stores a raw value
 * @tparam T The type of raw value to store
 */
template <typename T>
class RawValue : public Value {
    private:
        const T value;

    public:

        /**
         * Creates a new value value
         * @param value The value of the new RawValue
         */
        RawValue(T value) : value(value) {}

        virtual ~RawValue() {}

        // Value methods

        /**
         * @return true since this int represents a value
         */
        virtual bool isEmpty() { return false; }

        /**
         * Prints the value to the stream
         * @param stream The stream to print to
         */
        virtual void print(std::basic_ostream<char>& stream) {
            stream << value;
        }

        T get() {
            return value;
        }
};

typedef RawValue<int> IntValue;
typedef RawValue<float> FloatValue;
typedef RawValue<bool> BoolValue;