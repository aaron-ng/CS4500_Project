#pragma once

#include <iostream>

/**
 * Stores some value that may or may not exist.
 */
class Value {
    public:
        virtual ~Value() {}

        /**
         * @return true if this object contains a value
         */
        virtual bool isEmpty() = 0;

        /**
         * Prints the value to the output stream
         * @param stream The stream to print to
         */
        virtual void print(std::basic_ostream<char>& stream) = 0;
};

/**
 * An empty value
 */
class EmptyValue : public Value {
    public:

        virtual ~EmptyValue() {}

        /**
         * @return true since this class cannot contain a value
         */
        virtual bool isEmpty() { return true; }

        /**
         * "Prints" this empty to the stream. Since there is no value, nothing is printed
         * @param stream The stream to "print" to
         */
        virtual void print(std::basic_ostream<char>& stream) { }
};