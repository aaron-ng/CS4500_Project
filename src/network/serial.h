#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include "../string.h"
#include "../object.h"
#include "../element_column.h"

/**
 * Class to serialize an object
 * Written by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class Serializer {
    public:
        static const size_t STARTING_BUFFER = 512;
        char* _buffer = new char[STARTING_BUFFER];
        size_t _capacity = STARTING_BUFFER;
        size_t _writtenBytes = 0;

        Serializer() {}

        ~Serializer() {
            delete[] _buffer;
        }

        /**
         * Private method that converts the content and serializes the data into the buffer
         * @param contents the data being serialized
         * @param size the byte size of the data
         */
        void _write(void* contents, size_t size) {
            if (_writtenBytes + size > _capacity) {
                _expand(size);
            }

            memcpy(_buffer + _writtenBytes, contents, size);
            _writtenBytes += size;
        }

        /**
         * Private method to expand the given buffer based on the given size. Expands it by adding the current capacity
         * and the given size then times two.
         * @param size the given object that is being serialized
         */
        void _expand(size_t size) {
            size_t newCapacity = (_capacity + size) * 2;
            char* newBuffer = new char[newCapacity];
            memcpy(newBuffer, _buffer, sizeof(char) * _writtenBytes);
            delete[] _buffer;

            _buffer = newBuffer;
            _capacity = newCapacity;
        }

        /**
         * Gets the current size of the buffer
         * @return the size of the buffer
         */
        size_t getSize() {
            return _writtenBytes;
        }

        /**
         * Gets the buffer
         * @return the buffer
         */
        char* getBuffer() {
            return _buffer;
        }

        /**
         * Returns a copy of the buffer
         * @return The buffer
         */
        char* getUnownedBuffer() {
            char* buffer = new char[_writtenBytes];
            memcpy(buffer, _buffer, sizeof(char) * _writtenBytes);
            return buffer;
        }

        /**
         * Public write method that serializes a int8_t
         * @param data an int8_t object
         */
        void write(int8_t data) {
            _write(&data, sizeof(int8_t));
        }

        /**
         * Public write method that serializes a int16_t
         * @param data an int16_t object
         */
        void write(int16_t data) {
            _write(&data, sizeof(int16_t));
        }

        /**
        * Public write method that serializes a int32_t
        * @param data an int32_t object
        */
        void write(int32_t data) {
            _write(&data, sizeof(int32_t));
        }

        /**
        * Public write method that serializes a int64_t
        * @param data an int64_t object
        */
        void write(int64_t data) {
            _write(&data, sizeof(int64_t));
        }

        /**
        * Public write method that serializes a uint8_t
        * @param data an uint8_t object
        */
        void write(uint8_t data) {
            _write(&data, sizeof(uint8_t));
        }

        /**
        * Public write method that serializes a uint16_t
        * @param data an uint16_t object
        */
        void write(uint16_t data) {
            _write(&data, sizeof(uint16_t));
        }

        /**
        * Public write method that serializes a uint32_t
        * @param data an uint32_t object
        */
        void write(uint32_t data) {
            _write(&data, sizeof(uint32_t));
        }

        /**
        * Public write method that serializes a uint64_t
        * @param data an uint64_t object
        */
        void write(uint64_t data) {
            _write(&data, sizeof(uint64_t));
        }

        /**
        * Public write method that serializes a float
        * @param float an float object
        */
        void write(float data) {
            _write(&data, sizeof(float));
        }

        /**
        * Public write method that serializes a double
        * @param data an double object
        */
        void write(double data) {
            _write(&data, sizeof(double));
        }

        /**
        * Public write method that serializes an Element. It must be noted that sometimes an element
        * can be interpreted as a pointer. A deserialized element should never be interpreted as a pointer.
        * @param data an double object
        */
        void write(Element data) {
            _write(&data, sizeof(Element));
        }

        /**
        * Public write method that serializes a String
        * @param data a String pointer
        */
        void write(String* data) {
            uint64_t size = data->size();
            write(size);
            _write(data->c_str(), sizeof(char) * size);
        }

        /**
         * Public write method that serializes an Array of String*
         * @param data a String** object
         * @param length the size of the Array of String*
         */
        void write(String** data, uint64_t length) {
            write(length);
            for (uint64_t i = 0; i < length; i++) {
                write(data[i]);
            }
        }

        /**
         * Public write method that serializes an Array of Doubles
         * @param data an Array of doubles
         * @param length the size of the Array of doubles
         */
        void write(double* data, uint64_t length) {
            write(length);
            for (uint64_t i = 0; i < length; i++) {
                write(data[i]);
            }
        }

        /**
        * Public write method that serializes a sockaddr_in object
        * @param data an sockaddr_in object
        */
        void write(sockaddr_in data) {
            write(data.sin_family);
            write(data.sin_port);
            write(data.sin_addr.s_addr);
            _write(data.sin_zero, sizeof(char) * 8);
        }
};

/**
 * Class to deserialize an object
 * Written by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class Deserializer {
    public:
        size_t _length;
        size_t _position = 0;
        const char* _buffer;

        /**
         * Deserializer owns the buffer after its creation
         * @param length the length of the given serializer buffer
         * @param buffer the actual byte buffer of the serialized object
         */
        Deserializer(size_t length, const char* buffer): _length(length), _buffer(buffer) {}

        /**
         * Private method that deserializes the object and increments the current position in the buffer
         * @param length the length that we need to deserialize by
         */
        void _deserialize(size_t length) {
            if (_position + length > _length) {
                std::cout << "Error inside _deserialize" << "\n";
                exit(1);
            }

            _position += length;
        }

        /**
         * Reads an arbitrary number of bytes from the buffer
         * @param length The length to read in bytes
         * @return The data that was deserialized
         */
        char* read(size_t length) {
            char* data = new char[length];

            memcpy(data, _buffer + _position, sizeof(char) * length);

            _deserialize(sizeof(char) * length);

            return data;
        }

        /**
         * Reads an int8_t object
         * @return the int8_t object
         */
        int8_t read_int8() {
            int8_t* data = (int8_t*)(_buffer + _position);
            _deserialize(sizeof(int8_t));

            return *data;
        }

        /**
         * Reads an int16_t object
         * @return the int16_t object
         */
        int16_t read_int16() {
            int16_t* data = (int16_t*)(_buffer + _position);
            _deserialize(sizeof(int16_t));

            return *data;
        }

        /**
         * Reads an int32_t object
         * @return the int32_t object
         */
        int32_t read_int32() {
            int32_t* data = (int32_t*)(_buffer + _position);
            _deserialize(sizeof(int32_t));

            return *data;
        }

        /**
         * Reads an int64_t object
         * @return the int64_t object
         */
        int64_t read_int64() {
            int64_t* data = (int64_t*)(_buffer + _position);
            _deserialize(sizeof(int64_t));

            return *data;
        }

        /**
         * Reads an uint8_t object
         * @return the uint8_t object
         */
        uint8_t read_uint8() {
            uint8_t* data = (uint8_t*)(_buffer + _position);
            _deserialize(sizeof(uint8_t));

            return *data;
        }

        /**
         * Reads an uint16_t object
         * @return the uint16_t object
         */
        uint16_t read_uint16() {
            uint16_t* data = (uint16_t*)(_buffer + _position);
            _deserialize(sizeof(uint16_t));

            return *data;
        }

        /**
         * Reads an uint32_t object
         * @return the uint32_t object
         */
        uint32_t read_uint32() {
            uint32_t* data = (uint32_t*)(_buffer + _position);
            _deserialize(sizeof(uint32_t));

            return *data;
        }

        /**
         * Reads an uint64_t object
         * @return the uint64_t object
         */
        uint64_t read_uint64() {
            uint64_t* data = (uint64_t*)(_buffer + _position);
            _deserialize(sizeof(uint64_t));

            return *data;
        }

        /**
         * Reads an float object
         * @return the float object
         */
        float read_float() {
            float* data = (float*)(_buffer + _position);
            _deserialize(sizeof(float));

            return *data;
        }

        /**
         * Reads an double object
         * @return the double object
         */
        double read_double() {
            double* data = (double*)(_buffer + _position);
            _deserialize(sizeof(double));

            return *data;
        }

        /**
         * Reads an Element. It must be noted that sometimes an element
         * can be interpreted as a pointer. A deserialized element should never be interpreted as a pointer.
         */
        Element read_element() {
            Element* data = (Element*)(_buffer + _position);
            _deserialize(sizeof(Element));

            return *data;
        }

        /**
         * Reads an String* object by memcpying the buffer into a it's own String
         * @return the String* object
         */
        String* read_string() {
            uint64_t length = read_uint64();
            char* contents = new char[length + 1];
            _deserialize(sizeof(char) * length);

            memcpy(contents, _buffer + _position - length, sizeof(char) * length);
            contents[length] = '\0';

            String* finalString = new String(contents);

            delete[] contents;

            return finalString;
        }

        /**
         * Reads an String** object. Does so by looping through each string in the serialization buffer
         * @return the String** object
         */
        String** read_string_array() {
            uint64_t length = read_uint64();

            String** contents = new String*[length];

            for (uint64_t i = 0; i < length; i++) {
                contents[i] = read_string();
            }

            return contents;
        }

        /**
         * Reads an double* object. Does so by looping through each double and deserialize each value
         * @return the int16_t object
         */
        double* read_double_array() {
            uint64_t length = read_uint64();

            double* contents = new double[length];
            for (uint64_t i = 0; i< length; i++) {
                contents[i] = read_double();
            }

            return contents;
        }

        /**
        * Reads an sockaddr_in object
        * @return the sockaddr_in object
        */
        sockaddr_in read_socket_addr() {
            sockaddr_in content;

            content.sin_family = read_uint8();
            content.sin_port = read_uint16();
            content.sin_addr.s_addr = read_uint32();

            _deserialize(sizeof(char) * 8);
            memcpy(content.sin_zero, _buffer + _position - (sizeof(char) * 8), sizeof(char) * 8);

            return content;
        }
};