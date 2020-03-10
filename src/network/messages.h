#pragma once

#include <cstddef>
#include <cstdint>

#include "network.h"

/**
 * The types of messages that are sent between clients and between the client and server
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
enum MessageType: uint8_t {
    HANDSHAKE,
    CLIENT_INFO,
    DATA,
    TEARDOWN,
};

/**
 * The message header for a client-client or client-server message
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class MessageHeader: public Codable {
    public:

        /** The length in bytes of a header */
        static const size_t HEADER_SIZE = sizeof(uint32_t) + sizeof(uint8_t);

        /** The length of the message in bytes including the header **/
        uint32_t length;

        /** The type of message being sent */
        MessageType messageType;

        /** Constructor for deserialization */
        MessageHeader() {}

        /**
         * Default constructor
         * @param length The length of the message in bytes excluding the header
         * @param messageType The type of message being sent
         */
        MessageHeader(uint32_t length, MessageType messageType) : length(HEADER_SIZE + length), messageType(messageType) {}

        /**
         * Serializes the message header
         * @param serializer The serializer to write the data to
         */
        virtual void serialize(Serializer& serializer) {
            serializer.write((uint8_t)messageType);
            serializer.write(length);
        }

        /**
         * Deserializes the header from a buffer
         * @param buffer The buffer to deserialize from
         */
        virtual void deserialize(Deserializer& deserializer) {
            messageType = (MessageType)deserializer.read_uint8();
            length = deserializer.read_int32();
        }
};

/**
 * A message that has not yet been deserialized
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Message {
    public:

        /** The type of this message */
        const MessageType type;

        /** The length of the contents */
        const size_t contentSize;

        /** The contents of the message */
        const char* contents;

        /**
         * Create a new un-deserialized message
         * @param type The type of the message
         * @param contentSize The length of the contents
         * @param contents  The contents of the message
         */
        Message(MessageType type, uint32_t contentSize, const char *contents) : type(type), contentSize(contentSize),
                                                                                contents(contents) {}

        virtual ~Message() { delete[] contents; }

        /**
         * Create a new deserializaer for this message
         * @return A deserializer for this message
         */
        Deserializer deserializer() { return Deserializer(contentSize, contents); }
};

/**
 * A class that reads messages from a socket
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class MessageReader {
    public:
        /** The buffer to store the read data from */
        char buffer[MessageHeader::HEADER_SIZE];

        /** The socket to read from */
        Socket& _socket;

        /** Creates a new message reader for the socket */
        MessageReader(Socket &socket) : _socket(socket) {}

        /**
         * Reads a message
         * @return
         */
        Message* readMessage() {
            _socket.readData(buffer, MessageHeader::HEADER_SIZE);

            Deserializer deserializer(MessageHeader::HEADER_SIZE, buffer);
            MessageHeader header;
            header.deserialize(deserializer);

            char* data = new char[header.length];
            memcpy(data, buffer, MessageHeader::HEADER_SIZE);
            _socket.readData(data + MessageHeader::HEADER_SIZE, header.length - MessageHeader::HEADER_SIZE);

            return new Message(header.messageType, header.length, data);
        }
};

/**
 * The packet sent from the client to the server during the initial handshake
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Handshake: public Codable {
    public:

        /** The ip address of the client */
        uint32_t ip;

        /** The port of the client */
        uint16_t port;

        /** Constructor for deserialization */
        Handshake() {}

        /**
         * Default constructor
         * @param ip The ip address of the client
         * @param port The port of the client
         */
        Handshake(uint32_t ip, uint16_t port) : ip(ip), port(port) {}

        /**
         * Serializes the handshake to a buffer
         * @param serializer The buffer to write to
         */
        virtual void serialize(Serializer& serializer) {
            MessageHeader(sizeof(ip) + sizeof(port), HANDSHAKE).serialize(serializer);
            serializer.write(ip);
            serializer.write(port);
        }

        /**
         * Reads the handshake from a buffer
         * @param deserializer The buffer to read from
         */
        virtual void deserialize(Deserializer& deserializer) {
            MessageHeader header;
            header.deserialize(deserializer);
            assert(header.messageType == HANDSHAKE);

            ip = deserializer.read_uint32();
            port = deserializer.read_uint16();
        }
};

/**
 * The packet that is sent from the server to the client to tell the client that it should teardown
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Teardown: public Codable {
    public:

        /**
         * Serializes this packet's message header since the packet itself has no contents
         * @param serializer buffer to write to
         */
        virtual void serialize(Serializer& serializer) {
            return MessageHeader(0, TEARDOWN).serialize(serializer);
        }

        /**
         * Deserializes the teardown packed from the buffer
         * @param deserializer The buffer to read from
         */
        virtual void deserialize(Deserializer& deserializer) {
            MessageHeader header;
            header.deserialize(deserializer);
            assert(header.messageType == TEARDOWN);
        }
};

/**
 * A message that contains connection information for any number of clients
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ClientInformation: public Codable {
    public:

        /** The number of clients that there is information for */
        uint32_t numClients = 0;

        /** The information about the clients */
        ClientIdentification* information = nullptr;

        /** Constructor for deserialization */
        ClientInformation() {}

        /**
         * Default constructor
         * @param numClients The number of clients that there is information for
         * @param information The information about the clients
         */
        ClientInformation(uint32_t numClients, ClientIdentification *information) : numClients(numClients),
                                                                                    information(information) {}
        /**
         * Writes all of the client information out to
         * @param serializer buffer to write to
         */
        virtual void serialize(Serializer& serializer) {
            size_t size = sizeof(numClients) + numClients * ClientIdentification::CLIENT_ID_SIZE;
            MessageHeader(size, CLIENT_INFO).serialize(serializer);
            serializer.write(numClients);

            for (size_t i = 0; i < numClients; i++) {
                serializer.write(information[i].portNum);
                serializer.write(information[i].ipAddress);
            }
        }

        /**
         * Does nothing since there is no data to deserialize
         * @param deserializer The buffer to read from
         */
        virtual void deserialize(Deserializer& deserializer) {
            MessageHeader header;
            header.deserialize(deserializer);
            assert(header.messageType == CLIENT_INFO);

            numClients = deserializer.read_uint32();
            information = new ClientIdentification[numClients];

            for (size_t i = 0; i < numClients; i++) {
                information[i].portNum = deserializer.read_uint16();
                information[i].ipAddress = deserializer.read_uint32();
            }
        }

};

/**
 * A message that contains an arbitrary amount of data
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Data: public Codable {
    public:

        /** Use a string since the serializer already knows how to handle that */
        char* _data = nullptr;

        /** The length in bytes of data */
        uint64_t _length;

        /**
         * Default constructor
         * @param data The data for this message
         * @param length The length of the data in bytes
         */
        Data(const char* data, size_t length) : _length(length) {
            _data = new char[length];
            memcpy(_data, data, sizeof(char) * length);
        }

        /** Constructor for deserialziation */
        Data() {}

        ~Data() {
            delete[] _data;
        }

        /**
         * Serializes the data message
         * @param serializer buffer to write to
         */
        virtual void serialize(Serializer& serializer) {
            MessageHeader(sizeof(uint64_t) + sizeof(char) * _length, DATA).serialize(serializer);
            serializer.write(_length);
            serializer._write(_data, sizeof(char) * _length);
        }

        /**
         * Reads the data from a buffer
         * @param deserializer The buffer to read from
         */
        virtual void deserialize(Deserializer& deserializer) {
            MessageHeader header;
            header.deserialize(deserializer);
            assert(header.messageType == DATA);

            _length = deserializer.read_uint64();
            _data = deserializer.read(_length);
        }

        /**
         * Provides the data in this message
         * @return The data of the message
         */
        char* getData() const { return _data; }

        /**
         * Provides the length of the data
         * @return The length of the data in this message
         */
        size_t length() { return _length; }

};