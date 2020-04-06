#pragma once

#include <cstdint>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstdlib>
#include <cstdio>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <sys/ioctl.h>

#include "../../utils/serial.h"

/** The max number of clients that need to be handled */
const uint8_t MAX_NUMBER_OF_CLIENTS = 5;
const uint16_t SERVER_PORT = 30000;

/**
 * The information for a client to be reached somewhere
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ClientIdentification {
    public:

        /** The number of bytes that a client identification is */
        static const size_t CLIENT_ID_SIZE = 6;

        /** The port number that the client can be reached on */
        uint16_t portNum = 0;

        /** The ip address of the client */
        uint32_t ipAddress = 0;

        /**
         * Creates a new client identification
         * @param portNum The port number that the client can be reached on
         * @param ipAddress The ip address of the client
         */
        ClientIdentification(uint16_t portNum, in_addr_t ipAddress) : portNum(portNum), ipAddress(ipAddress) {}

        /** Empty constructor */
        ClientIdentification() {}

};

/** The maximum size that Codable should serialize to */
#define MAX_SERIALIZABLE_SIZE 1500

/**
 * A class that can write and read itself in and out of a buffer
 */
class Codable {
    public:

        /**
         * Writes this object out to a buffer
         * @param serializer The serializer to write data with
         */
        virtual void serialize(Serializer& serializer) = 0;

        /**
         * Reads this object from a buffer
         * @param data The deserializer to read data from
         */
        virtual void deserialize(Deserializer& deserializer) = 0;
};

/**
 * A wrapper around a C socket that read and writes messages
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Socket {
    public:
        /** The file descriptor of the socket */
        int _socketFD;

        /** false if the socket is open, true otherwise */
        bool _closed = false;

        /** The address of the socket */
        sockaddr_in _address;

        /**
         * Creates a new socket for the given ip on the given port
         * @param ipAddress The ip address to bind to
         * @param portNum The number of the port to bind the socket to
         */
        Socket(in_addr_t ipAddress = 0, uint16_t portNum = 0) {
            if ((_socketFD = socket(AF_INET, SOCK_STREAM, 0)) == 0) { exit(0); }

            int socketOptions = 1;
            if (setsockopt(_socketFD, SOL_SOCKET, SO_REUSEADDR, &socketOptions, sizeof(int))) {
                std::cout << "Could not get file descriptor for socket" << std::endl;
                exit(1);
            }

            _address.sin_family = AF_INET;
            _address.sin_addr.s_addr = ipAddress;
            _address.sin_port = htons(portNum);

            if (bind(_socketFD, (const sockaddr*)&_address, sizeof(_address)) < 0) {
                std::cout << "Failed to bind socket" << std::endl;
                exit(2);
            }
        }

        /**
         * Private constructor that creates a new socket from a socket file descriptor
         * @param socketFD The socket descriptor
         */
        Socket(int socketFD) { _socketFD = socketFD; }

        /**
         * Connects this socket to another socket
         * @param ipAddress The ip address to connect this socket to
         * @param portNum The port number to connect this socket to
         */
        void connectTo(in_addr_t ipAddress, uint16_t portNum) {
            sockaddr_in serverAddress;
            serverAddress.sin_family = AF_INET;
            serverAddress.sin_port = htons(portNum);
            serverAddress.sin_addr.s_addr = ipAddress;

            if (connect(_socketFD, (const sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
                std::cout << "Failed to connect" << std::endl;
                exit(3);
            }
        }

        /**
         * Accepts a connection from a new socket
         * @param blocking true if accepting a new client should be blocking or not
         * @return The new socket that was accepted
         */
        Socket* acceptConnection(bool blocking = true) {
            if (listen(_socketFD, MAX_NUMBER_OF_CLIENTS) < 0) { exit(4); }

            if (!blocking) {
                fd_set readfds;
                timeval tv;
                tv.tv_usec = 50;
                tv.tv_sec = 0;

                FD_ZERO(&readfds);
                FD_SET(_socketFD, &readfds);

                int selectResult = select(_socketFD + 1, &readfds, NULL, NULL, &tv);
                if (selectResult < 0) {
                    std::cout << "Select error" << std::endl;
                    exit(6);
                } else if (!selectResult) { return nullptr; }
            }

            int newSocketFD;
            socklen_t addressLen = sizeof(_address);

            int acceptStatus = newSocketFD = accept(_socketFD, (sockaddr*)&_address, &addressLen);
            if (acceptStatus < 0) {
                std::cout << "Error accepting incoming connection" << std::endl;
                exit(5);
            }

            return new Socket(newSocketFD);
        }

        /** Returns true if the socket has data to be read, false otherwise */
        bool hasData() {
            int bytesAvailable;
            if (ioctl(_socketFD, FIONREAD, &bytesAvailable) < 0) { exit(8); }
            return bytesAvailable > 0;
        }

        /**
         * Sends data over the socket
         * @param data The contents of the data to write
         * @param length The length in bytes of the data
         */
        void sendData(Codable& data) {
            Serializer serializer;
            data.serialize(serializer);
            if (send(_socketFD, serializer.getBuffer(), serializer.getSize(), 0) != serializer.getSize()) {
                std::cout << "Write error!" << std::endl;
                exit(10);
            }
        }

        /**
         * Reads a given amount of data from the socket
         * @param data The location to read the data into
         * @param length The amount of data to read
         */
        void readData(void* data, size_t length) {
            if (recv(_socketFD, data, length, 0) != length) {
                std::cout << "Read error: " << strerror(errno) << std::endl;
                exit(9);
            }
        }

        /**
         * Closes this socket
         */
        void closeSocket() {
            closeWithHow(2);
        }

        /**
         * Closes the socket, but allows customization of the close behavior. This is equivalent to shutdown()
         * @param how The description of how the socket should be closed, same as in shutdown().
         */
        void closeWithHow(int how) {
            if (!_closed) {
                shutdown(_socketFD, how);
                _closed = true;
            }
        }

        /**
         * Provides the status of the socket
         * @return true if the socket is open, false otherwise
         */
        bool isOpen() { return !_closed; }

};

/**
 * A class that knows how to handle incoming messages and perform some kind of response
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class MessageHandler {
    public:

        /**
         * Handles an incoming message. This could include sending more messages back and forth using the socket
         * @param connectedClient The client that sent the message
         */
        virtual void handleMessage(class Message* message, class RemoteClient& connectedClient) = 0;
};

/**
 * Prints an IP address to a string
 * @param ip The ip to print
 * @param buffer The buffer to write the string into
 */
inline void printIP(uint32_t ip, char* buffer) {
    unsigned char bytes[4];
    bytes[0] = ip & 0x000000FF;
    bytes[1] = (ip & 0x0000FF00) >> 8;
    bytes[2] = (ip & 0x00FF0000) >> 16;
    bytes[3] = (ip & 0xFF000000) >> 24;
    sprintf(buffer, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
}
