#pragma once

#include "shared/network.h"
#include "shared/messages.h"

/**
 * A client that is not running on this machine
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class RemoteClient {
    public:

        /** The socket used to communicate with the client */
        Socket _clientSocket;

        /** A reder that will recieve messages from the client */
        MessageReader _reader;

        /**
         * Default constructor
         * @param identification The information for the remote client
         */
        RemoteClient(ClientIdentification& identification) : _reader(_clientSocket) {
            _clientSocket.connectTo(identification.ipAddress, identification.portNum);
        }

        /**
         * Constructor for a client that is already connected on a socket
         * @param socket The socket that the client is connected on
         */
        RemoteClient(Socket& socket) : _clientSocket(socket._socketFD), _reader(_clientSocket) {}

        ~RemoteClient() { _clientSocket.closeSocket(); }

        /**
         * Sends a message to the remote client
         * @param message The message to send
         */
        void send(Codable& message) {
            _clientSocket.sendData(message);
        }

        /**
         * Recieves a message from the client. This is a blocking operation
         * @return The message that was sent to this cleint
         */
        Message* recieve() {
            return _reader.readMessage();
        }

};

/**
 * A client that connects to the central server
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Client {
    public:

        /** The IP that the client is reachable at */
        in_addr_t _ip;

        /** The port that the client is listening on */
        uint16_t _port;

        /** The node ID of the client */
        uint32_t _node = -1;

        /** The handler for messages. Owns the handler */
        MessageHandler* _handler;

        /** The socket that is used to communicate to the central server */
        Socket* _serverSocket = nullptr;

        /** The socket that is used to listen for incoming messages */
        Socket _listeningSocket;

        /** The information for the connected clients */
        ClientInformation _clientInfo;

        /**
         * Default constructor
         * @param ip The IP that the client is reachable at
         * @param handler The handler for messages. Owns the handler
         */
        Client(in_addr_t ip, uint16_t port, MessageHandler* handler): _ip(ip), _port(port), _handler(handler), _listeningSocket(ip, port) {
            _listeningSocket.acceptConnection(false);
        }

        ~Client() {
            if (_serverSocket) { _serverSocket->closeSocket(); }
            _listeningSocket.closeSocket();

            delete _serverSocket;
            delete _handler;
        }

        /**
         * Connects to the central server at the given IP. This will connect, register with the server and start
         * listening for messages from other clients until the server initiates teardown
         * @param serverIP The IP which the server can be reached on
         * @param serverPort The port that the server can be reached on
         */
        void connect(in_addr_t serverIP, uint16_t serverPort) {
            if (!_serverSocket) {
                _serverSocket = new Socket(_ip);
                _serverSocket->connectTo(serverIP, serverPort);
                _handshake(*_serverSocket);
            }
        }

        /**
         * Handshakes with the central server
         * @param s The socket used to communicate with the central server
         */
        void _handshake(Socket& s) {
            Handshake handshake(_ip, _port);
            s.sendData(handshake);

            MessageReader reader(s);
            Message* m = reader.readMessage();
            Deserializer deserializer = m->deserializer();

            HandshakeResponse response;
            response.deserialize(deserializer);

            _node = response.clientID;
            delete m;
        }

        /**
         * Determines if the client is listening to messages and connected to the central server
         */
        bool connected() { return _serverSocket && _serverSocket->isOpen(); }

        /**
         * Reads data from the central server if there is any. If the server tears the client down,
         * all of the open sockets are closed. If there is an incoming connection, data is read and the
         * message handler is used to generate a response
         */
        void poll() {
            if (connected()) {
                MessageReader reader(*_serverSocket);

                if (_serverSocket->hasData()) {
                    Message* response = reader.readMessage();

                    if (response->type == TEARDOWN) {
                        _teardown();
                        delete response;
                        return;
                    }

                    Deserializer infoDeserializer = response->deserializer();
                    _clientInfo.deserialize(infoDeserializer);
                    delete response;
                }

                Socket* newSocket = _listeningSocket.acceptConnection(false);
                if (newSocket) {
                    std::thread clientThread([&, newSocket]() {
                        MessageReader newReader(*newSocket);

                        Message* firstMessage = newReader.readMessage();
                        RemoteClient remoteClient(*newSocket);
                        _handler->handleMessage(firstMessage, remoteClient);

                        newSocket->closeWithHow(0);
                        delete firstMessage;
                        delete newSocket;
                    });

                    clientThread.detach();
                }
            }
        }

        /**
         * Sends a message to a client that is at clientInformation()[clientId]
         * @param clientId The index in clientInformation() to send the message to
         * @param data The data to send to the client
         */
        RemoteClient* send(size_t clientId, Codable& data) {
            RemoteClient* client = new RemoteClient(_clientInfo.information[clientId]);
            client->send(data);

            return client;
        }

        /**
         * Closes out the socket with the server and the listening socket
         */
        void _teardown() {
            _serverSocket->closeSocket();
            _serverSocket = nullptr;
        }

        /**
         * Provides the information of all of the clients connected to the central server, including this one
         */
        const ClientInformation& clientInformation() const { return _clientInfo; }

        /** Gets the node ID of the client. -1 if not connected to a server */
        uint32_t this_node() const { return _node; }

};