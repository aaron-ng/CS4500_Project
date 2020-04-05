#pragma once

#include "shared/network.h"
#include "../utils/datastructures/element_column.h"
#include "shared/messages.h"

/**
 * An association of a ClientIdentification with a Socket
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class ServerClientInfo {
    public:

        /** The identifying information for the client */
        ClientIdentification identification;

        /** The socket that the client is reachable at */
        Socket* socket;

        /**
         * Default constructor
         * @param identification The identifying information for the client
         * @param socket The socket that the client is reachable at
         */
        ServerClientInfo(const ClientIdentification &identification, Socket *socket) : identification(identification), socket(socket) {};

        ~ServerClientInfo() {
            delete socket;
        }

};

/**
 * The central registration server
 * Written by: pazol.l@husky.neu.edu and ng.h@husky.neu.edu
 */
class Server {
    public:

        /** The clients that are currently connected */
        ElementColumn _clients;

        /** The socket that is used to listen for incoming connections */
        Socket _s;

        /** true if this server has been torn down, false otherwise */
        bool _tornDown = false;

        /**
         * Creates a new central listening server
         * @param serverIP The IP to bind the server to
         * @param serverPort The port to bind the server to
         */
        Server(in_addr_t serverIP, uint16_t serverPort) : _s(serverIP, serverPort) {}

        virtual ~Server() {
            for (size_t i = 0; i < _clients.size(); i++) {
                delete _clients.get(i)->cI;
            }
        }

        /** Starts listening to incoming connections and serving the list of connected clients to any incoming clients */
        void run() {
            if (!_tornDown) {
                while (!_tornDown) {

                    // Connect a new client
                    Socket* newConnection = _s.acceptConnection(false);
                    if (!newConnection) {
                        usleep(50);
                        continue;
                    }

                    MessageReader reader(*newConnection);
                    Message* message = reader.readMessage();

                    // Handshake with the client
                    Handshake handshake;
                    Deserializer deserializer = message->deserializer();
                    handshake.deserialize(deserializer);
                    delete message;

                    HandshakeResponse response(_clients.size());
                    newConnection->sendData(response);

                    std::cout << "Client is listening" << std::endl;

                    // Notify all of the other clients a new client has connected
                    ClientIdentification newIdentification(handshake.port, handshake.ip);
                    _clients.grow()->cI = new ServerClientInfo(newIdentification, newConnection);
                    _notifyClients();
                }

                _sendTeardownToClients();
            }
        }

        /** Closes the server */
        void close() { _tornDown = true; }

        /**
         * Notifies all of the clients that the server is tearing down
         */
        void _sendTeardownToClients() {
            Teardown teardown;
            for (size_t i = 0; i < _clients.size(); i++) {
                Socket* socket = _clients.get(i)->cI->socket;
                socket->sendData(teardown);
                socket->closeSocket();
            }
        }

        /**
         * Notifies all of the clients of the currently connected clients
         */
        void _notifyClients() {
            ClientIdentification* clientIdentification = new ClientIdentification[_clients.size()];
            for (size_t i = 0; i < _clients.size(); i++) {
                clientIdentification[i] = _clients.get(i)->cI->identification;
            }

            ClientInformation info(_clients.size(), clientIdentification);
            for (size_t i = 0; i < _clients.size(); i++) {
                _clients.get(i)->cI->socket->sendData(info);
            }
        }

};
