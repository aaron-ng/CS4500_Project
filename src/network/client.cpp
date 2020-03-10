#include <iostream>
#include <arpa/inet.h>
#include "network.h"
#include "messages.h"
#include "client.h"

class Echo: public MessageHandler {
    public:

        /**
         * Echos the data from the message as a string
         * @param message The message that is received
         * @param client The client that sent the message
         */
        virtual void handleMessage(class Message* message, RemoteClient& client) {
            Deserializer deserializer = message->deserializer();

            Data data;
            data.deserialize(deserializer);
            std::cout << data.getData() << std::endl;
        }

};

int main(int argc, char** argv) {
    assert(argc >= 3);
    assert(!strcmp("-ip", argv[1]));

    uint16_t port = 25565;
    if (argc > 3 && !strcmp("-port", argv[3])) {
        assert(argc >= 5);
        port = atoi(argv[4]);
    }

    in_addr_t ip = inet_addr(argv[2]);
    Client c(ip, port, new Echo());

    c.connect(inet_addr("127.0.0.1"), SERVER_PORT);

    int connectedClients = 0;
    while (c.connected()) {
        c.poll();

        int newCount = c.clientInformation().numClients;
        if (newCount != connectedClients) {
            std::cout << newCount << " connected clients" << std::endl;
            connectedClients = newCount;

            // Send the last (assumed newest client) a hello
            const char* hello = "Hello there new guy";
            Data data(hello, strlen(hello) + 1);
            c.send(connectedClients - 1, data);
        }
    }

    std::cout << "Received teardown from server" << std::endl;

}
