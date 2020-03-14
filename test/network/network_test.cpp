#include <iostream>
#include <thread>

#include "../../src/network/messages.h"
#include "../../src/network/server.h"
#include "../../src/network/client.h"

/* Begin message tests ----------------------------------------------------------------*/

char messageBuffer[MAX_SERIALIZABLE_SIZE];

void testMessageHeader() {
    Serializer serializer;
    MessageHeader(248, DATA).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    MessageHeader read;
    read.deserialize(deserializer);

    // Add the header size here because the constructor adds the header size
    assert(read.length == 248 + MessageHeader::HEADER_SIZE);
    assert(read.messageType == DATA);
}

void testHandshakeMessage() {
    Serializer serializer;
    Handshake(16777343, 25565).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    Handshake read;
    read.deserialize(deserializer);
    assert(read.ip == 16777343);
    assert(read.port == 25565);
}

void testTeardownMessage() {
    Serializer serializer;
    Teardown().serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    Teardown read;
    read.deserialize(deserializer);
}

void testClientInformationMessage() {
    Serializer serializer;
    ClientIdentification info[] = {ClientIdentification(25565, 2602665218), ClientIdentification(35565, 16777343)};
    ClientInformation(2, info).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    ClientInformation read;
    read.deserialize(deserializer);
    assert(read.numClients == 2);
    assert(read.information[0].ipAddress == 2602665218);
    assert(read.information[0].portNum == 25565);
    assert(read.information[1].ipAddress == 16777343);
    assert(read.information[1].portNum == 35565);
}

void testDataMessage() {
    const char* hello = "Hello there new guy";
    Serializer serializer;
    Data(hello, strlen(hello) + 1).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    Data read;
    read.deserialize(deserializer);

    assert(read.length() == strlen(hello) + 1);
    assert(!strcmp(hello, read.getData()));

}

/* End message tests ------------------------------------------------------------------*/

/* Begin socket tests -----------------------------------------------------------------*/

void testSocketsCanConnectToEachOther() {
    Socket listeningSocket(16777343, 25565);
    Socket connectingSocket;

    listeningSocket.acceptConnection(false);
    connectingSocket.connectTo(16777343, 25565);
    Socket* newSocket = listeningSocket.acceptConnection();

    connectingSocket.closeSocket();
    listeningSocket.closeSocket();
    newSocket->closeSocket();
    delete newSocket;
}

void testSocketsCanSendData() {
    Socket listeningSocket(16777343, 25565);
    Socket connectingSocket;

    listeningSocket.acceptConnection(false);
    connectingSocket.connectTo(16777343, 25565);
    Socket* newSocket = listeningSocket.acceptConnection();

    Handshake handshake(2602665218, 25565);
    newSocket->sendData(handshake);

    MessageReader reader(connectingSocket);
    Message* message = reader.readMessage();

    Deserializer deserializer = message->deserializer();
    handshake.deserialize(deserializer);
    assert(handshake.ip == 2602665218);
    assert(handshake.port == 25565);

    connectingSocket.closeSocket();
    listeningSocket.closeSocket();
    newSocket->closeSocket();
    delete newSocket;
    delete message;
}

/* End socket tests -------------------------------------------------------------------*/

void testClientServer() {
    Server server(inet_addr("127.0.0.1"), 25565);
    std::thread serverThread(&Server::run, std::ref(server));

    Client c0(inet_addr("127.0.0.1"), 25566, nullptr);
    Client c1(inet_addr("127.0.0.1"), 25567, nullptr);

    c0.connect(inet_addr("127.0.0.1"), 25565);
    c1.connect(inet_addr("127.0.0.1"), 25565);

    // Wait for server to send the clients the updated info
    usleep(5000);
    for (size_t i = 0; i < 2; i++) {
        c0.poll();
        c1.poll();
    }

    assert(c0.clientInformation().numClients == 2);
    assert(c1.clientInformation().numClients == 2);

    server.close();
    serverThread.join();

    c0.poll();
    c1.poll();

    assert(!c0.connected());
    assert(!c1.connected());
}

int main(int argc, char** argv) {
    testMessageHeader();
    testHandshakeMessage();
    testTeardownMessage();
    testClientInformationMessage();
    testDataMessage();
    testSocketsCanConnectToEachOther();
    testSocketsCanSendData();
    testClientServer();
    std::cout << "Tests passed!" << std::endl;
    return 0;
}
