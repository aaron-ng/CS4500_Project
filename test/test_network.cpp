#include <gtest/gtest.h>
#include <thread>

#include "utils.h"
#include "../src/network/shared/messages.h"
#include "../src/network/server.h"
#include "../src/network/client.h"

/* Begin message tests ----------------------------------------------------------------*/

char messageBuffer[MAX_SERIALIZABLE_SIZE];

void testMessageHeader() {
    Serializer serializer;
    MessageHeader(248, DATA).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    MessageHeader read;
    read.deserialize(deserializer);

    // Add the header size here because the constructor adds the header size
    GT_TRUE(read.length == 248 + MessageHeader::HEADER_SIZE);
    GT_TRUE(read.messageType == DATA);

    exit(0);
}

void testHandshakeMessage() {
    Serializer serializer;
    Handshake(16777343, 25565).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    Handshake read;
    read.deserialize(deserializer);
    GT_TRUE(read.ip == 16777343);
    GT_TRUE(read.port == 25565);

    exit(0);
}

void testTeardownMessage() {
    Serializer serializer;
    Teardown().serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    Teardown read;
    read.deserialize(deserializer);

    exit(0);
}

void testClientInformationMessage() {
    Serializer serializer;
    ClientIdentification info[] = {ClientIdentification(25565, 2602665218), ClientIdentification(35565, 16777343)};
    ClientInformation(2, info).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    ClientInformation read;
    read.deserialize(deserializer);
    GT_TRUE(read.numClients == 2);
    GT_TRUE(read.information[0].ipAddress == 2602665218);
    GT_TRUE(read.information[0].portNum == 25565);
    GT_TRUE(read.information[1].ipAddress == 16777343);
    GT_TRUE(read.information[1].portNum == 35565);

    exit(0);
}

void testDataMessage() {
    const char* hello = "Hello there new guy";
    Serializer serializer;
    KBMessage(RESPONSE_DATA, hello, strlen(hello) + 1).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    KBMessage read;
    read.deserialize(deserializer);

    GT_TRUE(read.getKbMessageType() == RESPONSE_DATA);
    GT_TRUE(read.length() == strlen(hello) + 1);
    GT_TRUE(!strcmp(hello, read.getData()));

    exit(0);
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

    exit(0);
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
    GT_TRUE(handshake.ip == 2602665218);
    GT_TRUE(handshake.port == 25565);

    connectingSocket.closeSocket();
    listeningSocket.closeSocket();
    newSocket->closeSocket();

    exit(0);
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

    GT_TRUE(c0.clientInformation().numClients == 2);
    GT_TRUE(c1.clientInformation().numClients == 2);

    server.close();
    serverThread.join();

    c0.poll();
    c1.poll();

    GT_TRUE(!c0.connected());
    GT_TRUE(!c1.connected());

    exit(0);
}

TEST(W4, testMessageHeader) { ASSERT_EXIT_ZERO(testMessageHeader) }
TEST(W4, testHandshakeMessage) { ASSERT_EXIT_ZERO(testHandshakeMessage) }
TEST(W4, testTeardownMessage) { ASSERT_EXIT_ZERO(testTeardownMessage) }
TEST(W4, testClientInformationMessage) { ASSERT_EXIT_ZERO(testClientInformationMessage) }
TEST(W4, testDataMessage) { ASSERT_EXIT_ZERO(testDataMessage) }
TEST(W4, testSocketsCanConnectToEachOther) { ASSERT_EXIT_ZERO(testSocketsCanConnectToEachOther) }
TEST(W4, testSocketsCanSendData) { ASSERT_EXIT_ZERO(testSocketsCanSendData) }
TEST(W4, testClientServer) { ASSERT_EXIT_ZERO(testClientServer) }