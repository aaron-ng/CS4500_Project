#include <iostream>

#include "../../src/network/messages.h"
#include "../../src/network/network.h"
#include "../test_util.h"

/* Begin message tests ----------------------------------------------------------------*/

char messageBuffer[MAX_SERIALIZABLE_SIZE];

void testMessageHeader() {
    assert(MessageHeader(248, HELLO).serialize(messageBuffer) == MessageHeader::HEADER_SIZE);

    MessageHeader read;
    assert(read.deserialize(messageBuffer) == MessageHeader::HEADER_SIZE);

    // Add the header size here because the constructor adds the header size
    assert(read.length == 248 + MessageHeader::HEADER_SIZE);
    assert(read.messageType == HELLO);
}

void testHandshakeMessage() {
    assert(Handshake(16777343).serialize(messageBuffer) == sizeof(uint32_t) + MessageHeader::HEADER_SIZE);

    Handshake read;
    assert(read.deserialize(messageBuffer + MessageHeader::HEADER_SIZE) == sizeof(uint32_t));
    assert(read.ip == 16777343);
}

void testHandshakeResponseMessage() {
    assert(HandshakeResponse(25565).serialize(messageBuffer) == sizeof(uint16_t) + MessageHeader::HEADER_SIZE);

    HandshakeResponse read;
    assert(read.deserialize(messageBuffer + MessageHeader::HEADER_SIZE) == sizeof(uint16_t));
    assert(read.port == 25565);
}

void testListeningMessage() {
    assert(Listening().serialize(messageBuffer) == MessageHeader::HEADER_SIZE);

    Listening read;
    assert(read.deserialize(messageBuffer + MessageHeader::HEADER_SIZE) == 0);
}

void testClientInformationMessage() {
    ClientIdentification info[] = {ClientIdentification(25565, 2602665218), ClientIdentification(35565, 16777343)};
    assert(ClientInformation(2, info).serialize(messageBuffer) == sizeof(uint8_t) + 2 * (sizeof(uint16_t) + sizeof(uint32_t)) + MessageHeader::HEADER_SIZE);

    ClientInformation read;
    assert(read.deserialize(messageBuffer + MessageHeader::HEADER_SIZE) == sizeof(uint8_t) + 2 * (sizeof(uint16_t) + sizeof(uint32_t)));
    assert(read.numClients == 2);
    assert(read.information[0].ipAddress == 2602665218);
    assert(read.information[0].portNum == 25565);
    assert(read.information[1].ipAddress == 16777343);
    assert(read.information[1].portNum == 35565);
}

void testHelloMessage() {
    assert(Hello(2602665218, 25565).serialize(messageBuffer) == sizeof(uint16_t) + sizeof(uint32_t) + MessageHeader::HEADER_SIZE);

    Hello read;
    assert(read.deserialize(messageBuffer + MessageHeader::HEADER_SIZE) == sizeof(uint16_t) + sizeof(uint32_t));
    assert(read.ip == 2602665218);
    assert(read.port == 25565);
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

    Handshake handshake(2602665218);
    newSocket->sendData(handshake);

    MessageReader reader(connectingSocket);
    Message* message = reader.readMessage();

    assert(message->type == HANDSHAKE);
    assert(message->contentSize == sizeof(uint32_t));
    assert(*((uint32_t*)message->contents) == 2602665218);

    connectingSocket.closeSocket();
    listeningSocket.closeSocket();
    newSocket->closeSocket();
    delete newSocket;
    delete message;
}

/* End socket tests -------------------------------------------------------------------*/


void networkTest() {
    testMessageHeader();
    testHandshakeMessage();
    testHandshakeResponseMessage();
    testListeningMessage();
    testClientInformationMessage();
    testHelloMessage();
    testSocketsCanConnectToEachOther();
    testSocketsCanSendData();
    OK("Network Test Passed");
}
