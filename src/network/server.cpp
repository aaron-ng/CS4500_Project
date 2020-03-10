#include <thread>
#include <iostream>

#include "server.h"



int main(int argc, char** argv) {
    assert(argc >= 3);
    assert(!strcmp("-ip", argv[1]));

    bool lastsForever = argc > 3 && !strcmp("-f", argv[3]);

    Server server(inet_addr(argv[2]), SERVER_PORT);
    std::thread serverThread(&Server::run, std::ref(server));

    std::cout << "Hit enter to stop server..." << std::endl;

    if (!lastsForever) {
        char c;
        std::cin.read(&c, 1);
        server.close();
    } else { while(true) {} }

    serverThread.join();
}

