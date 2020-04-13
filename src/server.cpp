#include "network/server.h"

int main(int argc, char** argv) {
    Server(inet_addr("127.0.0.1"), SERVER_PORT).run();
    return 0;
}