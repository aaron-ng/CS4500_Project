#pragma once

/**
 * Example Directory class for serialization
 * Written by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class Directory {
public:
    int64_t _client;
    double * _ports;  // owned
    String ** _addresses;  // owned; strings owned

    /**
     * Constructor to construct a directory
     * @param client the number of clients
     * @param ports the port of the clients
     * @param addresses the addresses of the clients
     */
    Directory(int64_t client, double* ports, String** addresses): _client(client), _ports(ports), _addresses(addresses) {}
};