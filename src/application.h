#pragma once

#include <iostream>
#include "printer.h"

/**
 * An application that is running on 
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu 
 */
 class Application: public Printer {
    public:

         // TODO REMOVE
         size_t _TEMP;

        /** The distributed key store that this application is connected to */
        KVStore kv;

        /**
         * Creates a new application that runs on a distributed key store. 
         * This constructor will rendezvous with the central server.
         * @param idx An unused parameter so far. The client ID will be assigned by the server,
         * but the example code supplies this param
         */
        Application(size_t idx) { _TEMP = idx; }

        /**
         * Provides the node identifier of the running application. This is determined
         * by the rendezvous server
         */
        size_t this_node() const { return _TEMP; /* return kv.this_node(); */ }

        /** The function that is called after the application setup is complete */
        virtual void _run() = 0;

};