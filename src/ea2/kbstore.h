#pragma once

// Language: C++

#include <thread>

#include "../network/client.h"
#include "../utils/key.h"
#include "dataframe_description.h"

/**
 * A byte array with a length and contents
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class ByteArray: public Object {
    public:

        /** The contents of the buffer */
        const char* contents;

        /** The length of the buffer */
        size_t length;

        /** True if this byte array owns its data */
        bool _ownsData;

        /** Default constructor */
        ByteArray(const char *contents, size_t length, bool ownsData = true) : contents(contents), length(length), _ownsData(ownsData) {}

        virtual ~ByteArray() {
            if (_ownsData) {
                delete[] contents;
            }
        }

};

/**
 * An object wrapper a std::atomic that says if a key is ready
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class Ready: public Object {
    public:
        std::atomic<bool> isReady;

        /** Default constructor */
        Ready() : isReady(false) {}
};

/**
 * A key to byte array store. All arrays are owned by the store
 * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
 */
class KBStore {
    public:
        /** The map where all of the bytes are stored */
        Map _map;

        /** Statuses of keys that are used for waitAndGet. This prevents deadlocks */
        Map _statuses;

        /** Mutex for _statuses */
        std::mutex _statusMutex;

        /** The client used to talk to other KBstores */
        Client _client;

        /** tue if the byte store is still listening */
        std::atomic<bool> _listening;

        /** The thread that is listening for new connections */
        std::thread _listeningThread;

        /**
         * Default constructor
         * @param ip The IP that the client is reachable at
         * @param handler The handler for messages. Owns the handler
         * @param serverIP The IP of the rendezvous server
         * @param serverPort The port of the rendezvous server
         */
        KBStore(in_addr_t ip, uint16_t port, in_addr_t serverIP, uint16_t serverPort) : _listening(true), _client(ip, port, new KBStoreMessageHander(*this)) {
            _client.connect(serverIP, serverPort);

            _listeningThread = std::thread([&] {
                while (_listening) {
                    _client.poll();
                }
            });
        }

        ~KBStore() {
            _listening = false;
            _listeningThread.join();

            std::vector<Entry*>& entries = _map.entrySet();
            for (size_t i = 0; i < _map.get_size(); i++) {
                delete entries[i]->key;
                delete entries[i]->value;
            }

            std::vector<Entry*>& statusEntries = _statuses.entrySet();
            for (size_t i = 0; i < _statuses.get_size(); i++) {
                delete statusEntries[i]->key;
                delete statusEntries[i]->value;
            }
        }

        /**
         * Retrieves the buffer with the given key from the store. If the
         * value does not exist, nullptr is returned. The buffer is owned by this store.
         * @param key The key of the buffer to return
         * @return Returns the byte array. This byte array is unowned.
         */
        ByteArray* get(Key& key) {
            if (key._node == _client.this_node()) {
                ByteArray* existing = (ByteArray*)_map.get(&key);
                return existing ? new ByteArray(existing->contents, existing->length, false) : existing;
            } else {
                return _get(key, GET);
            }
        }

        /**
         * Retrieves the buffer with the given key from the store. This call will block until the value exists
         * @param key The key of the buffer to return
         */
        ByteArray* waitAndGet(Key& key) {
            if (key._node == _client.this_node()) {
                _statusMutex.lock();

                if (_map.contains_key(&key)) {
                    _statusMutex.unlock();
                    return get(key);
                }

                Ready* ready = dynamic_cast<Ready*>(_statuses.get(&key));
                if (!ready) {
                    ready = new Ready();
                    _statuses.put(key.clone(), ready);
                }

                _statusMutex.unlock();
                while (ready->isReady == false) {}
                return get(key);
            } else {
                return _get(key, GET_AND_WAIT);
            }
        }

        /**
         * Puts a series of bytes inside of the store
         * @param contents The buffer to put into the store
         * @param length The length of the bytes in the buffer
         * @param key The key to store the buffer under
         */
        void put(const char *contents, size_t length, Key& key) {
            if (key._node == _client.this_node()) {

                _statusMutex.lock();

                char* newBuffer = new char[length];
                memcpy(newBuffer, contents, sizeof(char) * length);
                _map.put(key.clone(), new ByteArray(newBuffer, length));

                Ready* ready = dynamic_cast<Ready*>(_statuses.get(&key));
                if (ready) { ready->isReady = true; }

                _statusMutex.unlock();
            } else {
                _put(key, contents, length);
            }
        }

        /**
         * Puts the data in a remote KBStore
         * @param key The key to put the data under
         * @param contents The data to put
         * @param length The length in bytes of the data
         */
        void _put(Key& key, const char *contents, size_t length) {
            Serializer serializer;
            serializer.write(key);
            serializer._write(contents, length);

            KBMessage message(PUT, serializer.getBuffer(), serializer.getSize());
            RemoteClient* client = _client.send(key.getNode(), message);

            Message* m = client->recieve();
            Deserializer deserializer = m->deserializer();

            KBMessage read;
            read.deserialize(deserializer);
            assert(read.getKbMessageType() == ACK);

            delete m;
            delete client;
        }

        /**
         * Performs a get from a remote client. This can be wait and get or just regular get
         * @param key The key to get
         * @param type either GET or GET_AND_WAIT
         * @return The bytes returned by the remote KBStore
         */
        ByteArray* _get(Key& key, KBMessageType type) {
            Serializer serializer;
            serializer.write(key);

            KBMessage message(type, serializer.getBuffer(), serializer.getSize());
            RemoteClient* client = _client.send(key.getNode(), message);

            Message* m = client->recieve();
            Deserializer deserializer = m->deserializer();

            KBMessage read;
            read.deserialize(deserializer);
            assert(read.getKbMessageType() == RESPONSE_DATA);

            if (!read.length()) { return nullptr; }
            char* buffer = new char[read.length()];
            memcpy(buffer, read.getData(), read.length() * sizeof(char));

            delete m;
            delete client;
            return new ByteArray(buffer, read.length());
        }

        /**
         * Provides the node identifier of the running application. This is determined
         * by the rendezvous server
         */
        size_t this_node() const { return _client.this_node(); }

        /** Provides the number of currently connected nodes  */
        size_t nodes() { return _client.connectedClients(); };

        /**
         * A message handler that performs all of the operations on the KVStore
         * Created by ng.h@husky.neu.edu and pazol.l@husky.neu.edu
         */
        class KBStoreMessageHander: public MessageHandler {
            public:

                /** The store to perform operations on */
                KBStore& _store;

                /** Default constructor */
                KBStoreMessageHander(KBStore &store) : _store(store) {}

                /**
                 * Handles the KBMessage. This does the various operations like put get and wait and get
                 * @param message
                 * @param connectedClient
                 */
                virtual void handleMessage(Message *message, RemoteClient &connectedClient) {
                    Deserializer deserializer(message->contentSize, message->contents);
                    KBMessage kbMessage;
                    kbMessage.deserialize(deserializer);

                    switch (kbMessage.getKbMessageType()) {
                        case PUT:
                            handlePut(kbMessage, connectedClient);
                            break;
                        case GET:
                            handleGet(kbMessage, connectedClient);
                            break;
                        case GET_AND_WAIT:
                            handleWaitAndGet(kbMessage, connectedClient);
                            break;
                        default:
                            break;
                    }
                }

                /**
                 * Handles putting data inside of the store
                 * @param message The data as well as the key
                 * @param connectedClient The connected client
                 */
                void handlePut(KBMessage& message, RemoteClient &connectedClient) {
                    Deserializer deserializer(message.length(), message.getData());
                    Key* key = deserializer.read_key();

                    _store.put(deserializer.head(), deserializer.remainingBytes(), *key);

                    KBMessage reply(ACK, nullptr, 0);
                    connectedClient.send(reply);

                    delete key;
                }

                /**
                 * Handles getting data out of the store. If the data is not in the store, 0 bytes are returned
                 * @param message The data as well as the key
                 * @param connectedClient The connected client
                 */
                void handleGet(KBMessage& message, RemoteClient &connectedClient) {
                    Deserializer deserializer(message.length(), message.getData());
                    Key* key = deserializer.read_key();

                    sendResponse(_store.get(*key), connectedClient);

                    delete key;
                }

                /**
                 * Handles getting data out of the store
                 * @param message The data as well as the key
                 * @param connectedClient The connected client
                 */
                void handleWaitAndGet(KBMessage& message, RemoteClient &connectedClient) {
                    Deserializer deserializer(message.length(), message.getData());
                    Key* key = deserializer.read_key();

                    sendResponse(_store.waitAndGet(*key), connectedClient);

                    delete key;
                }

                /**
                 * Sends the byte array to the given client. If the byte array is empty, a response data with 0 length
                 * is sent
                 * @param bytes The bytes to send
                 * @param connectedClient The client to send the bytes to
                 */
                void sendResponse(ByteArray* bytes, RemoteClient& connectedClient) {
                    if (!bytes) {
                        KBMessage reply(RESPONSE_DATA, nullptr, 0);
                        connectedClient.send(reply);
                    } else {
                        KBMessage reply(RESPONSE_DATA, bytes->contents, bytes->length);
                        connectedClient.send(reply);
                        delete bytes;
                    }
                }

        };

};