// Language C++

#include <thread>

#include "adapter/sor/schemabuilder.h"
#include "ea2/application.h"
#include "ea2/kvstore/kvstore.h"

Key m("main",0);
Key verify("verif",0);
Key check("ck",0);

class Demo : public Application {
    public:

        Demo(size_t idx, KVStore& kv): Application(idx, kv) {}

        virtual void _run() override {
            switch(this_node()) {
                case 0:   producer();     break;
                case 1:   counter();      break;
                case 2:   summarizer();
            }
        }

        void producer() {
            size_t SZ = 100*1000;
            double* vals = new double[SZ];
            double sum = 0;
            for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
            DataFrame::fromArray(&m, &kv, SZ, vals);
            DataFrame::fromScalar(&check, &kv, sum);

            delete [] vals;
        }

        void counter() {
            DataFrame* v = kv.waitAndGet(m);
            size_t sum = 0;
            for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
            p("The sum is  ").pln(sum);
            DataFrame::fromScalar(&verify, &kv, (double)sum);

            delete v;
        }

        void summarizer() {
            DataFrame* result = kv.waitAndGet(verify);
            DataFrame* expected = kv.waitAndGet(check);
            pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");

            delete result;
            delete expected;
        }
};

int main(int argc, char** argv) {

    std::vector<KVStore*> stores = { new KVStore(), new KVStore(), new KVStore() };
    std::vector<std::thread> threads;

    for (int i = 0; i < stores.size(); i++) {
        stores[i]->_stores = stores;

        threads.push_back(std::thread([i, &stores]() {
            Demo demo(i, *stores[i]);
            demo._run();
        }));
    }

    for (int i = 0; i < 3; i++) {
        threads[i].join();
    }

    for (int i = 0; i < 3; i++) {
        delete stores[i];
    }

    return 0;
}



