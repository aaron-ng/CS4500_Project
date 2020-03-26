#include <thread>

#include "adapter/sor/schema.h"
#include "application.h"
#include "kvstore/kvstore.h"

Key m("main",0);
Key verify("verif",0);
Key check("ck",0);

class Demo : public Application {
    public:

        Demo(size_t idx): Application(idx) {}

        virtual void _run() override {
            switch(this_node()) {
                case 0:   producer();     break;
                case 1:   counter();      break;
                case 2:   summarizer();
            }
        }

        void producer() {
            size_t SZ = 100;
            float* vals = new float[SZ];
            float sum = 0;
            for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
            DataFrame::fromArray(&m, &kv, SZ, vals);
            DataFrame::fromScalar(&check, &kv, sum);

            delete [] vals;
        }

        void counter() {
            DataFrame* v = kv.waitAndGet(m);
            size_t sum = 0;
            for (size_t i = 0; i < 100; ++i) sum += v->get_float(0,i);
            p("The sum is  ").pln((float)sum);
            DataFrame::fromScalar(&verify, &kv, (float)sum);

            delete v;
        }

        void summarizer() {
            DataFrame* result = kv.waitAndGet(verify);
            DataFrame* expected = kv.waitAndGet(check);
            pln(expected->get_float(0,0)==result->get_float(0,0) ? "SUCCESS":"FAILURE");

            delete result;
            delete expected;
        }
};

int main(int argc, char** argv) {

    std::vector<std::thread> threads;
    for (int i = 0; i < 3; i++) {
        // TEMP code that uses C++
        threads.push_back(std::thread([i]() {
            Demo demo(i);
            demo._run();
        }));
    }

    for (int i = 0; i < 3; i++) {
        threads[i].join();
    }

    return 0;
}



