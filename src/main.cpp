#include "adapter/sor/schema.h"
#include "application.h"
#include "kvstore.h"

class Trivial : public Application {
public:
    Trivial(size_t idx) : Application(idx) { }
    virtual void _run() {
        size_t SZ = 1000*1000;
        float* vals = new float[SZ];
        double sum = 0;
        for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
        Key key("triv",0);
        DataFrame* df = DataFrame::fromArray(&key, &kv, SZ, vals);
        assert(df->get_float(0,1) == 1);
        DataFrame* df2 = kv.get(key);

        float temp = df2->get_float(0, 100);
        for (size_t i = 0; i < SZ; ++i) sum -= df2->get_float(0,i);
        assert(sum==0);
        delete df;
    }
};

int main(int argc, char** argv) {

    Trivial(0)._run();

    return 0;
}



