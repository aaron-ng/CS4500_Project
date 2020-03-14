#include "adapter/sor/lineparser_test.h"
#include "adapter/sor/valuefactory_test.h"
#include "adapter/values/valueproducers_test.h"
#include "dataframe/parallel_map_examples.h"


void testAdapter() {
    Lineparser_test lineparser_test = Lineparser_test();
    lineparser_test.run();

    Valuefactory_test valuefactory_test = Valuefactory_test();
    valuefactory_test.run();

    ValueProducers_test valueProducers_test = ValueProducers_test();
    valueProducers_test.run();
};

void testDataFrame() {
    testParallelMapExamples();
}


// Main testing .cpp file
int main() {
    testAdapter();
    testDataFrame();
    return 0;
}
