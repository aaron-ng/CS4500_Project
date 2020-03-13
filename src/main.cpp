#include "adapter/sor/schema.h"

int main(int argc, char** argv) {

    SchemaBuilder builder;
    DataFrame* dataFrame = builder.build(fopen("./data/data.sor", "r"));

    // Add up all of the numbers in the first column
    long sum = 0;

    for (size_t i = 0; i < dataFrame->nrows(); i++) {
        sum += dataFrame->get_int(0, i);
    }

    std::cout << "Sum of first column: " << sum << std::endl;
    delete dataFrame;

    return 0;
}



