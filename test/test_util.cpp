#include <gtest/gtest.h>

#include "utils.h"
#include "../src/ea2/dataframe_description.h"

/* Start util tests                                                */
/*-----------------------------------------------------------------*/

void testColumnDescription() {
    Key k("HELLO", 21);
    Serializer s;

    ColumnDescription(k, INT).serialize(s);

    Deserializer deserializer(s.getSize(), s.getBuffer());
    ColumnDescription read;
    read.deserialize(deserializer);

    GT_TRUE(read.type == INT);
    GT_TRUE(!strcmp(read.location->getName(), k.getName()));
    GT_TRUE(read.location->getNode() == k.getNode());

    exit(0);
}

void testDataframeDescriptions() {

    const char _schema[3] = {INT, STRING, '\0'};
    String* schema = new String(_schema);

    Key kA("HELLO-A", 21);
    Key kB("HELLO-B", 12);

    ColumnDescription** descriptions = new ColumnDescription*[2];
    descriptions[0] = new ColumnDescription(kA, INT);
    descriptions[1] = new ColumnDescription(kB, STRING);

    Serializer serializer;
    DataframeDescription(schema->clone(), 2, descriptions).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    DataframeDescription read;
    read.deserialize(deserializer);

    GT_TRUE(read.schema->equals(schema));

    GT_TRUE(read.columns[0]->type == INT);
    GT_TRUE(!strcmp(read.columns[0]->location->getName(), kA.getName()));
    GT_TRUE(read.columns[0]->location->getNode() == kA.getNode());

    GT_TRUE(read.columns[1]->type == STRING);
    GT_TRUE(!strcmp(read.columns[1]->location->getName(), kB.getName()));
    GT_TRUE(read.columns[1]->location->getNode() == kB.getNode());

    delete schema;

    exit(0);
}

TEST(W2, testColumnDescription) { ASSERT_EXIT_ZERO(testColumnDescription) }
TEST(W2, testDataframeDescriptions) { ASSERT_EXIT_ZERO(testDataframeDescriptions) }