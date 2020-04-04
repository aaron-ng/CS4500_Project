#include <gtest/gtest.h>

#include "utils.h"
#include "../src/ea2/dataframe_description.h"

/* Start util tests                                                */
/*-----------------------------------------------------------------*/

void testColumnDescription() {
    Key** keys = new Key*[2];
    keys[0] = new Key("HELLO", 21);
    keys[1] = new Key("BYE", 11);

    Serializer s;
    ColumnDescription(keys, 2, 25, INT).serialize(s);

    Deserializer deserializer(s.getSize(), s.getBuffer());
    ColumnDescription read;
    read.deserialize(deserializer);

    GT_TRUE(read.type == INT);
    GT_TRUE(read.chunks == 2);
    GT_TRUE(!strcmp(read.keys[0]->getName(), "HELLO"));
    GT_TRUE(!strcmp(read.keys[1]->getName(), "BYE"));
    GT_TRUE(read.keys[0]->getNode() == 21);
    GT_TRUE(read.keys[1]->getNode() == 11);

    exit(0);
}

void testDataframeDescriptions() {

    const char _schema[3] = {INT, STRING, '\0'};
    String* schema = new String(_schema);

    Key** kA = new Key*[2];
    kA[0] = new Key("HELLO", 21);
    kA[1] = new Key("BYE", 11);

    Key** kB = new Key*[2];
    kB[0] = new Key("HOUSE", 0);
    kB[1] = new Key("BRICK", 5);

    ColumnDescription** descriptions = new ColumnDescription*[2];
    descriptions[0] = new ColumnDescription(kA, 2, 2020, INT);
    descriptions[1] = new ColumnDescription(kB, 2, 97, STRING);

    Serializer serializer;
    DataframeDescription(schema->clone(), 2, descriptions).serialize(serializer);

    Deserializer deserializer(serializer.getSize(), serializer.getBuffer());
    DataframeDescription read;
    read.deserialize(deserializer);

    GT_TRUE(read.schema->equals(schema));

    GT_TRUE(read.columns[0]->type == INT);
    GT_TRUE(read.columns[0]->chunks == 2);
    GT_TRUE(!strcmp(read.columns[0]->keys[0]->getName(), "HELLO"));
    GT_TRUE(!strcmp(read.columns[0]->keys[1]->getName(), "BYE"));
    GT_TRUE(read.columns[0]->keys[0]->getNode() == 21);
    GT_TRUE(read.columns[0]->keys[1]->getNode() == 11);

    GT_TRUE(read.columns[1]->type == STRING);
    GT_TRUE(read.columns[1]->chunks == 2);
    GT_TRUE(!strcmp(read.columns[1]->keys[0]->getName(), "HOUSE"));
    GT_TRUE(!strcmp(read.columns[1]->keys[1]->getName(), "BRICK"));
    GT_TRUE(read.columns[1]->keys[0]->getNode() == 0);
    GT_TRUE(read.columns[1]->keys[1]->getNode() == 5);

    delete schema;

    exit(0);
}

TEST(W2, testColumnDescription) { ASSERT_EXIT_ZERO(testColumnDescription) }
TEST(W2, testDataframeDescriptions) { ASSERT_EXIT_ZERO(testDataframeDescriptions) }