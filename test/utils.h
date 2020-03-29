#pragma once

#define GT_TRUE(a)   ASSERT_EQ((a),true)
#define GT_FALSE(a)  ASSERT_EQ((a),false)
#define GT_EQUALS(a, b)   ASSERT_EQ(a, b)
#define ASSERT_EXIT_ZERO(a)  \
  ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");