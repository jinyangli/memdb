// Copyright (c) 2011 The memdb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef MEMDB_UTIL_TESTHARNESS_H_
#define MEMDB_UTIL_TESTHARNESS_H_

#include <stdio.h>
#include <stdlib.h>
#include <sstream>
//#include "env.h"
//#include "util/random.h"

namespace memdb {
namespace test {

// Run some of the tests registered by the TEST() macro.  If the
// environment variable "memdb_TESTS" is not set, runs all tests.
// Otherwise, runs only the tests whose name contains the value of
// "memdb_TESTS" as a substring.  E.g., suppose the tests are:
//    TEST(Foo, Hello) { ... }
//    TEST(Foo, World) { ... }
// memdb_TESTS=Hello will run the first test
// memdb_TESTS=o     will run both tests
// memdb_TESTS=Junk  will run no tests
//
// Returns 0 if all tests pass.
// Dies or returns a non-zero value if some test fails.
extern int RunAllTests();

// Return the directory to use for temporary storage.
//extern std::string TmpDir();

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
//extern int RandomSeed();

// An instance of Tester is allocated to hold temporary state during
// the execution of an assertion.
class Tester {
 private:
  bool ok_;
  const char* fname_;
  int line_;
  std::stringstream ss_;

 public:
  Tester(const char* f, int l)
      : ok_(true), fname_(f), line_(l) {
  }

  ~Tester() {
    if (!ok_) {
      fprintf(stderr, "%s:%d:%s\n", fname_, line_, ss_.str().c_str());
      exit(1);
    }
  }

  Tester& Is(bool b, const char* msg) {
    if (!b) {
      ss_ << " Assertion failure " << msg;
      ok_ = false;
    }
    return *this;
  }
/*
  Tester& IsOk(const Status& s) {
    if (!s.ok()) {
      ss_ << " " << s.ToString();
      ok_ = false;
    }
    return *this;
  }
*/
#define BINARY_OP(name,op)                              \
  template <class X, class Y>                           \
  Tester& name(const X& x, const Y& y) {                \
    if (! (x op y)) {                                   \
      ss_ << " failed: " << x << (" " #op " ") << y;    \
      ok_ = false;                                      \
    }                                                   \
    return *this;                                       \
  }

  BINARY_OP(IsEq, ==)
  BINARY_OP(IsNe, !=)
  BINARY_OP(IsGe, >=)
  BINARY_OP(IsGt, >)
  BINARY_OP(IsLe, <=)
  BINARY_OP(IsLt, <)
#undef BINARY_OP

  // Attach the specified value to the error message if an error has occurred
  template <class V>
  Tester& operator<<(const V& value) {
    if (!ok_) {
      ss_ << " " << value;
    }
    return *this;
  }
};

#define ASSERT_TRUE(c) ::memdb::test::Tester(__FILE__, __LINE__).Is((c), #c)
//#define ASSERT_OK(s) ::memdb::test::Tester(__FILE__, __LINE__).IsOk((s))
#define ASSERT_EQ(a,b) ::memdb::test::Tester(__FILE__, __LINE__).IsEq((a),(b))
#define ASSERT_NE(a,b) ::memdb::test::Tester(__FILE__, __LINE__).IsNe((a),(b))
#define ASSERT_GE(a,b) ::memdb::test::Tester(__FILE__, __LINE__).IsGe((a),(b))
#define ASSERT_GT(a,b) ::memdb::test::Tester(__FILE__, __LINE__).IsGt((a),(b))
#define ASSERT_LE(a,b) ::memdb::test::Tester(__FILE__, __LINE__).IsLe((a),(b))
#define ASSERT_LT(a,b) ::memdb::test::Tester(__FILE__, __LINE__).IsLt((a),(b))

#define TCONCAT(a,b) TCONCAT1(a,b)
#define TCONCAT1(a,b) a##b

#define TEST(base,name)                                                 \
class TCONCAT(_Test_,name) : public base {                              \
 public:                                                                \
  void _Run();                                                          \
  static void _RunIt() {                                                \
    TCONCAT(_Test_,name) t;                                             \
    t._Run();                                                           \
  }                                                                     \
};                                                                      \
bool TCONCAT(_Test_ignored_,name) =                                     \
  ::memdb::test::RegisterTest(#base, #name, &TCONCAT(_Test_,name)::_RunIt); \
void TCONCAT(_Test_,name)::_Run()

// Register the specified test.  Typically not used directly, but
// invoked via the macro expansion of TEST.
extern bool RegisterTest(const char* base, const char* name, void (*func)());

static long timediff(struct timespec *now, struct timespec *then)
{
  return (now->tv_sec-then->tv_sec)*1000000+(now->tv_nsec-then->tv_nsec)/1000;
}

static std::string RandomStr(int maxlen) {
	std::stringstream ss;
	int len = random() % maxlen;
	if (!len) len = 1;
	for (int i = 0; i < len; i++) {
		char c = 'a'+ (random()%26);
		ss << c;
	}
	return ss.str();
}

}  // namespace test
}  // namespace memdb

#endif  // MEMDB_UTIL_TESTHARNESS_H_
