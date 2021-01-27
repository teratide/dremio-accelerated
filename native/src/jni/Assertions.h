//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_ASSERTIONS_H
#define SPARK_EXAMPLE_ASSERTIONS_H

#include <iostream>
#include <jni.h>

void exitWithError(const std::string& msg);

#define ASSERT_OK(s)                                                                        \
  do {                                                                                      \
    ::arrow::Status _s = ::arrow::internal::GenericToStatus(s);                             \
    if (!_s.ok()) {                                                                         \
        std::cout << "ERROR " << _s.CodeAsString() << ": " << _s.message() << std::endl;    \
        exitWithError("ERROR - " + _s.CodeAsString() + ": " + _s.message());                \
    }                                                                                       \
  } while (0)

#define ASSERT_FLETCHER_OK(s)                                                               \
  do {                                                                                      \
    fletcher::Status _s = s;                                                      \
    if (!_s.ok()) {                                                                     \
        std::cout << "FLETCHER ERROR: "  << _s.message << std::endl;                        \
        exitWithError(_s.message);                                                          \
    }                                                                                       \
  } while (0)

#define ASSERT(condition, msg)                                                              \
  do {                                                                                      \
    if (!(condition)) {                                                                     \
        exitWithError(msg);                                                                 \
    }                                                                                       \
  } while (0)


#endif //SPARK_EXAMPLE_ASSERTIONS_H
