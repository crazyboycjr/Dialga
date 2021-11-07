/*!
 *  Copyright (c) 2018 by Contributors
 * \file utils.h
 */
#ifndef PRISM_UTILS_H_
#define PRISM_UTILS_H_
#include <stdlib.h>
#include <cassert>
#include <cstdio>
#include <string>

namespace prism {

template <typename... Args>
inline std::string FormatString(const char* fmt, Args... args) {
  int length = std::snprintf(nullptr, 0, fmt, args...);
  assert(length >= 0);

  char* buf = new char[length + 1];
  std::snprintf(buf, length + 1, fmt, args...);

  std::string str(buf);
  delete[] buf;
  return str;
}

template <typename T>
inline T GetEnvOrDefault(const char* name, T default_value);

#define REGISTER_GET_ENV_FUNC(type, func)                             \
  template <>                                                         \
  inline type GetEnvOrDefault(const char* name, type default_value) { \
    const char* value = getenv(name);                                 \
    if (!value) {                                                     \
      return default_value;                                           \
    }                                                                 \
    return func(value);                                               \
  }

REGISTER_GET_ENV_FUNC(int, atoi)
REGISTER_GET_ENV_FUNC(long, atol)
REGISTER_GET_ENV_FUNC(long long, atoll)
REGISTER_GET_ENV_FUNC(double, atof)
REGISTER_GET_ENV_FUNC(std::string, std::string)
REGISTER_GET_ENV_FUNC(const char*, )

#undef REGISTER_GET_ENV_FUNC

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(T) \
  T(T const&) = delete;             \
  T(T&&) = delete;                  \
  T& operator=(T const&) = delete;  \
  T& operator=(T&&) = delete
#endif

}  // namespace prism

#endif  // PRISM_UTILS_H_
