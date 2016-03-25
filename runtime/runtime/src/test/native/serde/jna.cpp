#include "serde.hpp"

#include <iostream>
#include <cassert>

// for JNA
extern "C" {
using namespace asakusafw::serde;

int32_t jna_compact_int_size(int8_t v) {
    auto r = compact_int_size(v);
    return r;
}

int64_t jna_read_compact_int(int8_t *p) {
    auto p0 = p;
    auto r = read_compact_int(p);
    assert(p == (p0 + compact_int_size(*p0)));
    return r;
}

#define DECL_JNA_COMPARE(T) int32_t jna_compare_##T(int8_t *a, int8_t *b) { \
    auto a0 = a; \
    auto b0 = b; \
    auto r = compare_##T(a, b); \
    if (r == 0) { \
        skip_##T(a0); \
        skip_##T(b0); \
        assert(a == a0); \
        assert(b == b0); \
    } \
    return r; \
}

DECL_JNA_COMPARE(boolean)
DECL_JNA_COMPARE(byte)
DECL_JNA_COMPARE(short)
DECL_JNA_COMPARE(int)
DECL_JNA_COMPARE(long)
DECL_JNA_COMPARE(float)
DECL_JNA_COMPARE(double)
DECL_JNA_COMPARE(date)
DECL_JNA_COMPARE(date_time)
DECL_JNA_COMPARE(string)
DECL_JNA_COMPARE(decimal)

} // extern
