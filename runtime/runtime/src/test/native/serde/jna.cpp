/*
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
