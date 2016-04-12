/**
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
package com.asakusafw.dag.runtime.io;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.BiFunction;

import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.runtime.io.util.DataBuffer;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.runtime.value.ValueOption;
import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;

/**
 * Test for {@code "native/include/serde.hpp"}.
 */
public class SerDeNativeTest {

    static final Logger LOG = LoggerFactory.getLogger(SerDeNativeTest.class);

    static final Mapper MAPPER;
    static {
        String path = new File("target/native/test/lib").getAbsolutePath();
        NativeLibrary.addSearchPath("test-serde", path);
        Mapper mapper;
        try {
            mapper = (Mapper) Native.loadLibrary("test-serde", Mapper.class);
        } catch (LinkageError e) {
            LOG.warn("native library is not available", e);
            mapper = null;
        }
        MAPPER = mapper;
    }

    /**
     * Check whether native library is enabled.
     */
    @ClassRule
    public static final TestRule NATIVE_CHECKER = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            Assume.assumeNotNull(MAPPER);
        }
    };

    /**
     * Test for {@code compact_int_size}.
     * @throws Exception if failed
     */
    @Test
    public void compact_int_size() throws Exception {
        checkCompactIntSize(0, 1);
        checkCompactIntSize(+1, 1);
        checkCompactIntSize(-1, 1);
        checkCompactIntSize(ValueOptionSerDe.COMPACT_INT_HEAD_MIN, 1);
        checkCompactIntSize(ValueOptionSerDe.COMPACT_INT_HEAD_MIN - 1, 2);
        checkCompactIntSize(Byte.MAX_VALUE, 1);
        checkCompactIntSize(Byte.MIN_VALUE, 2);
        checkCompactIntSize(Short.MAX_VALUE, 3);
        checkCompactIntSize(Short.MIN_VALUE, 3);
        checkCompactIntSize(Integer.MAX_VALUE, 5);
        checkCompactIntSize(Integer.MIN_VALUE, 5);
        checkCompactIntSize(Long.MAX_VALUE, 9);
        checkCompactIntSize(Long.MIN_VALUE, 9);
    }

    /**
     * Test for {@code read_compact_int}.
     * @throws Exception if failed
     */
    @Test
    public void read_compact_int() throws Exception {
        checkCompactInt(0);
        checkCompactInt(+1);
        checkCompactInt(-1);
        checkCompactInt(ValueOptionSerDe.COMPACT_INT_HEAD_MIN);
        checkCompactInt(ValueOptionSerDe.COMPACT_INT_HEAD_MIN - 1);
        checkCompactInt(Short.MAX_VALUE);
        checkCompactInt(Short.MIN_VALUE);
        checkCompactInt(Integer.MAX_VALUE);
        checkCompactInt(Integer.MIN_VALUE);
        checkCompactInt(Long.MAX_VALUE);
        checkCompactInt(Long.MIN_VALUE);
    }

    private void checkCompactIntSize(long value, int expected) throws IOException {
        DataBuffer buffer = new DataBuffer();
        ValueOptionSerDe.writeCompactLong(value, buffer);
        assertThat(MAPPER.jna_compact_int_size(buffer.getData()[0]), is(expected));
    }

    private void checkCompactInt(long value) throws IOException {
        Memory memory = new Memory(1024);
        ByteBuffer buffer = memory.getByteBuffer(0, memory.size());
        ByteBufferDataOutput output = new ByteBufferDataOutput(buffer);
        ValueOptionSerDe.writeCompactLong(value, output);
        assertThat(MAPPER.jna_read_compact_int(memory), is(value));
    }

    /**
     * Test for {@code compare_boolean}.
     * @throws Exception if failed
     */
    @Test
    public void compare_boolean() throws Exception {
        Comparator<BooleanOption> cmp = comparator(MAPPER::jna_compare_boolean);

        assertThat(cmp.compare(new BooleanOption(false), new BooleanOption(false)), is(0));
        assertThat(cmp.compare(new BooleanOption(true), new BooleanOption(true)), is(0));
        assertThat(cmp.compare(new BooleanOption(true), new BooleanOption(false)), greaterThan(0));
        assertThat(cmp.compare(new BooleanOption(false), new BooleanOption(true)), lessThan(0));

        assertThat(cmp.compare(new BooleanOption(), new BooleanOption()), is(0));
        assertThat(cmp.compare(new BooleanOption(false), new BooleanOption()), greaterThan(0));
        assertThat(cmp.compare(new BooleanOption(), new BooleanOption(false)), lessThan(0));
    }

    /**
     * Test for {@code compare_byte}.
     * @throws Exception if failed
     */
    @Test
    public void compare_byte() throws Exception {
        Comparator<ByteOption> cmp = comparator(MAPPER::jna_compare_byte);

        assertThat(cmp.compare(new ByteOption((byte) 0), new ByteOption((byte) 0)), is(0));
        assertThat(cmp.compare(new ByteOption((byte) 1), new ByteOption((byte) 0)), greaterThan(0));
        assertThat(cmp.compare(new ByteOption((byte) 0), new ByteOption((byte) 1)), lessThan(0));

        assertThat(cmp.compare(new ByteOption(), new ByteOption()), is(0));
        assertThat(cmp.compare(new ByteOption((byte) -1), new ByteOption()), greaterThan(0));
        assertThat(cmp.compare(new ByteOption(), new ByteOption((byte) -1)), lessThan(0));
    }

    /**
     * Test for {@code compare_short}.
     * @throws Exception if failed
     */
    @Test
    public void compare_short() throws Exception {
        Comparator<ShortOption> cmp = comparator(MAPPER::jna_compare_short);

        assertThat(cmp.compare(new ShortOption((short) 0), new ShortOption((short) 0)), is(0));
        assertThat(cmp.compare(new ShortOption((short) 1), new ShortOption((short) 0)), greaterThan(0));
        assertThat(cmp.compare(new ShortOption((short) 0), new ShortOption((short) 1)), lessThan(0));

        assertThat(cmp.compare(new ShortOption(), new ShortOption()), is(0));
        assertThat(cmp.compare(new ShortOption((short) -1), new ShortOption()), greaterThan(0));
        assertThat(cmp.compare(new ShortOption(), new ShortOption((short) -1)), lessThan(0));
    }

    /**
     * Test for {@code compare_int}.
     * @throws Exception if failed
     */
    @Test
    public void compare_int() throws Exception {
        Comparator<IntOption> cmp = comparator(MAPPER::jna_compare_int);

        assertThat(cmp.compare(new IntOption(0), new IntOption(0)), is(0));
        assertThat(cmp.compare(new IntOption(1), new IntOption(0)), greaterThan(0));
        assertThat(cmp.compare(new IntOption(0), new IntOption(1)), lessThan(0));

        assertThat(cmp.compare(new IntOption(), new IntOption()), is(0));
        assertThat(cmp.compare(new IntOption(-1), new IntOption()), greaterThan(0));
        assertThat(cmp.compare(new IntOption(), new IntOption(-1)), lessThan(0));
    }

    /**
     * Test for {@code compare_long}.
     * @throws Exception if failed
     */
    @Test
    public void compare_long() throws Exception {
        Comparator<LongOption> cmp = comparator(MAPPER::jna_compare_long);

        assertThat(cmp.compare(new LongOption(0), new LongOption(0)), is(0));
        assertThat(cmp.compare(new LongOption(1), new LongOption(0)), greaterThan(0));
        assertThat(cmp.compare(new LongOption(0), new LongOption(1)), lessThan(0));

        assertThat(cmp.compare(new LongOption(), new LongOption()), is(0));
        assertThat(cmp.compare(new LongOption(-1), new LongOption()), greaterThan(0));
        assertThat(cmp.compare(new LongOption(), new LongOption(-1)), lessThan(0));
    }

    /**
     * Test for {@code compare_float}.
     * @throws Exception if failed
     */
    @Test
    public void compare_float() throws Exception {
        Comparator<FloatOption> cmp = comparator(MAPPER::jna_compare_float);

        assertThat(cmp.compare(new FloatOption(0), new FloatOption(0)), is(0));
        assertThat(cmp.compare(new FloatOption(1), new FloatOption(0)), greaterThan(0));
        assertThat(cmp.compare(new FloatOption(0), new FloatOption(1)), lessThan(0));

        assertThat(cmp.compare(new FloatOption(), new FloatOption()), is(0));
        assertThat(cmp.compare(new FloatOption(-1), new FloatOption()), greaterThan(0));
        assertThat(cmp.compare(new FloatOption(), new FloatOption(-1)), lessThan(0));
    }

    /**
     * Test for {@code compare_double}.
     * @throws Exception if failed
     */
    @Test
    public void compare_double() throws Exception {
        Comparator<DoubleOption> cmp = comparator(MAPPER::jna_compare_double);

        assertThat(cmp.compare(new DoubleOption(0), new DoubleOption(0)), is(0));
        assertThat(cmp.compare(new DoubleOption(1), new DoubleOption(0)), greaterThan(0));
        assertThat(cmp.compare(new DoubleOption(0), new DoubleOption(1)), lessThan(0));

        assertThat(cmp.compare(new DoubleOption(), new DoubleOption()), is(0));
        assertThat(cmp.compare(new DoubleOption(-1), new DoubleOption()), greaterThan(0));
        assertThat(cmp.compare(new DoubleOption(), new DoubleOption(-1)), lessThan(0));
    }

    /**
     * Test for {@code compare_date}.
     * @throws Exception if failed
     */
    @Test
    public void compare_date() throws Exception {
        Comparator<DateOption> cmp = comparator(MAPPER::jna_compare_date);

        assertThat(cmp.compare(newDate(0), newDate(0)), is(0));
        assertThat(cmp.compare(newDate(1), newDate(0)), greaterThan(0));
        assertThat(cmp.compare(newDate(0), newDate(1)), lessThan(0));

        assertThat(cmp.compare(new DateOption(), new DateOption()), is(0));
        assertThat(cmp.compare(newDate(0), new DateOption()), greaterThan(0));
        assertThat(cmp.compare(new DateOption(), newDate(0)), lessThan(0));
    }

    /**
     * Test for {@code compare_date_time}.
     * @throws Exception if failed
     */
    @Test
    public void compare_date_time() throws Exception {
        Comparator<DateTimeOption> cmp = comparator(MAPPER::jna_compare_date_time);

        assertThat(cmp.compare(newDateTime(0), newDateTime(0)), is(0));
        assertThat(cmp.compare(newDateTime(1), newDateTime(0)), greaterThan(0));
        assertThat(cmp.compare(newDateTime(0), newDateTime(1)), lessThan(0));

        assertThat(cmp.compare(new DateTimeOption(), new DateTimeOption()), is(0));
        assertThat(cmp.compare(newDateTime(0), new DateTimeOption()), greaterThan(0));
        assertThat(cmp.compare(new DateTimeOption(), newDateTime(0)), lessThan(0));
    }

    /**
     * Test for {@code compare_string}.
     * @throws Exception if failed
     */
    @Test
    public void compare_string() throws Exception {
        Comparator<StringOption> cmp = comparator(MAPPER::jna_compare_string);

        assertThat(cmp.compare(new StringOption("a"), new StringOption("a")), is(0));
        assertThat(cmp.compare(new StringOption("b"), new StringOption("a")), greaterThan(0));
        assertThat(cmp.compare(new StringOption("a"), new StringOption("b")), lessThan(0));

        assertThat(cmp.compare(new StringOption("AAA"), new StringOption("AAA")), is(0));
        assertThat(cmp.compare(new StringOption("ABA"), new StringOption("AAB")), greaterThan(0));
        assertThat(cmp.compare(new StringOption("AAB"), new StringOption("ABA")), lessThan(0));

        assertThat(cmp.compare(new StringOption(), new StringOption()), is(0));
        assertThat(cmp.compare(new StringOption("a"), new StringOption()), greaterThan(0));
        assertThat(cmp.compare(new StringOption(), new StringOption("a")), lessThan(0));
    }

    /**
     * Test for {@code compare_decimal}.
     * @throws Exception if failed
     */
    @Test
    public void compare_decimal() throws Exception {
        Comparator<DecimalOption> cmp = comparator(MAPPER::jna_compare_decimal);

        assertThat(cmp.compare(newDecimal("1"), newDecimal("1")), is(0));
        assertThat(cmp.compare(newDecimal("1.1"), newDecimal("1")), greaterThan(0));
        assertThat(cmp.compare(newDecimal("1.10"), newDecimal("1")), greaterThan(0));
        assertThat(cmp.compare(newDecimal("1.10"), newDecimal("2")), lessThan(0));
        assertThat(cmp.compare(newDecimal("1"), newDecimal("1.1")), lessThan(0));
        assertThat(cmp.compare(newDecimal("1"), newDecimal("1.10")), lessThan(0));
        assertThat(cmp.compare(newDecimal("2"), newDecimal("1.10")), greaterThan(0));

        assertThat(cmp.compare(newDecimal("1"), newDecimal("1")), is(0));
        assertThat(cmp.compare(newDecimal("1"), newDecimal("-1")), greaterThan(0));
        assertThat(cmp.compare(newDecimal("-1"), newDecimal("1")), lessThan(0));

        assertThat(cmp.compare(new DecimalOption(), new DecimalOption()), is(0));
        assertThat(cmp.compare(newDecimal("1.1"), new DecimalOption()), greaterThan(0));
        assertThat(cmp.compare(new DecimalOption(), newDecimal("1.1")), lessThan(0));
    }

    private DateOption newDate(int v) {
        return new DateOption(new Date(v));
    }

    private DateTimeOption newDateTime(long v) {
        return new DateTimeOption(new DateTime(v));
    }

    private DecimalOption newDecimal(String v) {
        return new DecimalOption(new BigDecimal(v));
    }

    private <T extends ValueOption<?>> Comparator<T> comparator(BiFunction<Pointer, Pointer, Integer> func) {
        return (a, b) -> {
            Pointer pa = serialize(a);
            Pointer pb = serialize(b);
            return func.apply(pa, pb);
        };
    }

    private Pointer serialize(ValueOption<?> v) {
        try {
            Memory memory = new Memory(1024);
            ByteBuffer buffer = memory.getByteBuffer(0, memory.size());
            ByteBufferDataOutput output = new ByteBufferDataOutput(buffer);

            Class<?> type = v.getClass();
            Method target = ValueOptionSerDe.class.getMethod("serialize", type, DataOutput.class);
            target.invoke(null, v, output);

            return memory;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * native mapper.
     */
    @SuppressWarnings("javadoc")
    public interface Mapper extends Library {
        int jna_compact_int_size(byte head);
        long jna_read_compact_int(Pointer bytes);

        int jna_compare_boolean(Pointer a, Pointer b);
        int jna_compare_byte(Pointer a, Pointer b);
        int jna_compare_short(Pointer a, Pointer b);
        int jna_compare_int(Pointer a, Pointer b);
        int jna_compare_long(Pointer a, Pointer b);
        int jna_compare_float(Pointer a, Pointer b);
        int jna_compare_double(Pointer a, Pointer b);
        int jna_compare_date(Pointer a, Pointer b);
        int jna_compare_date_time(Pointer a, Pointer b);
        int jna_compare_string(Pointer a, Pointer b);
        int jna_compare_decimal(Pointer a, Pointer b);
    }
}
