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
package com.asakusafw.dag.compiler.codegen;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.asakusafw.dag.runtime.io.ValueOptionSerDe;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.Invariants;
import com.asakusafw.dag.utils.common.Io;
import com.asakusafw.dag.utils.common.Lang;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;

/**
 * Generates {@code C++ style} value comparators.
 */
public class NativeValueComparatorGenerator implements Io {

    /**
     * The header file name.
     */
    public static final String HEADER_FILE_NAME = "serde.hpp";

    static final Charset ENCODE = StandardCharsets.UTF_8;

    static final Location HEADER_PATH =
            Location.of("com/asakusafw/dag/runtime/io/native/include/serde.hpp"); //$NON-NLS-1$

    private static final Pattern PATTERN_VARIABLE = Pattern.compile("\\$\\{(\\w+)\\}"); //$NON-NLS-1$

    private static final String[] FILE_HEADER = {
            "#include \"serde.hpp\"",
            "",
            "#define COMPARE_T(t, a, b, op) \\",
            "{ \\",
            "    int diff = compare_##t((a), (b)); \\",
            "    if (diff != 0) { \\",
            "        return diff op 0; \\",
            "    } \\",
            "}",
            "",
            "extern \"C\" {",
            "",
            "using namespace asakusafw::serde;",
            "",
    };

    private static final String[] FILE_FOOTER = {
            "} // extern",
    };

    private static final String[] FUNC_HEADER = {
            "bool ${name}(const void *_a, const void *_b) {",
            "    int8_t *a = (int8_t *) _a;",
            "    int8_t *b = (int8_t *) _b;",
    };

    private static final String[] FUNC_FOOTER = {
            "    return 0;",
            "}",
    };

    private static final String[] FUNC_BODY = {
            "COMPARE_T(${type}, a, b, ${op});",
    };

    private static final Map<TypeDescription, String> TYPE_NAME = Lang.let(new HashMap<>(), m -> {
        m.put(Descriptions.typeOf(BooleanOption.class), "boolean");
        m.put(Descriptions.typeOf(ByteOption.class), "byte");
        m.put(Descriptions.typeOf(ShortOption.class), "short");
        m.put(Descriptions.typeOf(IntOption.class), "int");
        m.put(Descriptions.typeOf(LongOption.class), "long");
        m.put(Descriptions.typeOf(FloatOption.class), "float");
        m.put(Descriptions.typeOf(DoubleOption.class), "double");
        m.put(Descriptions.typeOf(DecimalOption.class), "decimal");
        m.put(Descriptions.typeOf(DateOption.class), "date");
        m.put(Descriptions.typeOf(DateTimeOption.class), "date_time");
        m.put(Descriptions.typeOf(StringOption.class), "string");
    });

    private static final Map<Group.Direction, String> OPERATOR = Lang.let(new HashMap<>(), m -> {
        m.put(Group.Direction.ASCENDANT, "<");
        m.put(Group.Direction.DESCENDANT, ">");
    });

    private final PrintWriter writer;

    private boolean first = true;

    private boolean closed = false;

    /**
     * Creates a new instance.
     * @param writer the target writer
     */
    public NativeValueComparatorGenerator(PrintWriter writer) {
        Arguments.requireNonNull(writer);
        this.writer = writer;
    }

    /**
     * Generates a value comparator function.
     * @param reference the target reference
     * @param orderings the ordering information
     * @param name the generating class name
     */
    public void add(DataModelReference reference, List<Group.Ordering> orderings, String name) {
        append(FUNC_HEADER, Collections.singletonMap("name", name)); //$NON-NLS-1$
        for (Group.Ordering order : orderings) {
            append(FUNC_BODY, Lang.let(new HashMap<>(), m -> {
                PropertyReference property = Invariants.requireNonNull(
                        reference.findProperty(order.getPropertyName()));
                m.put("type", Invariants.requireNonNull(TYPE_NAME.get(property.getType())));
                m.put("op", Invariants.requireNonNull(OPERATOR.get(order.getDirection())));
            }));
        }
        append(FUNC_FOOTER, Collections.singletonMap("name", name)); //$NON-NLS-1$
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        append(FILE_FOOTER, Collections.emptyMap());
        writer.close();
    }

    private void append(String[] lines, Map<String, String> variables) {
        if (first) {
            first = false;
            append(FILE_HEADER, Collections.emptyMap());
        }
        append(writer, lines, variables);
    }

    private static void append(PrintWriter writer, String[] lines, Map<String, String> variables) {
        for (String line : lines) {
            Matcher m = PATTERN_VARIABLE.matcher(line);
            int start = 0;
            while (m.find(start)) {
                if (m.start() > start) {
                    writer.print(line.substring(start, m.start()));
                }
                String name = m.group(1);
                Invariants.require(variables.containsKey(name));
                writer.print(variables.get(name));
                start = m.end();
            }
            if (start < line.length()) {
                writer.print(line.substring(start));
            }
            writer.println();
        }
    }

    /**
     * Puts header file contents into the target writer.
     * @param writer the target writer
     */
    public static void putHeader(PrintWriter writer) {
        try (InputStream in = ValueOptionSerDe.class.getClassLoader().getResourceAsStream(HEADER_PATH.toPath())) {
            Invariants.requireNonNull(in);
            try (Scanner s = new Scanner(new InputStreamReader(in, ENCODE))) {
                while (s.hasNextLine()) {
                    writer.println(s.nextLine());
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(MessageFormat.format(
                    "error occurred while copying header file: {0}",
                    HEADER_PATH), e);
        }
    }
}
