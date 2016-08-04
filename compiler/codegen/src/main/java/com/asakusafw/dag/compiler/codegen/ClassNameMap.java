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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.Invariants;
import com.asakusafw.dag.utils.common.Tuple;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * A basic implementation of class name providers.
 * @since 0.2.0
 */
public class ClassNameMap {

    static final Pattern PATTERN_CATEGORY = Pattern.compile("[A-Za-z][A-Za-z0-9_]*(\\.[A-Za-z][A-Za-z0-9_]*)*"); //$NON-NLS-1$

    static final Pattern PATTERN_HINT = Pattern.compile("[A-Za-z][A-Za-z0-9]*"); //$NON-NLS-1$

    private final String prefix;

    private final Map<Tuple<String, String>, AtomicInteger> counters = new HashMap<>();

    /**
     * Creates a new instance.
     * @param prefix the prefix of fully qualified class names to generate
     */
    public ClassNameMap(String prefix) {
        Arguments.requireNonNull(prefix);
        this.prefix = prefix;
    }

    /**
     * Returns a unique class name.
     * @param category the category name
     * @param hint an optional class name hint
     * @return the class name
     */
    public ClassDescription get(String category, String hint) {
        Arguments.require(PATTERN_CATEGORY.matcher(category).matches());
        Arguments.require(hint == null || PATTERN_HINT.matcher(hint).matches());
        Tuple<String, String> key = new Tuple<>(category, hint);
        int count = counters.computeIfAbsent(key, k -> new AtomicInteger()).getAndIncrement();
        return new ClassDescription(toClassName(category, hint, count));
    }

    private String toClassName(String category, String hint, int count) {
        Invariants.require(count >= 0);
        return String.format("%s%s.%s_%d", //$NON-NLS-1$
                prefix,
                category,
                hint != null ? hint : "", //$NON-NLS-1$
                count);
    }
}
