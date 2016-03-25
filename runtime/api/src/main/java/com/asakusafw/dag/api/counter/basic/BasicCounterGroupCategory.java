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
package com.asakusafw.dag.api.counter.basic;

import java.text.MessageFormat;
import java.util.List;
import java.util.function.Supplier;

import com.asakusafw.dag.api.counter.CounterGroup;
import com.asakusafw.dag.api.counter.CounterGroup.Scope;
import com.asakusafw.dag.utils.common.Arguments;

/**
 * A basic implementation of {@link com.asakusafw.dag.api.counter.CounterGroup.Category}.
 * @param <T> the type of target {@link CounterGroup}
 */
public class BasicCounterGroupCategory<T extends CounterGroup> implements CounterGroup.Category<T> {

    private final String description;

    private final Scope scope;

    private final List<CounterGroup.Column> columns;

    private final Supplier<? extends T> supplier;

    /**
     * Creates a new instance.
     * @param description the description
     * @param scope the scope of the target {@link CounterGroup}
     * @param columns the description of the target {@link CounterGroup}
     * @param supplier the set available columns in the target {@link CounterGroup}
     */
    public BasicCounterGroupCategory(
            String description,
            Scope scope,
            List<? extends CounterGroup.Column> columns,
            Supplier<? extends T> supplier) {
        Arguments.requireNonNull(description);
        Arguments.requireNonNull(scope);
        Arguments.requireNonNull(columns);
        Arguments.requireNonNull(supplier);
        this.description = description;
        this.scope = scope;
        this.columns = Arguments.freeze(columns);
        this.supplier = supplier;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Scope getScope() {
        return scope;
    }

    @Override
    public List<CounterGroup.Column> getColumns() {
        return columns;
    }

    @Override
    public T newInstance() {
        return supplier.get();
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "CounterGroup.Descriptor({0}: {1})",
                description,
                columns);
    }
}
