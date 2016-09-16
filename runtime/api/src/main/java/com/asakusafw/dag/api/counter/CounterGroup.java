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
package com.asakusafw.dag.api.counter;

import java.util.List;

/**
 * Represents a group of counter.
 */
@FunctionalInterface
public interface CounterGroup {

    /**
     * Returns the actual count for the target column.
     * @param column the target column
     * @return the actual count
     */
    long getCount(Column column);

    /**
     * Represents a column meta-data of {@link CounterGroup}.
     * @since 0.1.0
     * @version 0.2.0
     */
    interface Column {

        /**
         * Returns the description of this column.
         * @return the description of this column
         */
        String getDescription();

        /**
         * Returns the index text of this column.
         * @return the index text
         * @since 0.2.0
         */
        default String getIndexText() {
            return String.format("?.%s", getDescription()); //$NON-NLS-1$
        }
    }

    /**
     * Represents a category of {@link CounterGroup}.
     * @param <T> the type of member {@link CounterGroup}
     */
    interface Category<T extends CounterGroup> {

        /**
         * Returns the description of the target {@link CounterGroup}.
         * @return the description
         */
        String getDescription();

        /**
         * Returns the scope of the target {@link CounterGroup}.
         * @return the scope
         */
        Scope getScope();

        /**
         * Returns the set available columns in the target {@link CounterGroup}.
         * @return the available columns
         */
        List<Column> getColumns();

        /**
         * Creates a new {@link CounterGroup} which is member in this category.
         * @return the created {@link CounterGroup}
         */
        T newInstance();
    }

    /**
     * Represents a scope of {@link CounterGroup}.
     */
    enum Scope {

        /**
         * Graph scoped {@link CounterGroup}.
         */
        GRAPH,

        /**
         * Vertex scoped {@link CounterGroup}.
         */
        VERTEX,
    }
}
