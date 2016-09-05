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
package com.asakusafw.dag.runtime.jdbc;

import java.io.IOException;

import com.asakusafw.dag.api.processor.ObjectWriter;

/**
 * Processes output into JDBC.
 * @since 0.2.0
 */
@FunctionalInterface
public interface JdbcOutputDriver {

    /**
     * Returns the max number of threads to write objects into the target resource.
     * @return the max concurrency, or {@code -1} if it is not defined
     */
    default int getMaxConcurrency() {
        return 1;
    }

    /**
     * Returns the output commit granularity.
     * @return the output commit granularity
     */
    default Granularity getGranularity() {
        return Granularity.COARSE;
    }

    /**
     * Initializes the target resource.
     * @throws IOException if I/O error was occurred while initializing the target resource
     * @throws InterruptedException if interrupted while initializing the target resource
     */
    default void initialize() throws IOException, InterruptedException {
        return;
    }

    /**
     * Creates a new writer which accepts each output object.
     * @return the created writer
     * @throws IOException if I/O error was occurred while initializing the writer
     * @throws InterruptedException if interrupted while initializing the writer
     */
    ObjectWriter open() throws IOException, InterruptedException;

    /**
     * Represents a kind of output granularity.
     * @since 0.2.0
     */
    enum Granularity {

        /**
         * Fine grained.
         */
        FINE,

        /**
         * Coarse grained.
         */
        COARSE,
    }
}
