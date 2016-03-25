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
package com.asakusafw.dag.api.common;

import java.io.IOException;

/**
 * An abstract super interface of providing objects.
 */
public interface ObjectCursor {

    /**
     * Advances the cursor and returns whether or not the next object exists.
     * This method may change previously {@link #getObject()} result objects.
     * @throws IOException if I/O error occurred while reading the next object
     * @throws InterruptedException if interrupted while reading the next object
     * @return {@code true} if the next object exists, otherwise {@code false}
     */
    boolean nextObject() throws IOException, InterruptedException;

    /**
     * Returns an object which is on the current cursor position.
     * @return the current object
     * @throws IllegalStateException if the cursor is not on the current object
     * @throws IOException if I/O error occurred while reading the current object
     * @throws InterruptedException if interrupted while reading the current object
     * @see #nextObject()
     */
    Object getObject() throws IOException, InterruptedException;
}
