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
package com.asakusafw.dag.compiler.model.build;

import java.util.Collection;
import java.util.Set;

import com.asakusafw.dag.utils.common.Arguments;

/**
 * Represents a resolved output.
 */
public class ResolvedOutputInfo {

    private final String id;

    private final Set<ResolvedInputInfo> downstreams;

    /**
     * Creates a new instance.
     * @param id the output ID
     * @param downstreams the downstream inputs
     */
    public ResolvedOutputInfo(String id, Collection<? extends ResolvedInputInfo> downstreams) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(downstreams);
        this.id = id;
        this.downstreams = Arguments.freezeToSet(downstreams);
    }

    /**
     * Returns the id.
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the downstream inputs.
     * @return the downstream inputs
     */
    public Set<ResolvedInputInfo> getDownstreams() {
        return downstreams;
    }
}
