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

import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;

/**
 * {@link ValueSerDe} and {@link KeyValueSerDe} class provider.
 */
public interface SerDeProvider {

    /**
     * Returns the ser/de class for the target type.
     * @param target the target type
     * @return the corresponded class description
     * @throws DiagnosticException if error occurred while preparing the ser/de
     */
    ClassDescription getValueSerDe(TypeDescription target);

    /**
     * Returns the ser/de class for the target type.
     * @param target the target type
     * @param group the grouping information
     * @return the corresponded class description
     * @throws DiagnosticException if error occurred while preparing the ser/de
     */
    ClassDescription getKeyValueSerDe(TypeDescription target, Group group);
}
