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

import java.util.function.Supplier;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * {@link Supplier} class provider.
 */
@FunctionalInterface
public interface SupplierProvider {

    /**
     * Returns the supplier class for the target type.
     * @param target the target type
     * @return the corresponded class description
     * @throws DiagnosticException if error occurred while preparing the provider
     */
    ClassDescription getSupplier(TypeDescription target);
}
