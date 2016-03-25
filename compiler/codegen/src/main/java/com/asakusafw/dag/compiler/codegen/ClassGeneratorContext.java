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

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An abstract super interface of class generator contexts.
 */
public interface ClassGeneratorContext {

    /**
     * Returns the current class loader.
     * @return the current class loader
     */
    ClassLoader getClassLoader();

    /**
     * Returns the current data model loader.
     * @return the current data model loader
     */
    DataModelLoader getDataModelLoader();

    /**
     * Returns the current supplier provider.
     * @return the current supplier provider
     */
    SupplierProvider getSupplierProvider();

    /**
     * Adds a new Java class file.
     * @param data the class file contents
     * @return the target class description
     * @throws DiagnosticException if an error was occurred while adding the class file
     */
    ClassDescription addClassFile(ClassData data);

    /**
     * Forwarding for {@link ClassGeneratorContext}.
     */
    public interface Forward extends ClassGeneratorContext {

        /**
         * Returns the forwarding target.
         * @return the forwarding target
         */
        ClassGeneratorContext getForward();

        @Override
        default ClassLoader getClassLoader() {
            return getForward().getClassLoader();
        }

        @Override
        default DataModelLoader getDataModelLoader() {
            return getForward().getDataModelLoader();
        }

        @Override
        default SupplierProvider getSupplierProvider() {
            return getForward().getSupplierProvider();
        }

        @Override
        default ClassDescription addClassFile(ClassData data) {
            return getForward().addClassFile(data);
        }
    }
}
