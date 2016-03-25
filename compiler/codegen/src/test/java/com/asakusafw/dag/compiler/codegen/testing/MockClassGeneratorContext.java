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
package com.asakusafw.dag.compiler.codegen.testing;

import com.asakusafw.dag.compiler.codegen.BasicSupplierProvider;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.SupplierProvider;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.testing.MockDataModelLoader;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Mock {@link ClassGeneratorContext}.
 */
public class MockClassGeneratorContext implements ClassGeneratorContext {

    private final ClassLoader classLoader;

    private final DataModelLoader dataModelLoader;

    private final SupplierProvider supplierProvider;

    private final ResourceContainer resourceContainer;

    /**
     * Creates a new instance.
     * @param classLoader the class loader
     * @param resourceContainer the resource container
     */
    public MockClassGeneratorContext(ClassLoader classLoader, ResourceContainer resourceContainer) {
        this.classLoader = classLoader;
        this.resourceContainer = resourceContainer;
        this.dataModelLoader = new MockDataModelLoader(classLoader);
        this.supplierProvider = new BasicSupplierProvider(
                c -> c.dump(resourceContainer),
                s -> new ClassDescription("testing." + s + "Supplier")); //$NON-NLS-1$
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public DataModelLoader getDataModelLoader() {
        return dataModelLoader;
    }

    @Override
    public SupplierProvider getSupplierProvider() {
        return supplierProvider;
    }

    @Override
    public ClassDescription addClassFile(ClassData data) {
        data.dump(resourceContainer);
        return data.getDescription();
    }
}
