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

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription.TypeKind;

/**
 * A basic implementation of {@link SupplierProvider}.
 */
public class BasicSupplierProvider implements SupplierProvider {

    private final Consumer<ClassData> consumer;

    private final Function<String, ClassDescription> namer;

    private final Map<ClassDescription, ClassDescription> generated = new HashMap<>();

    /**
     * Creates a new instance.
     * @param consumer the class data consumer
     * @param namer the supplier class name generator
     */
    public BasicSupplierProvider(Consumer<ClassData> consumer, Function<String, ClassDescription> namer) {
        this.consumer = consumer;
        this.namer = namer;
    }

    @Override
    public ClassDescription getSupplier(TypeDescription target) {
        Arguments.requireNonNull(target);
        ReifiableTypeDescription erasure = target.getErasure();
        Arguments.require(erasure.getTypeKind() == TypeKind.CLASS, MessageFormat.format(
                "must be a class type: {0}",
                target));
        return getSupplier((ClassDescription) erasure);
    }

    private ClassDescription getSupplier(ClassDescription target) {
        Arguments.requireNonNull(target);
        return generated.computeIfAbsent(target, c -> {
            ClassDescription gen = namer.apply("serde.kv" + target.getSimpleName()); //$NON-NLS-1$
            ClassData data = new SupplierGenerator().generate(target, gen);
            consumer.accept(data);
            return data.getDescription();
        });
    }
}
