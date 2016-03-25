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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.Tuple;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;

/**
 * A basic implementation of {@link SerDeProvider}.
 */
public class BasicSerDeProvider implements SerDeProvider {

    private final DataModelLoader loader;

    private final Consumer<ClassData> consumer;

    private final Function<String, ClassDescription> namer;

    private final Map<ClassDescription, ClassDescription> valueOnly = new HashMap<>();

    private final Map<Tuple<ClassDescription, Group>, ClassDescription> keyValue = new HashMap<>();

    /**
     * Creates a new instance.
     * @param loader the data model loader
     * @param consumer the target resource sink
     * @param namer the class name generator
     */
    public BasicSerDeProvider(
            DataModelLoader loader,
            Consumer<ClassData> consumer, Function<String, ClassDescription> namer) {
        this.loader = loader;
        this.consumer = consumer;
        this.namer = namer;
    }

    @Override
    public ClassDescription getValueSerDe(TypeDescription target) {
        Arguments.requireNonNull(target);
        return get(loader.load(target));
    }

    private ClassDescription get(DataModelReference target) {
        Arguments.requireNonNull(target);
        ClassDescription source = target.getDeclaration();
        return valueOnly.computeIfAbsent(source, c -> {
            ClassDescription gen = namer.apply("serde.v" + source.getSimpleName()); //$NON-NLS-1$
            ClassData data = new ValueSerDeGenerator().generate(target, gen);
            consumer.accept(data);
            return data.getDescription();
        });
    }

    @Override
    public ClassDescription getKeyValueSerDe(TypeDescription target, Group group) {
        Arguments.requireNonNull(target);
        Arguments.requireNonNull(group);
        DataModelReference ref = loader.load(target);
        return get(ref, group);
    }

    private ClassDescription get(DataModelReference target, Group group) {
        ClassDescription source = target.getDeclaration();
        Tuple<ClassDescription, Group> key = new Tuple<>(source, group);
        return keyValue.computeIfAbsent(key, c -> {
            ClassDescription gen = namer.apply("serde.kv" + source.getSimpleName()); //$NON-NLS-1$
            ClassData data = new KeyValueSerDeGenerator().generate(target, group, gen);
            consumer.accept(data);
            return data.getDescription();
        });
    }
}
