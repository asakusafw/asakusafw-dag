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
package com.asakusafw.dag.runtime.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

import com.asakusafw.dag.api.common.Deserializer;
import com.asakusafw.dag.api.common.Serializer;
import com.asakusafw.dag.api.common.TaggedSupplier;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.Invariants;
import com.asakusafw.dag.utils.common.Lang;

/**
 * A {@link TaggedSupplier} of{@link ValueSerDe} for {@link UnionRecord}.
 * @since 0.2.0
 */
public class UnionRecordSerDeSupplier implements TaggedSupplier<ValueSerDe> {

    private final Map<String, List<Supplier<Encoder>>> upstreams = new HashMap<>();

    private final List<Supplier<? extends Deserializer>> downstreams = new ArrayList<>();

    private final Set<String> downstreamNames = new HashSet<>();

    /**
     * Adds an upstream serialization information.
     * @param tags the upstream port tags
     * @param element the element serializer class
     * @return this
     */
    public final UnionRecordSerDeSupplier upstream(Collection<String> tags, Class<? extends ValueSerDe> element) {
        Arguments.requireNonNull(tags);
        Arguments.requireNonNull(element);
        return upstream(tags, () -> Lang.safe(element::newInstance));
    }

    /**
     * Adds an upstream serialization information.
     * @param tags the upstream port tags
     * @param element the element serializer supplier
     * @return this
     */
    public final UnionRecordSerDeSupplier upstream(Collection<String> tags, Supplier<? extends ValueSerDe> element) {
        Arguments.requireNonNull(tags);
        Arguments.requireNonNull(element);
        int index = downstreams.size();
        Set<String> saw = new HashSet<>();
        for (String tag : tags) {
            Invariants.require(saw.contains(tag) == false);
            Invariants.require(downstreamNames.contains(tag) == false);
            upstreams.computeIfAbsent(tag, s -> new ArrayList<>()).add(() -> new Encoder(index, element.get()));
            saw.add(tag);
        }
        downstreams.add(element);
        return this;
    }

    /**
     * Adds a downstream name.
     * @param tag the upstream port tag (nullable)
     * @return this
     */
    public final UnionRecordSerDeSupplier downstream(String tag) {
        Invariants.require(upstreams.containsKey(tag) == false);
        Invariants.require(downstreamNames.contains(tag) == false);
        downstreamNames.add(tag);
        return this;
    }

    @Override
    public ValueSerDe get(String tag) {
        if (upstreams.containsKey(tag)) {
            List<Supplier<Encoder>> elements = upstreams.get(tag);
            if (elements.size() == 1) {
                return elements.get(0).get();
            } else {
                return new Multiplexer(elements.stream()
                        .sequential()
                        .map(Supplier::get)
                        .toArray(Encoder[]::new));
            }
        } else if (downstreamNames.contains(tag)) {
            return new Decoder(downstreams.stream()
                    .sequential()
                    .map(Supplier::get)
                    .toArray(Deserializer[]::new));
        } else {
            throw new NoSuchElementException(MessageFormat.format(
                    "unrecognized tag: {0}",
                    tag));
        }
    }

    private static final class Encoder implements ValueSerDe {

        private final int index;

        private final Serializer element;

        Encoder(int index, Serializer element) {
            this.index = index;
            this.element = element;
        }

        @Override
        public void serialize(Object object, DataOutput output) throws IOException, InterruptedException {
            output.writeInt(index);
            element.serialize(object, output);
        }

        @Override
        public Object deserialize(DataInput input) throws IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }
    }

    private static final class Multiplexer implements ValueSerDe {

        private final Encoder[] elements;

        Multiplexer(Encoder[] elements) {
            this.elements = elements;
        }

        @Override
        public void serialize(Object object, DataOutput output) throws IOException, InterruptedException {
            Encoder[] es = elements;
            for (Encoder s : es) {
                s.serialize(object, output);
            }
        }

        @Override
        public Object deserialize(DataInput input) throws IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }
    }

    private static final class Decoder implements ValueSerDe {

        private final UnionRecord buffer = new UnionRecord();

        private final Deserializer[] elements;

        Decoder(Deserializer[] elements) {
            this.elements = elements;
        }

        @Override
        public void serialize(Object object, DataOutput output) throws IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object deserialize(DataInput input) throws IOException, InterruptedException {
            UnionRecord union = buffer;
            int index = input.readInt();
            union.tag = index;
            union.entity = elements[index].deserialize(input);
            return union;
        }
    }
}
