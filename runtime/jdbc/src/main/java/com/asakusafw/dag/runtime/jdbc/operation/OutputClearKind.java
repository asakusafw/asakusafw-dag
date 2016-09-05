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
package com.asakusafw.dag.runtime.jdbc.operation;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.utils.common.Optionals;

/**
 * Represents an operation kind of clearing outputs.
 * @since 0.9.0
 */
public enum OutputClearKind {

    /**
     * Never clear outputs.
     */
    KEEP,

    /**
     * Clear outputs by using {@code DELETE} statement.
     */
    DELETE,

    /**
     * Clear outputs by using {@code TRUNCATE} statement.
     */
    TRUNCATE,
    ;

    static final Logger LOG = LoggerFactory.getLogger(OutputClearKind.class);

    private static final String OPTION_PREFIX = "OUTPUT_CLEAR:";

    /**
     * Returns a symbol of this object.
     * @return the symbol
     */
    public String toOption() {
        return OPTION_PREFIX + name();
    }

    /**
     * Returns this object from a set of options.
     * @param options the options
     * @return the found object
     */
    public static Optional<OutputClearKind> fromOptions(Collection<String> options) {
        Set<OutputClearKind> found = options.stream()
                .filter(Objects::nonNull)
                .filter(s -> s.startsWith(OPTION_PREFIX))
                .map(s -> s.substring(OPTION_PREFIX.length()))
                .flatMap(s -> {
                    try {
                        return Stream.of(OutputClearKind.valueOf(s));
                    } catch (NoSuchElementException e) {
                        LOG.debug("invalid option: {}", s, e);
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toSet());
        if (found.size() == 1) {
            return found.stream().findAny();
        } else {
            return Optionals.empty();
        }
    }
}
