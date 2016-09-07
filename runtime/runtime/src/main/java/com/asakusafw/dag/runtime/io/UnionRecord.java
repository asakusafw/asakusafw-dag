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

import java.util.Objects;

/**
 * Represents a tagged union record.
 * @since 0.2.0
 */
public class UnionRecord {

    /**
     * The value tag.
     */
    public int tag;

    /**
     * The actual value.
     */
    public Object entity;

    /**
     * Creates a new empty instance.
     */
    public UnionRecord() {
        return;
    }

    /**
     * Creates a new instance.
     * @param tag the value tag
     * @param entity the actual value
     */
    public UnionRecord(int tag, Object entity) {
        this.tag = tag;
        this.entity = entity;
    }

    /**
     * Returns the value tag.
     * @return the value tag
     */
    public int getTag() {
        return tag;
    }

    /**
     * Returns the actual value.
     * @return the actual value
     */
    public Object getEntity() {
        return entity;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + tag;
        result = prime * result + Objects.hashCode(entity);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UnionRecord other = (UnionRecord) obj;
        if (tag != other.tag) {
            return false;
        }
        if (!Objects.equals(entity, other.entity)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("Union(tag=%d, entity=%s)", tag, entity); //$NON-NLS-1$
    }
}
