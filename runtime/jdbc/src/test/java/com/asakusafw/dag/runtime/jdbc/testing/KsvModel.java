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
package com.asakusafw.dag.runtime.jdbc.testing;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.Objects;

@SuppressWarnings("javadoc")
public class KsvModel {

    private long key;

    private BigDecimal sort;

    private String value;

    public KsvModel() {
        this(0, BigDecimal.ZERO, "");
    }

    public KsvModel(KsvModel copy) {
        this(copy.key, copy.sort, copy.value);
    }

    public KsvModel(long key, BigDecimal sort, String value) {
        this.key = key;
        this.sort = sort;
        this.value = value;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public BigDecimal getSort() {
        return sort;
    }

    public void setSort(BigDecimal sort) {
        this.sort = sort;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Long.hashCode(key);
        result = prime * result + Objects.hashCode(sort);
        result = prime * result + Objects.hashCode(value);
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
        KsvModel other = (KsvModel) obj;
        if (key != other.key) {
            return false;
        }
        if (!Objects.equals(sort, other.sort)) {
            return false;
        }
        if (!Objects.equals(value, other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "({0}, {1}, {2})",
                getKey(),
                getSort(),
                getValue());
    }
}
