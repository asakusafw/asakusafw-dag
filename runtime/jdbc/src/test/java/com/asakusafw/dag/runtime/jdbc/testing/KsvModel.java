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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.MessageFormat;

import com.asakusafw.dag.utils.common.Lang;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.runtime.value.ValueOption;

@SuppressWarnings({ "javadoc", "deprecation" })
public class KsvModel implements DataModel<KsvModel> {

    private final LongOption keyOption = new LongOption();

    private final DecimalOption sortOption = new DecimalOption();

    private final StringOption valueOption = new StringOption();

    public KsvModel() {
        this(0, BigDecimal.ZERO, "");
    }

    public KsvModel(KsvModel copy) {
        copyFrom(copy);
    }

    public KsvModel(long key, BigDecimal sort, String value) {
        keyOption.modify(key);
        sortOption.modify(sort);
        valueOption.modify(value);
    }

    public LongOption getKeyOption() {
        return keyOption;
    }

    public DecimalOption getSortOption() {
        return sortOption;
    }

    public StringOption getValueOption() {
        return valueOption;
    }

    public long getKey() {
        return keyOption.get();
    }

    public void setKey(long key) {
        keyOption.modify(key);
    }

    public BigDecimal getSort() {
        return sortOption.or(null);
    }

    public void setSort(BigDecimal sort) {
        sortOption.modify(sort);
    }

    public String getValue() {
        return valueOption.or((String) null);
    }

    public void setValue(String value) {
        valueOption.modify(value);
    }

    @Override
    public void reset() {
        try {
            for (Method method : getClass().getMethods()) {
                if (method.getParameterCount() != 0
                        || ValueOption.class.isAssignableFrom(method.getReturnType()) == false) {
                    continue;
                }
                ValueOption<?> value = (ValueOption<?>) method.invoke(this);
                value.setNull();
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public final void copyFrom(KsvModel other) {
        try {
            for (Method method : getClass().getMethods()) {
                if (method.getParameterCount() != 0
                        || ValueOption.class.isAssignableFrom(method.getReturnType()) == false) {
                    continue;
                }
                ValueOption from = (ValueOption<?>) method.invoke(other);
                ValueOption to = (ValueOption<?>) method.invoke(this);
                to.copyFrom(from);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public int hashCode() {
        return Lang.hashCode(keyOption, sortOption, valueOption);
    }

    @Override
    public boolean equals(Object obj) {
        return Lang.equals(this, obj,
                KsvModel::getKeyOption,
                KsvModel::getSortOption,
                KsvModel::getValueOption);
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
