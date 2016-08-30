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
package com.asakusafw.dag.compiler.jdbc.windgate;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcOperationProcessor;
import com.asakusafw.dag.runtime.jdbc.util.WindGateJdbcDirect;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.Lang;
import com.asakusafw.dag.utils.common.Optionals;
import com.asakusafw.dag.utils.common.Tuple;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Generates {@link JdbcOperationProcessor} using {@link WindGateJdbcDirect} API.
 * @since 0.2.0
 */
public final class WindGateJdbcTruncateProcessorGenerator {

    private static final Type TYPE_BUILDER = typeOf(WindGateJdbcDirect.TruncateBuilder.class);

    private static final String CATEGORY = "jdbc.windgate"; //$NON-NLS-1$

    private static final String HINT = "TruncateProcessor"; //$NON-NLS-1$

    private WindGateJdbcTruncateProcessorGenerator() {
        return;
    }

    /**
     * Generates {@link JdbcOperationProcessor} class.
     * @param context the current context
     * @param spec the target input spec
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, Spec spec) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(spec);
        return generate(context, Arrays.asList(spec));
    }

    /**
     * Generates {@link JdbcOperationProcessor} class.
     * @param context the current context
     * @param specs the target input specs
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, List<Spec> specs) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(specs);
        return generate(context, specs, context.getClassName(CATEGORY, HINT));
    }

    /**
     * Generates {@link JdbcOperationProcessor} class.
     * @param context the current context
     * @param specs the target input specs
     * @param target the target class
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, List<Spec> specs, ClassDescription target) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(specs);
        ClassWriter writer = newWriter(target, JdbcOperationProcessor.class);
        defineEmptyConstructor(writer, JdbcOperationProcessor.class, v -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            for (Spec spec : specs) {
                self.load(v);
                getConst(v, spec.id);

                getConst(v, spec.model.getProfileName());
                getConst(v, spec.model.getTableName());
                getList(v, Lang.project(spec.model.getColumnMappings(), Tuple::left));
                v.visitMethodInsn(Opcodes.INVOKESTATIC,
                        typeOf(WindGateJdbcDirect.class).getInternalName(),
                        "truncate",
                        Type.getMethodDescriptor(TYPE_BUILDER,
                                typeOf(String.class), // profileName
                                typeOf(String.class), // tableName
                                typeOf(List.class)), // columnNames
                        false);
                Lang.forEach(Optionals.of(spec.model.getCustomTruncate()), s -> {
                    getConst(v, s);
                    v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                            TYPE_BUILDER.getInternalName(), "withCustomTruncate", //$NON-NLS-1$
                            Type.getMethodDescriptor(TYPE_BUILDER, typeOf(String.class)),
                            false);
                });
                Lang.forEach(spec.model.getOptions(), s -> {
                    getConst(v, s);
                    v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                            TYPE_BUILDER.getInternalName(), "withOption", //$NON-NLS-1$
                            Type.getMethodDescriptor(TYPE_BUILDER, typeOf(String.class)),
                            false);
                });
                v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        TYPE_BUILDER.getInternalName(), "build", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(Function.class)),
                        false);
                v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        target.getInternalName(), "bind", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(JdbcOperationProcessor.class),
                                typeOf(String.class), typeOf(Function.class)),
                        false);
                v.visitInsn(Opcodes.POP);
            }
        });
        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }


    /**
     * Represents an operation spec for {@link JdbcOperationDriver} for truncating tables.
     * @since 0.2.0
     */
    public static class Spec {

        final String id;

        final WindGateJdbcOutputModel model;

        /**
         * Creates a new instance.
         * @param id the output ID
         * @param model the output model
         */
        public Spec(String id, WindGateJdbcOutputModel model) {
            Arguments.requireNonNull(id);
            Arguments.requireNonNull(model);
            this.id = id;
            this.model = model;
        }
    }
}
