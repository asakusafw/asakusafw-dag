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

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.util.function.Supplier;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.utils.common.Lang;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Generates {@link Supplier} class.
 */
public class SupplierGenerator {

    /**
     * Generates {@link Supplier} class.
     * @param source the supplying type
     * @param target the generating class name
     * @return the generated class data
     */
    public ClassData generate(ClassDescription source, ClassDescription target) {
        ClassWriter writer = newWriter(target, Object.class, Supplier.class);
        defineEmptyConstructor(writer, Object.class);
        Lang.let(writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "get",
                Type.getMethodDescriptor(typeOf(Object.class)),
                null,
                new String[0]), v -> {
                    v.visitVarInsn(Opcodes.ALOAD, 0);
                    v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                            target.getInternalName(),
                            "get",
                            Type.getMethodDescriptor(typeOf(source)),
                            false);
                    v.visitInsn(Opcodes.ARETURN);
                    v.visitMaxs(0, 0);
                    v.visitEnd();
                });
        Lang.let(writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "get",
                Type.getMethodDescriptor(typeOf(source)),
                null,
                new String[0]), v -> {
                    getNew(v, source);
                    v.visitInsn(Opcodes.ARETURN);
                    v.visitMaxs(0, 0);
                    v.visitEnd();
                });
        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }
}
