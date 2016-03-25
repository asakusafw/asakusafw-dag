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

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.adapter.ObjectCopier;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Generates {@link ObjectCopier} class.
 */
public class ObjectCopierGenerator {

    /**
     * Generates {@link ObjectCopier} class.
     * @param source the supplying type
     * @param target the generating class name
     * @return the generated class data
     */
    public ClassData generate(TypeDescription source, ClassDescription target) {
        ClassWriter writer = AsmUtil.newWriter(target, Object.class, ObjectCopier.class);
        defineEmptyConstructor(writer, Object.class);
        defineNew(writer, source);
        defineNewWithBuffer(writer, source);
        return new ClassData(target, writer::toByteArray);
    }

    private void defineNew(ClassWriter writer, TypeDescription source) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "newCopy",
                Type.getMethodDescriptor(typeOf(Object.class), typeOf(Object.class)),
                null,
                null);
        LocalVarRef input = cast(v, 1, source);
        getNew(v, source);
        LocalVarRef target = putLocalVar(v, Type.OBJECT, 2);
        generateBody(v, source, target, input);
    }

    private void defineNewWithBuffer(ClassWriter writer, TypeDescription source) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "newCopy",
                Type.getMethodDescriptor(typeOf(Object.class), typeOf(Object.class), typeOf(Object.class)),
                null,
                null);
        LocalVarRef input = cast(v, 1, source);
        LocalVarRef target = cast(v, 2, source);
        generateBody(v, source, target, input);
    }

    private void generateBody(MethodVisitor v, TypeDescription type, LocalVarRef target, LocalVarRef source) {
        target.load(v);
        source.load(v);
        copyDataModel(v, type);

        target.load(v);
        v.visitInsn(Opcodes.ARETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }
}
