/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dml.upsert;

import io.crate.core.collections.StringObjectMaps;
import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Used apply update expressions to create a new source to index
 *
 * <pre>
 * {@code
 * For updates:
 *
 *  UPDATE t SET x = x + 10
 *
 *      getResult:  {x: 20, y: 30}
 *      updateAssignments: x + 10
 *          (x = Reference)
 *          (10 = Literal)
 *      insertValues: null
 *
 *      resultSource: {x: 30, y: 30}
 *
 *
 * For ON CONFLICT DO UPDATE:
 *
 *  INSERT INTO t VALUES (10) ON CONFLICT .. DO UPDATE SET x = x + excluded.x
 *      getResult: {x: 20, y: 30}
 *      updateAssignments: x + excluded.x
 *          (x = Reference)
 *          (excluded.x = Reference)
 *      insertValues: [10]
 *      
 *      resultSource: {x: 30, y: 30}
 * </pre>
 */
final class UpdateSourceGen {

    private final Evaluator eval;
    private final GeneratedColumns<Map<String, Object>> generatedColumns;
    private final ArrayList<Reference> updateColumns;

    UpdateSourceGen(Functions functions, DocTableInfo table, String[] updateColumns) {
        this.eval = new Evaluator(functions, FromSourceRefResolver.INSTANCE);
        this.updateColumns = new ArrayList<>(updateColumns.length);
        for (String updateColumn : updateColumns) {
            ColumnIdent column = ColumnIdent.fromPath(updateColumn);
            Reference reference = table.getReference(column);
            if (reference == null) {
                this.updateColumns.add(table.getDynamic(column, true));
            } else {
                this.updateColumns.add(reference);
            }
        }
        if (table.generatedColumns().isEmpty()) {
            generatedColumns = GeneratedColumns.empty();
        } else {
            generatedColumns = new GeneratedColumns<>(
                new InputFactory(functions),
                InsertSourceGen.Validation.GENERATED_VALUE_MATCH,
                FromSourceRefResolver.INSTANCE,
                this.updateColumns,
                table.generatedColumns()
            );
        }
    }

    BytesReference generateSource(GetResult result, Symbol[] updateAssignments, Object[] insertValues) throws IOException {
        Map<String, Object> source = result.getSource();
        for (int i = 0; i < updateColumns.size(); i++) {
            Reference ref = updateColumns.get(i);
            Object value = eval.process(updateAssignments[i], source).value();
            ColumnIdent column = ref.column();
            StringObjectMaps.mergeInto(source, column.name(), column.path(), value);
            generatedColumns.setNextRow(source);
            generatedColumns.validateValue(ref, value);
        }
        for (Map.Entry<Reference, Input<?>> entry : generatedColumns.toInject()) {
            ColumnIdent column = entry.getKey().column();
            Object value = entry.getValue().value();
            StringObjectMaps.mergeInto(source, column.name(), column.path(), value);
        }
        return XContentFactory.jsonBuilder().map(source).bytes();
    }


    private static class Evaluator extends BaseImplementationSymbolVisitor<Map<String, Object>> {

        private final ReferenceResolver<CollectExpression<Map<String, Object>, ?>> refResolver;

        private Evaluator(Functions functions,
                          ReferenceResolver<CollectExpression<Map<String, Object>, ?>> refResolver) {
            super(functions);
            this.refResolver = refResolver;
        }

        @Override
        public Input<?> visitReference(Reference symbol, Map<String, Object> source) {
            CollectExpression<Map<String, Object>, ?> expr = refResolver.getImplementation(symbol);
            expr.setNextRow(source);
            return expr;
        }
    }
}
