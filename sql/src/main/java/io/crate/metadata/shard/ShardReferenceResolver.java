/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.shard;

import com.google.common.collect.ImmutableMap;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.LiteralNestableInput;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardMinLuceneVersionExpression;
import io.crate.expression.reference.sys.shard.ShardNumDocsExpression;
import io.crate.expression.reference.sys.shard.ShardPartitionIdentExpression;
import io.crate.expression.reference.sys.shard.ShardPartitionOrphanedExpression;
import io.crate.expression.reference.sys.shard.ShardPathExpression;
import io.crate.expression.reference.sys.shard.ShardPrimaryExpression;
import io.crate.expression.reference.sys.shard.ShardRecoveryExpression;
import io.crate.expression.reference.sys.shard.ShardRelocatingNodeExpression;
import io.crate.expression.reference.sys.shard.ShardRoutingStateExpression;
import io.crate.expression.reference.sys.shard.ShardSizeExpression;
import io.crate.expression.reference.sys.shard.ShardStateExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexParts;
import io.crate.metadata.MapBackedRefResolver;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.Locale;

public class ShardReferenceResolver {

    private static final Logger LOGGER = Loggers.getLogger(ShardReferenceResolver.class);

    public static ReferenceResolver<NestableInput<?>> create(ClusterService clusterService,
                                                             Schemas schemas,
                                                             IndexShard indexShard) {
        ShardId shardId = indexShard.shardId();
        Index index = shardId.getIndex();

        ImmutableMap.Builder<ColumnIdent, NestableInput> builder = ImmutableMap.builder();
        if (IndexParts.isPartitioned(index.getName())) {
            addPartitions(index, schemas, builder);
            builder.put(SysShardsTableInfo.Columns.ORPHAN_PARTITION,
                new ShardPartitionOrphanedExpression(index.getName(), clusterService));
        } else {
            builder.put(SysShardsTableInfo.Columns.ORPHAN_PARTITION, new LiteralNestableInput<>(false));
        }
        IndexParts indexParts = new IndexParts(index.getName());
        builder.put(SysShardsTableInfo.Columns.ID, new LiteralNestableInput<>(shardId.getId()));
        builder.put(SysShardsTableInfo.Columns.SIZE, new ShardSizeExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.NUM_DOCS, new ShardNumDocsExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.PRIMARY, new ShardPrimaryExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.RELOCATING_NODE,
            new ShardRelocatingNodeExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.SCHEMA_NAME,
            new LiteralNestableInput<>(new BytesRef(indexParts.getSchema())));
        builder.put(SysShardsTableInfo.Columns.STATE, new ShardStateExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.ROUTING_STATE, new ShardRoutingStateExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.TABLE_NAME,
            new LiteralNestableInput<>(new BytesRef(indexParts.getTable())));
        builder.put(SysShardsTableInfo.Columns.PARTITION_IDENT,
            new ShardPartitionIdentExpression(shardId));
        builder.put(SysShardsTableInfo.Columns.PATH, new ShardPathExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.BLOB_PATH, new LiteralNestableInput<>(null));
        builder.put(
            SysShardsTableInfo.Columns.MIN_LUCENE_VERSION,
            new ShardMinLuceneVersionExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.RECOVERY, new ShardRecoveryExpression(indexShard));
        builder.put(SysShardsTableInfo.Columns.NODE, new NodeNestableInput(clusterService.localNode()));
        return new MapBackedRefResolver(builder.build());
    }

    private static void addPartitions(Index index,
                                      Schemas schemas,
                                      ImmutableMap.Builder<ColumnIdent, NestableInput> builder) {
        PartitionName partitionName;
        try {
            partitionName = PartitionName.fromIndexOrTemplate(index.getName());
        } catch (IllegalArgumentException e) {
            throw new UnhandledServerException(String.format(Locale.ENGLISH,
                "Unable to load PARTITIONED BY columns from partition %s", index.getName()), e);
        }
        RelationName relationName = partitionName.relationName();
        try {
            DocTableInfo info = schemas.getTableInfo(relationName);
            if (!schemas.isOrphanedAlias(info)) {
                assert info.isPartitioned() : "table must be partitioned";
                int i = 0;
                int numPartitionedColumns = info.partitionedByColumns().size();

                List<BytesRef> partitionValue = partitionName.values();
                assert partitionValue.size() ==
                       numPartitionedColumns : "invalid number of partitioned columns";
                for (Reference partitionedInfo : info.partitionedByColumns()) {
                    builder.put(
                        partitionedInfo.column(),
                        new LiteralNestableInput<>(partitionedInfo.valueType().value(partitionValue.get(i)))
                    );
                    i++;
                }
            } else {
                LOGGER.error("Orphaned partition '{}' with missing table '{}' found", index, relationName.fqn());
            }
        } catch (ResourceUnknownException e) {
            LOGGER.error("Orphaned partition '{}' with missing table '{}' found", index, relationName.fqn());
        }
    }
}
