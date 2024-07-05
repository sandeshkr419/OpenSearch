/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.MAGIC_MARKER;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.VERSION;
import static org.opensearch.index.compositeindex.datacube.startree.node.OffHeapStarTreeNode.SERIALIZABLE_DATA_SIZE_IN_BYTES;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeBuilderUtils.ALL;

public class StarTreeDataSerializer {

    private static final Logger logger = LogManager.getLogger(StarTreeDataSerializer.class);

    public static long serializeStarTree(IndexOutput indexOutput, StarTreeBuilderUtils.TreeNode rootNode, int numNodes) throws IOException {
        int headerSizeInBytes = computeStarTreeDataHeaderByteSize();
        long totalSizeInBytes = headerSizeInBytes + (long) numNodes * SERIALIZABLE_DATA_SIZE_IN_BYTES;

        logger.info("Star tree size in bytes : {}", totalSizeInBytes);

        writeStarTreeHeader(indexOutput, numNodes);
        writeStarTreeNodes(indexOutput, rootNode);
        return totalSizeInBytes;
    }

    private static int computeStarTreeDataHeaderByteSize() {
        // Magic marker (8), version (4)
        int headerSizeInBytes = 12;

        // For number of nodes.
        headerSizeInBytes += Integer.BYTES;
        return headerSizeInBytes;
    }

    private static void writeStarTreeHeader(IndexOutput output, int numNodes) throws IOException {
        output.writeLong(MAGIC_MARKER);
        output.writeInt(VERSION);
        output.writeInt(numNodes);
    }

    private static void writeStarTreeNodes(IndexOutput output, StarTreeBuilderUtils.TreeNode rootNode) throws IOException {
        Queue<StarTreeBuilderUtils.TreeNode> queue = new LinkedList<>();
        queue.add(rootNode);

        int currentNodeId = 0;
        while (!queue.isEmpty()) {
            StarTreeBuilderUtils.TreeNode node = queue.remove();

            if (node.children == null) {
                writeStarTreeNode(output, node, ALL, ALL);
            } else {

                // Sort all children nodes based on dimension value
                List<StarTreeBuilderUtils.TreeNode> sortedChildren = new ArrayList<>(node.children.values());
                sortedChildren.sort(Comparator.comparingLong(o -> o.dimensionValue));

                int firstChildId = currentNodeId + queue.size() + 1;
                int lastChildId = firstChildId + sortedChildren.size() - 1;
                writeStarTreeNode(output, node, firstChildId, lastChildId);

                queue.addAll(sortedChildren);
            }

            currentNodeId++;
        }
    }

    private static void writeStarTreeNode(IndexOutput output, StarTreeBuilderUtils.TreeNode node, int firstChildId, int lastChildId)
        throws IOException {
        output.writeInt(node.dimensionId);
        output.writeLong(node.dimensionValue);
        output.writeInt(node.startDocId);
        output.writeInt(node.endDocId);
        output.writeInt(node.aggregatedDocId);
        output.writeInt(node.isStarNode == false ? 0 : 1);
        output.writeInt(firstChildId);
        output.writeInt(lastChildId);
    }

}
