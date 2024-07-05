/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

import java.util.List;

/** Interface for star tree */
public interface StarTree {

    /** Get the root node of the star tree. */
    StarTreeNode getRoot();

    /**
     * Get a list of all dimension names. The node dimension id is the index of the dimension name in
     * this list.
     */
    List<String> getDimensionNames();

}
