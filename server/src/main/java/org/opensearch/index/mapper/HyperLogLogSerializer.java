/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class for serializing and deserializing HyperLogLog++ sketches
 * for storage in the HyperLogLog field type.
 *
 * @opensearch.internal
 */
public final class HyperLogLogSerializer {

    private static final byte VERSION = 1;
    private static final int HEADER_SIZE = 9; // version(1) + precision(4) + algorithm(1) + reserved(3)

    private HyperLogLogSerializer() {
        // Utility class
    }

    /**
     * Serialize a HyperLogLog++ sketch to bytes
     */
    public static byte[] serialize(HyperLogLogPlusPlus hll, long bucketOrd) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write header
            out.writeByte(VERSION);
            out.writeInt(hll.precision());
            
            // Determine which algorithm is being used
            boolean isLinearCounting = hll.getAlgorithm(bucketOrd);
            out.writeBoolean(isLinearCounting);
            
            // Reserved bytes for future use
            out.writeByte((byte) 0);
            out.writeByte((byte) 0);
            out.writeByte((byte) 0);

            if (isLinearCounting) {
                serializeLinearCounting(out, hll, bucketOrd);
            } else {
                serializeHyperLogLog(out, hll, bucketOrd);
            }

            return out.bytes().toBytesRef().bytes;
        }
    }

    /**
     * Deserialize bytes to a HyperLogLog++ sketch
     */
    public static HyperLogLogPlusPlus deserialize(byte[] data) throws IOException {
        if (data.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid HyperLogLog data: too short");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        
        // Read header
        byte version = buffer.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported HyperLogLog version: " + version);
        }

        int precision = buffer.getInt();
        boolean isLinearCounting = buffer.get() != 0;
        
        // Skip reserved bytes
        buffer.get();
        buffer.get();
        buffer.get();

        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

        try {
            if (isLinearCounting) {
                deserializeLinearCounting(buffer, hll, 0);
            } else {
                deserializeHyperLogLog(buffer, hll, 0);
            }
            return hll;
        } catch (Exception e) {
            hll.close();
            throw e;
        }
    }

    /**
     * Merge two serialized HyperLogLog++ sketches
     */
    public static byte[] merge(byte[] sketch1, byte[] sketch2) throws IOException {
        if (sketch1 == null && sketch2 == null) {
            return null;
        }
        if (sketch1 == null) {
            return sketch2;
        }
        if (sketch2 == null) {
            return sketch1;
        }

        try (HyperLogLogPlusPlus hll1 = deserialize(sketch1);
             HyperLogLogPlusPlus hll2 = deserialize(sketch2)) {
            
            if (hll1.precision() != hll2.precision()) {
                throw new IllegalArgumentException(
                    "Cannot merge HyperLogLog sketches with different precisions: " 
                    + hll1.precision() + " vs " + hll2.precision()
                );
            }

            // Create a new HLL for the merged result
            try (HyperLogLogPlusPlus merged = new HyperLogLogPlusPlus(hll1.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1)) {
                merged.merge(0, hll1, 0);
                merged.merge(0, hll2, 0);
                return serialize(merged, 0);
            }
        }
    }

    /**
     * Get the cardinality estimate from serialized HyperLogLog++ data
     */
    public static long getCardinality(byte[] data) throws IOException {
        try (HyperLogLogPlusPlus hll = deserialize(data)) {
            return hll.cardinality(0);
        }
    }

    /**
     * Get the precision from serialized HyperLogLog++ data
     */
    public static int getPrecision(byte[] data) {
        if (data.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid HyperLogLog data: too short");
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        buffer.get(); // skip version
        return buffer.getInt();
    }

    private static void serializeLinearCounting(BytesStreamOutput out, HyperLogLogPlusPlus hll, long bucketOrd) throws IOException {
        // Get the linear counting iterator
        var iterator = hll.getLinearCounting(bucketOrd);
        
        // Write the number of elements
        out.writeVInt(iterator.size());
        
        // Write each hash value
        while (iterator.next()) {
            out.writeInt(iterator.value());
        }
    }

    private static void serializeHyperLogLog(BytesStreamOutput out, HyperLogLogPlusPlus hll, long bucketOrd) throws IOException {
        // Get the HyperLogLog iterator
        var iterator = hll.getHyperLogLog(bucketOrd);
        
        // Calculate the number of registers (2^precision)
        int numRegisters = 1 << hll.precision();
        
        // Write each register value
        for (int i = 0; i < numRegisters; i++) {
            if (iterator.next()) {
                out.writeByte(iterator.value());
            } else {
                out.writeByte((byte) 0);
            }
        }
    }

    private static void deserializeLinearCounting(ByteBuffer buffer, HyperLogLogPlusPlus hll, long bucketOrd) throws IOException {
        // Read number of elements
        int numElements = readVInt(buffer);
        
        // Read and collect each hash value
        for (int i = 0; i < numElements; i++) {
            int hash = buffer.getInt();
            hll.collect(bucketOrd, hash);
        }
    }

    private static void deserializeHyperLogLog(ByteBuffer buffer, HyperLogLogPlusPlus hll, long bucketOrd) throws IOException {
        // Calculate the number of registers
        int numRegisters = 1 << hll.precision();
        
        // Read each register value and add to HLL
        for (int register = 0; register < numRegisters; register++) {
            byte runLen = buffer.get();
            if (runLen > 0) {
                // Use reflection or package-private access to add run length
                // This is a simplified approach - in practice you'd need proper access to HLL internals
                // For now, we'll simulate by collecting appropriate hash values
                long hash = ((long) register << (64 - hll.precision())) | (1L << (64 - hll.precision() - runLen));
                hll.collect(bucketOrd, hash);
            }
        }
    }

    private static int readVInt(ByteBuffer buffer) {
        byte b = buffer.get();
        if (b >= 0) {
            return b;
        }
        int i = b & 0x7F;
        b = buffer.get();
        i |= (b & 0x7F) << 7;
        if (b >= 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7F) << 14;
        if (b >= 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x7F) << 21;
        if (b >= 0) {
            return i;
        }
        b = buffer.get();
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) {
            return i;
        }
        throw new IllegalArgumentException("Invalid vInt");
    }

    /**
     * Create a new HyperLogLog++ sketch from a set of hash values
     */
    public static byte[] createFromHashes(long[] hashes, int precision) throws IOException {
        try (HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1)) {
            for (long hash : hashes) {
                hll.collect(0, hash);
            }
            return serialize(hll, 0);
        }
    }

    /**
     * Create a new empty HyperLogLog++ sketch
     */
    public static byte[] createEmpty(int precision) throws IOException {
        try (HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1)) {
            return serialize(hll, 0);
        }
    }
}
