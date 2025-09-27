#!/bin/bash

# Set Java 24
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-24.jdk/Contents/Home

echo "=== HyperLogLog Field Mapper Verification ==="
echo "Java Version:"
$JAVA_HOME/bin/java -version

echo ""
echo "=== Checking if our files exist ==="
echo "HyperLogLogFieldMapper.java:"
ls -la server/src/main/java/org/opensearch/index/mapper/HyperLogLogFieldMapper.java

echo "HyperLogLogSerializer.java:"
ls -la server/src/main/java/org/opensearch/index/mapper/HyperLogLogSerializer.java

echo "HyperLogLogAggregator.java:"
ls -la server/src/main/java/org/opensearch/index/mapper/HyperLogLogAggregator.java

echo "HyperLogLogFieldMapperTests.java:"
ls -la server/src/test/java/org/opensearch/index/mapper/HyperLogLogFieldMapperTests.java

echo "IndicesModule.java (should contain our registration):"
grep -n "HyperLogLogFieldMapper" server/src/main/java/org/opensearch/indices/IndicesModule.java

echo ""
echo "=== Attempting compilation ==="
./gradlew :server:compileJava --no-daemon --console=plain 2>&1 | tail -20

echo ""
echo "=== Attempting test compilation ==="
./gradlew :server:compileTestJava --no-daemon --console=plain 2>&1 | tail -20
