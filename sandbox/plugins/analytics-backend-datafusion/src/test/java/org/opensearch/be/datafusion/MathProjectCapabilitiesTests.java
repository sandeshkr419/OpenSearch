/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.substrait.extension.SimpleExtension;

/**
 * Contract test for Group G: every Tier-1 math function and every Tier-2 adapter
 * target is registered as a Scalar project capability on the DataFusion backend.
 * Without this registration {@code OpenSearchProjectRule} drops the function
 * through to a residual project on the coordinator, defeating native pushdown.
 *
 * <p>Also holds {@link #testFpOnlyYamlFunctionsHaveWideningAdapters()}, which derives adapter
 * coverage for floating-point-only functions from the yaml.
 */
public class MathProjectCapabilitiesTests extends OpenSearchTestCase {

    private Set<ScalarFunction> exposedProjectScalars() {
        DataFusionAnalyticsBackendPlugin backendPlugin = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin());
        BackendCapabilityProvider provider = backendPlugin.getCapabilityProvider();
        Set<ScalarFunction> seen = new HashSet<>();
        for (ProjectCapability cap : provider.projectCapabilities()) {
            if (cap instanceof ProjectCapability.Scalar scalar) {
                seen.add(scalar.function());
            }
        }
        return seen;
    }

    public void testMathFunctionsAreProjectCapable() {
        Set<ScalarFunction> projectable = exposedProjectScalars();
        ScalarFunction[] functions = new ScalarFunction[] {
            ScalarFunction.ABS,
            ScalarFunction.ACOS,
            ScalarFunction.ASIN,
            ScalarFunction.ATAN,
            ScalarFunction.ATAN2,
            ScalarFunction.CBRT,
            ScalarFunction.CEIL,
            ScalarFunction.COS,
            ScalarFunction.COT,
            ScalarFunction.DEGREES,
            ScalarFunction.EXP,
            ScalarFunction.FLOOR,
            ScalarFunction.LN,
            ScalarFunction.LOG,
            ScalarFunction.LOG10,
            ScalarFunction.LOG2,
            ScalarFunction.PI,
            ScalarFunction.POWER,
            ScalarFunction.RADIANS,
            ScalarFunction.RAND,
            ScalarFunction.ROUND,
            ScalarFunction.SIGN,
            ScalarFunction.SIN,
            ScalarFunction.TAN,
            ScalarFunction.TRUNCATE, };
        for (ScalarFunction f : functions) {
            assertTrue("function not registered as Scalar project capability: " + f, projectable.contains(f));
        }
    }

    public void testAdapterTargetFunctionsAreProjectCapable() {
        Set<ScalarFunction> projectable = exposedProjectScalars();
        ScalarFunction[] functions = new ScalarFunction[] {
            ScalarFunction.COSH,
            ScalarFunction.SINH,
            ScalarFunction.E,
            ScalarFunction.EXPM1,
            ScalarFunction.SCALAR_MAX,
            ScalarFunction.SCALAR_MIN, };
        for (ScalarFunction f : functions) {
            assertTrue("adapter target not registered as Scalar project capability: " + f, projectable.contains(f));
        }
    }

    public void testAdapterTargetFunctionsHaveAdapters() {
        DataFusionAnalyticsBackendPlugin backendPlugin = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin());
        Map<ScalarFunction, ScalarFunctionAdapter> adapters = backendPlugin.getCapabilityProvider().scalarFunctionAdapters();
        assertNotNull("SINH must have an adapter registered", adapters.get(ScalarFunction.SINH));
        assertNotNull("COSH must have an adapter registered", adapters.get(ScalarFunction.COSH));
        assertNotNull("E must have an adapter registered", adapters.get(ScalarFunction.E));
        assertNotNull("EXPM1 must have an adapter registered", adapters.get(ScalarFunction.EXPM1));
        assertNotNull("SCALAR_MAX must have an adapter registered", adapters.get(ScalarFunction.SCALAR_MAX));
        assertNotNull("SCALAR_MIN must have an adapter registered", adapters.get(ScalarFunction.SCALAR_MIN));
        assertNotNull("SIGN must have an adapter registered", adapters.get(ScalarFunction.SIGN));
        assertNotNull("RADIANS must have an adapter registered", adapters.get(ScalarFunction.RADIANS));
        assertNotNull("DEGREES must have an adapter registered", adapters.get(ScalarFunction.DEGREES));
    }

    /** MINUS must be project-capable because Expm1Adapter rewrites {@code expm1(x)} to {@code MINUS(EXP(x), 1)}. */
    public void testMinusIsProjectCapableForExpm1AdapterOutput() {
        Set<ScalarFunction> projectable = exposedProjectScalars();
        assertTrue("MINUS must be project-capable because Expm1Adapter emits it", projectable.contains(ScalarFunction.MINUS));
    }

    // ── Derived adapter-parity guard ───────────────────────────────────────
    // A yaml-declared substrait extension needs an exact signature match, so a function whose
    // declared impls are all fp32/fp64 needs a widening adapter to accept an integer operand.
    // Derived from the yaml rather than listed by hand, so a newly declared function is covered.

    /** Functions whose yaml name differs from the enum name; a missing rename fails the guard below rather than skipping. */
    private static final Map<ScalarFunction, String> YAML_NAME_OVERRIDES = Map.of(
        ScalarFunction.RAND,
        "random",
        ScalarFunction.TRUNCATE,
        "trunc"
    );

    private static final Set<String> FLOATING_POINT_TYPES = Set.of("fp32", "fp64");

    public void testFpOnlyYamlFunctionsHaveWideningAdapters() throws Exception {
        DataFusionAnalyticsBackendPlugin backendPlugin = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin());
        BackendCapabilityProvider provider = backendPlugin.getCapabilityProvider();
        Map<ScalarFunction, ScalarFunctionAdapter> adapters = provider.scalarFunctionAdapters();
        Map<String, List<SimpleExtension.ScalarFunctionVariant>> yamlByName = yamlScalarsByName();

        List<String> missing = new ArrayList<>();
        for (ScalarFunction function : exposedProjectScalars()) {
            String yamlName = YAML_NAME_OVERRIDES.getOrDefault(function, function.name().toLowerCase(Locale.ROOT));
            List<SimpleExtension.ScalarFunctionVariant> variants = yamlByName.get(yamlName);
            if (variants == null) {
                // Not yaml-declared — resolved natively, so no exact-signature constraint applies.
                continue;
            }
            if (isFloatingPointOnly(variants) && adapters.get(function) == null) {
                missing.add(yamlName + " (" + function + ")");
            }
        }

        assertEquals(
            "these functions declare floating-point-only impls in opensearch_scalar_functions.yaml, so an "
                + "integer operand has no matching signature and needs a widening ScalarFunctionAdapter "
                + "(see NumericToDoubleAdapter / IntegerRoundingCastAdapter)",
            List.of(),
            missing
        );
    }

    /** Guards the mapping itself: a broken enum-to-yaml name would make the guard above skip silently. */
    public void testKnownFpOnlyFunctionsAreActuallyResolved() throws Exception {
        Map<String, List<SimpleExtension.ScalarFunctionVariant>> yamlByName = yamlScalarsByName();
        for (ScalarFunction function : List.of(ScalarFunction.CBRT, ScalarFunction.COT)) {
            String yamlName = function.name().toLowerCase(Locale.ROOT);
            List<SimpleExtension.ScalarFunctionVariant> variants = yamlByName.get(yamlName);
            assertNotNull(yamlName + " must be declared in opensearch_scalar_functions.yaml", variants);
            assertTrue(yamlName + " must declare floating-point-only impls, found " + keysOf(variants), isFloatingPointOnly(variants));
        }
    }

    private static Map<String, List<SimpleExtension.ScalarFunctionVariant>> yamlScalarsByName() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(MathProjectCapabilitiesTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection scalars = SimpleExtension.load(List.of("/opensearch_scalar_functions.yaml"));
            return scalars.scalarFunctions().stream().collect(Collectors.groupingBy(SimpleExtension.ScalarFunctionVariant::name));
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    /**
     * True when every declared impl takes at least one argument and all argument types are
     * {@code fp32}/{@code fp64}. Types come from the variant key, formatted
     * {@code <name>:<type>_<type>...}; zero-argument constants such as {@code pi} return false.
     */
    private static boolean isFloatingPointOnly(List<SimpleExtension.ScalarFunctionVariant> variants) {
        if (variants.isEmpty()) {
            return false;
        }
        for (SimpleExtension.ScalarFunctionVariant variant : variants) {
            if (variant.args().isEmpty()) {
                return false;
            }
            String key = variant.key();
            String signature = key.substring(key.indexOf(':') + 1);
            for (String argType : signature.split("_")) {
                if (FLOATING_POINT_TYPES.contains(argType) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Set<String> keysOf(List<SimpleExtension.ScalarFunctionVariant> variants) {
        return variants.stream().map(SimpleExtension.ScalarFunctionVariant::key).collect(Collectors.toSet());
    }
}
