package org.elasticsearch.discovery.consul;

import consul.model.DiscoveryResult;
import org.assertj.core.api.JUnitJupiterSoftAssertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;


class ConsulUnicastHostsProviderTest {

    private static final String IPV4 = "192.168.1.1";
    private static final String IPV6 = "2001:1db8:85a3:1234:1234:8a2e:1370:7334";
    private static final int PORT = 8080;

    @RegisterExtension
    JUnitJupiterSoftAssertions softly = new JUnitJupiterSoftAssertions();

    @TestFactory
    Collection<DynamicTest> addressConverterTests() {
        return Arrays.asList(
            DynamicTest.dynamicTest("Proper IPv4 address test",
                () -> goodCase(IPV4, "%s:%d")),

            DynamicTest.dynamicTest("Proper IPv6 address test",
                () -> goodCase(IPV6, "[%s]:%d")),

            DynamicTest.dynamicTest("Proper IPv4 address test",
                () -> badCase("wrong address")),

            DynamicTest.dynamicTest("Proper IPv4 address test",
                () -> badCase("192.168.1"))

        );
    }

    private void goodCase(String stringAddress, String resultFormat) {
        Optional<String> addressOptional = ConsulUnicastHostsProvider
            .buildProperAddress(new DiscoveryResult(stringAddress, PORT));
        softly.assertThat(addressOptional).as("Address should not be empty")
            .isPresent()
            .containsInstanceOf(String.class)
            .as("Address should be properly converted")
            .contains(String.format(resultFormat, stringAddress, PORT));
    }

    private void badCase(String stringAddress) {
        Optional<String> address = ConsulUnicastHostsProvider.buildProperAddress(new DiscoveryResult(stringAddress, PORT));
        softly.assertThat(address.isPresent()).as("Should return empty optional").isFalse();
    }
}
