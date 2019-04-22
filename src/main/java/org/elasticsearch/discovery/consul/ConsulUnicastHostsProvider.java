package org.elasticsearch.discovery.consul;
/**
 * Copyright © 2015 Lithium Technologies, Inc. All rights reserved subject to the terms of
 * the MIT License located at
 *
 *
 * LICENSE FILE DISCLOSURE STATEMENT AND COPYRIGHT NOTICE This LICENSE.txt file sets forth
 * the general licensing terms and attributions for the “Elasticsearch Consul Discovery
 * plugin” project software provided by Lithium Technologies, Inc. (“Lithium”).  This
 * software is based upon certain software files authored by Grant Rodgers in 2015 as part
 * of the “Elasticsearch SRV discovery plugin” project.  As a result, some of the files in
 * this Elasticsearch Consul Discovery plugin” project software are wholly authored by
 * Lithium, some are wholly authored by Grant Rodgers, and others are originally authored
 * by Grant Rodgers and subsequently modified by Lithium.  Files that were either modified
 * or wholly authored by Lithium contain an additional LICENSE.txt file indicating whether
 * they were modified or wholly authored by Lithium.  Any LICENSE.txt files, copyrights or
 * attribution originally included in the files of the original “Elasticsearch SRV
 * discovery plugin” remain unchanged. Copyright Notices The following copyright notice
 * applies to only those files and modifications authored by Lithium: Copyright © 2015
 * Lithium Technologies, Inc.  All rights reserved subject to the terms of the MIT License
 * below. The following copyright notice applies to only those files in the original
 * “Elasticsearch SRV discovery plugin” project software excluding any modifications by
 * Lithium: Copyright (c) 2015 Grant Rodgers License The following MIT License, as
 * originally presented in the “Elasticsearch SRV discovery plugin” project software, also
 * applies to files authored or modified by Lithium.  Your use of the “Elasticsearch
 * Consul Discovery plugin” project software is therefore subject to the terms of the
 * following MIT License.  Except as may be granted herein or by separate express written
 * agreement, this file provides no license to any Lithium patents, trademarks,
 * copyrights, or other intellectual property. MIT License Permission is hereby granted,
 * free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit persons to
 * whom the Software is furnished to do so, subject to the following conditions: The above
 * copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT
 * WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
 * SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 *
 *
 * Created by Jigar Joshi on 8/9/15.
 */

import consul.model.DiscoveryResult;
import consul.service.ConsulService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * Consul unicast host provider class.
 */
public class ConsulUnicastHostsProvider implements UnicastHostsProvider {

    private static Logger logger = LogManager.getLogger(ConsulUnicastHostsProvider.class);

    public static final Setting<String> CONSUL_LOCALWSHOST = Setting.simpleString("discovery.consul.local-ws-host",
        Property.NodeScope);
    public static final Setting<Integer> CONSUL_LOCALWSPORT = Setting.intSetting("discovery.consul.local-ws-port", 8500,
        Property.NodeScope);
    public static final Setting<List<String>> CONSUL_SERVICENAMES = Setting.listSetting("discovery.consul.service-names",
        emptyList(), Function.identity(), Property.NodeScope);
    public static final Setting<String> CONSUL_TAG = Setting.simpleString("discovery.consul.tag", Property.NodeScope);
    public static final Setting<Boolean> CONSUL_HEALTHY = Setting.boolSetting("discovery.consul.healthy", true,
        Property.NodeScope);

    private final TransportService transportService;
    private final ConsulService consulService;
    private final Set<String> consulServiceNames;

    public ConsulUnicastHostsProvider(Settings settings, TransportService transportService) {
        this.transportService = transportService;
        this.consulServiceNames = new HashSet<>(CONSUL_SERVICENAMES.get(settings));
        this.consulService = new ConsulService(
            CONSUL_LOCALWSHOST.get(settings),
            CONSUL_LOCALWSPORT.get(settings),
            CONSUL_TAG.get(settings),
            CONSUL_HEALTHY.get(settings));
    }

    @Override
    public List<TransportAddress> buildDynamicHosts(HostsResolver hostsResolver) {
        logger.debug("Discovering nodes");
        List<TransportAddress> discoNodes = getConsulServices().stream()
            .map(ConsulUnicastHostsProvider::buildProperAddress)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(this::createAddress)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        logger.debug("Using Consul based dynamic discovery nodes {}", discoNodes);

        return discoNodes;
    }

    static Optional<String> buildProperAddress(DiscoveryResult dr) {
        try {
            String properAddress = InetAddresses.toUriString(InetAddresses.forString(dr.getIp()));
            return Optional.of(String.format("%s:%d", properAddress, dr.getPort()));
        } catch (Exception e) {
            logger.error("Can't convert {}:{} to proper address string", dr.getIp(), dr.getPort(), e);
            return Optional.empty();
        }
    }

    private Optional<TransportAddress> createAddress(String address) {
        try {
            TransportAddress transportAddress = transportService.addressesFromString(address, 1)[0];
            logger.debug("Created transport_address {} from {}", transportAddress, address);
            return Optional.of(transportAddress);
        } catch (Exception e) {
            logger.warn("Failed to add address {}", address, e);
            return Optional.empty();
        }
    }

    private Set<DiscoveryResult> getConsulServices() {
        try {
            logger.debug("Starting discovery request");
            long startTime = System.currentTimeMillis();
            Set<DiscoveryResult> discoveryResults = consulService.discoverNodes(consulServiceNames);

            logger.debug("Discovered {} nodes", (discoveryResults != null ? discoveryResults.size() : 0));
            logger.debug("{} ms it took for discovery request", (System.currentTimeMillis() - startTime));
            return discoveryResults;
        } catch (IOException ioException) {
            logger.error("Failed to discover nodes, failed in making consul based discovery", ioException);
        } catch (PrivilegedActionException privilegeException) {
            logger.error("Failed to discover nodes, due to security privileges", privilegeException);
        }
        return Collections.emptySet();
    }
}
