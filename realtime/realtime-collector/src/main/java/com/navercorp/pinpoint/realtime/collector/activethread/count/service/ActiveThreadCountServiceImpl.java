/*
 * Copyright 2023 NAVER Corp.
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
package com.navercorp.pinpoint.realtime.collector.activethread.count.service;

import com.navercorp.pinpoint.common.server.cluster.ClusterKey;
import com.navercorp.pinpoint.realtime.collector.service.AgentCommandService;
import com.navercorp.pinpoint.realtime.dto.ATCDemand;
import com.navercorp.pinpoint.realtime.dto.ATCSupply;
import com.navercorp.pinpoint.thrift.dto.command.TCmdActiveThreadCount;
import com.navercorp.pinpoint.thrift.dto.command.TCmdActiveThreadCountRes;
import org.apache.thrift.TBase;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Objects;

/**
 * @author youngjin.kim2
 */
class ActiveThreadCountServiceImpl implements ActiveThreadCountService {

    private final AgentCommandService commandService;
    private final ATCSupplyFactory supplyFactory;
    private final long demandDurationMillis;

    ActiveThreadCountServiceImpl(
            AgentCommandService commandService,
            long demandDurationMillis,
            long minPublishTermNanos
    ) {
        this.commandService = Objects.requireNonNull(commandService, "commandService");
        this.supplyFactory = new ATCSupplyFactory(minPublishTermNanos);
        this.demandDurationMillis = demandDurationMillis;
    }

    @Override
    public Flux<ATCSupply> requestAsync(ATCDemand demand) {
        final TCmdActiveThreadCount command = new TCmdActiveThreadCount();

        final ClusterKey clusterKey = new ClusterKey(
                demand.getApplicationName(),
                demand.getAgentId(),
                demand.getStartTimestamp()
        );

        final Flux<TBase<?, ?>> resFlux = commandService.requestStream(clusterKey, command, this.demandDurationMillis);
        if (resFlux == null) {
            return null;
        }

        final Flux<ATCSupply> supplyFlux = resFlux
                .mapNotNull(ActiveThreadCountServiceImpl::deserialize)
                .mapNotNull(values -> this.supplyFactory.build(clusterKey, values));

        final Flux<ATCSupply> notifier = Flux.just(supplyFactory.buildConnectionNotifier(clusterKey));

        return Flux.concat(notifier, supplyFlux);
    }

    private static List<Integer> deserialize(TBase<?, ?> data) {
        if (data instanceof  TCmdActiveThreadCountRes) {
            return ((TCmdActiveThreadCountRes) data).getActiveThreadCount();
        } else {
            return null;
        }
    }

}
