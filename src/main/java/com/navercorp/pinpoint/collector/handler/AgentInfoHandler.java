/*
 * Copyright 2014 NAVER Corp.
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

package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.navercorp.pinpoint.collector.dao.AgentInfoDao;
import com.navercorp.pinpoint.collector.dao.ApplicationIndexDao;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import com.navercorp.pinpoint.thrift.dto.TResult;

/**
 * @author emeroad
 * @author koo.taejin
 */
@Service("agentInfoHandler")
public class AgentInfoHandler implements SimpleHandler, RequestResponseHandler {

    private final Logger logger = LoggerFactory.getLogger(AgentInfoHandler.class.getName());

    @Autowired
    private AcceptedTimeService timeService;

    @Autowired
    private AgentInfoFilter agentInfoFilter;

    @Autowired
    private AgentInfoDao agentInfoDao;

    @Autowired
    private ApplicationIndexDao applicationIndexDao;

    @Override
    public void handleSimple(TBase<?, ?> tbase) {
        handleRequest(tbase);
    }

    @Override
    public TBase<?, ?> handleRequest(TBase<?, ?> tbase) {
        timeService.accept();
        if (!(tbase instanceof TAgentInfo)) {
            logger.warn("invalid tbase:{}", tbase);
            // it happens to return null  not only at this BO(Business Object) but also at other BOs.

            return null;
        }

        try {
            TAgentInfo agentInfo = (TAgentInfo) tbase;

            AgentInstance agentInstance = new AgentInstance(agentInfo.getAgentId(), agentInfo.getStartTimestamp());
            long acceptTime = timeService.getAcceptedTime();

            if (agentInfoFilter.needPersistent(agentInstance, acceptTime)) {
                logger.info("Received and stored AgentInfo={}", agentInfo);

                // agent info
                agentInfoDao.insert(agentInfo);

                // for querying agentid using applicationname
                applicationIndexDao.insert(agentInfo);

                agentInfoFilter.persistent(agentInstance, acceptTime);
            } else {
                logger.info("agentInfo:\"{}\" was discarded", agentInfo);
            }


            return new TResult(true);

            // for querying applicationname using agentid
        } catch (Exception e) {
            logger.warn("AgentInfo handle error. Caused:{}", e.getMessage(), e);
            TResult result = new TResult(false);
            result.setMessage(e.getMessage());
            return result;
        }
    }

}
