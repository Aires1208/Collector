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

import com.navercorp.pinpoint.collector.dao.TracesDao;
import com.navercorp.pinpoint.thrift.dto.TSpanChunk;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author emeroad
 */
@Service
public class SpanChunkHandler implements SimpleHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private TracesDao traceDao;

    @Override
    public void handleSimple(TBase<?, ?> tbase) {

        if (!(tbase instanceof TSpanChunk)) {
            throw new IllegalArgumentException("unexpected tbase:" + tbase + " expected:" + this.getClass().getName());
        }

        try {
            TSpanChunk spanChunk = (TSpanChunk) tbase;

            if (logger.isDebugEnabled()) {
                logger.debug("Received SpanChunk={}", spanChunk);
            }

            traceDao.insertSpanChunk(spanChunk);

        } catch (Exception e) {
            logger.warn("SpanChunk handle error Caused:{}", e.getMessage(), e);
        }
    }
}