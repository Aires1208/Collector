package com.navercorp.pinpoint.collector.service;

import com.navercorp.pinpoint.collector.dao.InstanceIndexDao;
import com.navercorp.pinpoint.collector.dao.ServiceIndexDao;
import com.navercorp.pinpoint.collector.dao.TransactionsDao;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventValue;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.service.ServiceTypeRegistryService;
import com.navercorp.pinpoint.common.usercase.CalculateInstanceTopoLineUserCase;
import com.navercorp.pinpoint.common.usercase.CalculateTopoLineUserCase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by root on 16-12-13.
 */
@Service
public class TopoServiceImpl implements TopoService {
    @Autowired
    private ServiceTypeRegistryService registryService;

    @Autowired
    private TransactionsDao transactionsDao;

    @Autowired
    private ServiceIndexDao serviceIndexDao;

    @Autowired
    private InstanceIndexDao instanceIndexDao;

    @Override
    public void updateTopo(TransactionEventValue value, long timestamp) {
        TransactionEventKey key = value.getTransactionEventKey();
        List<SpanBo> spanBos = transactionsDao.selectSpans(key);

        if (spanBos != null && !spanBos.isEmpty()) {
            CalculateTopoLineUserCase serviceUserCase = new CalculateTopoLineUserCase(spanBos, registryService);
            CalculateInstanceTopoLineUserCase instanceUserCase = new CalculateInstanceTopoLineUserCase(spanBos, registryService);

            serviceIndexDao.update(value.getAppName(), timestamp, serviceUserCase.execute());
            instanceIndexDao.update(value.getAppName(), timestamp, instanceUserCase.execute());
        }
    }
}
