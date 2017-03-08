package com.navercorp.pinpoint.collector.usercase;

import com.navercorp.pinpoint.common.topo.domain.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

public class CalculateTopoLineMergeUserCase {
    private TopoLine first;
    private TopoLine second;

    public CalculateTopoLineMergeUserCase(TopoLine first, TopoLine second) {
        this.first = first;
        this.second = second;
    }

    public TopoLine execute() {
        return mergeTopoLineAndList();
    }

    private TopoLine mergeTopoLineAndList() {
        List<XNode> resultXNodeList;
        List<XLink> resultXLinkList;
        if (null == first) {
            resultXNodeList = second.getXNodes();
            resultXLinkList = second.getXLinks();
        }else {
            List<XNode>  xNodesOfTopoLine = first.getXNodes();
            List<XLink> xLinksOfTopoLine = first.getXLinks();
            resultXNodeList=mergeTwoXNodeList(second.getXNodes(),xNodesOfTopoLine);
            resultXLinkList = mergeTwoXLinkList(second.getXLinks(),xLinksOfTopoLine);
        }
        return new TopoLine(resultXNodeList, resultXLinkList);
    }

    private List<XNode> mergeTwoXNodeList(List<XNode> oneList, List<XNode> otherList) {
        Map<String, XNode> tempXnode = newHashMap();
        for (XNode xnode : oneList) {
            tempXnode.put(xnode.getName(), xnode);
        }
        for (XNode xnode : otherList) {
            if (null == tempXnode.get(xnode.getName())) {
                tempXnode.put(xnode.getName(), xnode);
            } else {
                XNode temp = tempXnode.get(xnode.getName());
                XNode tempNode = new XNodeBuilder().Name(xnode.getName())
                        .Calls(xnode.getCalls() + temp.getCalls())
                        .Errors(xnode.getErrors() + temp.getErrors())
                        .Response(xnode.getResponseTime() + temp.getResponseTime())
                        .ServiceType(temp.getServiceType())
                        .build();
                tempXnode.put(xnode.getName(), tempNode);
            }
        }
        return newArrayList(tempXnode.values());
    }

    private List<XLink> mergeTwoXLinkList(List<XLink> oneList, List<XLink> otherList) {
        Map<String, XLink> xLinkMap = newHashMap();
        for (XLink xLink : oneList) {
            xLinkMap.put(xLink.getFrom() + xLink.getTo(), xLink);
        }
        for (XLink xLink : otherList) {
            String key = xLink.getFrom() + xLink.getTo();
            if (null == xLinkMap.get(key)) {
                xLinkMap.put(key, xLink);
            } else {
                XLink tempXLink = xLinkMap.get(key);
                XLink mergeResultXLink = new XLinkBuilder().Link(xLink.getFrom(), xLink.getTo())
                        .Calls(xLink.getCalls() + tempXLink.getCalls())
                        .Errors(xLink.getErrors() + tempXLink.getErrors())
                        .Response(xLink.getResponseTime() + tempXLink.getResponseTime())
                        .build();
                xLinkMap.put(key, mergeResultXLink);
            }
        }
        return newArrayList(xLinkMap.values());
    }
}
