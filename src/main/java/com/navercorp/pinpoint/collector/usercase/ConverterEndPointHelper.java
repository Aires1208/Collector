package com.navercorp.pinpoint.collector.usercase;

import com.google.common.base.Optional;
import zipkin.*;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Created by root on 17-1-24.
 */
public class ConverterEndPointHelper {
    private static final String DEFAULT_SERVICE = "unknown_service";
    private static final int DEFAULT_LOCAL_HOST_IP = 2130706433;
    private Span span;
    private Map<Long, List<Span>> spanIdMap = newHashMap();

    public ConverterEndPointHelper(Span span, Map<Long, List<Span>> spanIdMap) {
        this.span = span;
        this.spanIdMap = spanIdMap;
    }

    public Endpoint getEndPoint() {
        Endpoint annotationEndPoint = getAnnotationEndPoint();
        if (null != annotationEndPoint) {
            return annotationEndPoint;
        }

        Endpoint binaryEndPoint = getBinaryEndPoint();
        if (null != binaryEndPoint) {
            return binaryEndPoint;
        }

        Optional<Long> parentId = Optional.fromNullable(span.parentId);
        List<Span> parents = this.spanIdMap.get(parentId.or(-1L));
        if (parents != null && !parents.isEmpty()) {
            for (Span parent : parents) {
                this.span = parent;
                return getEndPoint();
            }
        }

        return Endpoint.create(DEFAULT_SERVICE, DEFAULT_LOCAL_HOST_IP, -1);
    }

    private Endpoint getAnnotationEndPoint() {
        if (!span.annotations.isEmpty()) {
            for (Annotation annotation : span.annotations) {
                if (null != annotation.endpoint) {
                    return annotation.endpoint;
                }
            }
        }

        return null;
    }

    private Endpoint getBinaryEndPoint() {
        if (!span.binaryAnnotations.isEmpty()) {
            for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
                if (null != binaryAnnotation.endpoint
                        && binaryAnnotation.key.equals(Constants.LOCAL_COMPONENT)) {
                    return binaryAnnotation.endpoint;
                }
            }
        }

        return null;
    }
}
