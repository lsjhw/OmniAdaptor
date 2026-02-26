package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OmniParseListFactory
 *
 * @description omni parse list factory
 */

public class OmniParseListFactory extends OmniParseFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OmniParseListFactory.class);

    @Override
    public StateDescriptor<?, ?> buildDescriptorBy(String stateTableName, OmniNativeSerializerJsonInfo info) {
        if (!super.check(stateTableName, info)) {
            return null;
        }
        OmniNativeSerializerJsonInfo valueSerializerInfo = info.getValueSerializer();
        if (null == valueSerializerInfo) {
            LOG.warn("method : buildDescriptorBy -> stateTableName : {}, valueSerializerInfo is null.", stateTableName);
            return null;
        }
        TypeInformation<?> elementTypeInfo = buildTypeInformationBy(valueSerializerInfo, DEPTH_START);
        if (null == elementTypeInfo) {
            LOG.warn("method : buildDescriptorBy -> stateTableName : {}, elementTypeInfo is null.", stateTableName);
            return null;
        }

        return new ListStateDescriptor<>(stateTableName, elementTypeInfo);
    }
}
