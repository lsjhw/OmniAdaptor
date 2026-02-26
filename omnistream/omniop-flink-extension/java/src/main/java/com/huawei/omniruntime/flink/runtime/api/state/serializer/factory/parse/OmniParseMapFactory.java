package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OmniParseMapFactory
 *
 */

public class OmniParseMapFactory extends OmniParseFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OmniParseMapFactory.class);

    @Override
    public StateDescriptor<?, ?> buildDescriptorBy(String stateTableName, OmniNativeSerializerJsonInfo info) {
        if (!super.check(stateTableName, info)) {
            return null;
        }
        OmniNativeSerializerJsonInfo keySerializerInfo = info.getKeySerializer();
        if (null == keySerializerInfo) {
            LOG.warn("method : buildDescriptorBy -> stateTableName : {}, keySerializerInfo is null.", stateTableName);
            return null;
        }
        OmniNativeSerializerJsonInfo valueSerializerInfo = info.getValueSerializer();
        if (null == valueSerializerInfo) {
            LOG.warn("method : buildDescriptorBy -> stateTableName : {}, valueSerializerInfo is null.", stateTableName);
            return null;
        }
        TypeInformation<?> keyTypeInfo = buildTypeInformationBy(keySerializerInfo, DEPTH_START);
        if (null == keyTypeInfo) {
            LOG.warn("method : buildDescriptorBy -> stateTableName : {}, keyTypeInfo is null.", stateTableName);
            return null;
        }
        TypeInformation<?> valueTypeInfo = buildTypeInformationBy(valueSerializerInfo, DEPTH_START);
        if (null == valueTypeInfo) {
            LOG.warn("method : buildDescriptorBy -> stateTableName : {}, valueTypeInfo is null.", stateTableName);
            return null;
        }

        return new MapStateDescriptor<>(stateTableName, keyTypeInfo, valueTypeInfo);
    }
}
