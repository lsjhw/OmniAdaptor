package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OmniParseValueFactory
 *
 * @description omni parse value factory
 */

public class OmniParseValueFactory extends OmniParseFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OmniParseValueFactory.class);

    @Override
    public StateDescriptor<?, ?> buildDescriptorBy(String stateTableName, OmniNativeSerializerJsonInfo info) {
        // check
        if (!super.check(stateTableName, info)) {
            return null;
        }
        // build
        TypeInformation<?> typeInfo = buildTypeInformationBy(info, DEPTH_START);
        if (null == typeInfo) {
            LOG.warn("method : buildDescriptorBy -> stateTableName : {}, typeInfo is null.", stateTableName);
            return null;
        }
        return new ValueStateDescriptor<>(stateTableName, typeInfo);
    }
}
