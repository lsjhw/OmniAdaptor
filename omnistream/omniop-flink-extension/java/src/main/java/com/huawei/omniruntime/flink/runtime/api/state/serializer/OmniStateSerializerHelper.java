package com.huawei.omniruntime.flink.runtime.api.state.serializer;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerJson;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerKey;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse.OmniParseFactory;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniStateMetaSerializerInfo;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * OmniStateSerializerFactory
 *
 * @description omni state serializer factory
 */

public class OmniStateSerializerHelper {
    private static final Logger LOG = LoggerFactory.getLogger(OmniStateSerializerHelper.class);

    public static final String STREAM_TASK_PRIVATE_KEY_MAIN_OPERATOR = "mainOperator";

    // recursion depth max
    protected static final int DEPTH_MAX = 100;
    // recursion depth start
    protected static final int DEPTH_START = 0;
    // recursion depth interval
    protected static final int DEPTH_INTERVAL = 1;

    /**
     * disabled new instantiation
     */
    private OmniStateSerializerHelper() {
    }

    /**
     * build serializer info
     *
     * @param taskKey             task key
     * @param stateTableName      state table name
     * @param typeCode            type code
     * @param serializerMap       serializer map
     * @param executionConfig     execution config
     * @param userCodeClassLoader userCodeClassLoader
     */
    public static OmniStateMetaSerializerInfo.Builder buildSerializerInfo(String taskKey,
                                                                          String stateTableName,
                                                                          int typeCode,
                                                                          Map<String, String> serializerMap,
                                                                          ExecutionConfig executionConfig,
                                                                          ClassLoader userCodeClassLoader) {
        try {
            StateMetaInfoSnapshot.BackendStateType backendStateType = StateMetaInfoSnapshot.BackendStateType.byCode(typeCode);
            if (null == backendStateType) {
                LOG.warn("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, typeCode : {} undefined.", taskKey, stateTableName, typeCode);
                return null;
            }
            if (null == serializerMap || serializerMap.isEmpty()) {
                LOG.warn("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, serializer is null or empty.", taskKey, stateTableName);
                return null;
            }
            if (null == executionConfig) {
                LOG.warn("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, executionConfig is null.", taskKey, stateTableName);
                return null;
            }
            if (null == userCodeClassLoader) {
                LOG.warn("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, userCodeClassLoader is null.", taskKey, stateTableName);
                return null;
            }
            OmniStateMetaSerializerInfo.Builder builder = OmniStateMetaSerializerInfo.builder();
            builder.backendStateType(backendStateType);
            for (Map.Entry<String, String> item : serializerMap.entrySet()) {
                if (null == item) {
                    LOG.warn("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, item is null.", taskKey, stateTableName);
                    continue;
                }
                OmniSerializerKey serializerKey = OmniSerializerKey.get(item.getKey());
                if (null == serializerKey) {
                    LOG.warn("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, key : {} undefined.", taskKey, stateTableName, item.getKey());
                    continue;
                }
                if (StringUtils.isEmpty(item.getValue())) {
                    // special deal
                    if (StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.equals(backendStateType)
                            && OmniSerializerKey.NAMESPACE_SERIALIZER.equals(serializerKey)) {
                        builder.serializerGroup(OmniSerializerKey.NAMESPACE_SERIALIZER.getMetaKeyStr(), VoidNamespaceSerializer.INSTANCE);
                        LOG.warn("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, key : {}, item value is empty, default VoidNamespaceSerializer.",
                                taskKey, stateTableName, item.getKey());
                    } else {
                        LOG.warn("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, key : {}, item value is empty.",
                                taskKey, stateTableName, item.getKey());
                    }
                    continue;
                }
                StateDescriptor<?, ?> stateDescriptor = buildStateDescriptor(taskKey, stateTableName, item.getKey(), item.getValue(), executionConfig, userCodeClassLoader);
                if (null == stateDescriptor) {
                    LOG.warn("method : buildStateDescriptor -> taskKey : {}, stateTableName : {}, key : {} stateDescriptor is null.",
                            taskKey, stateTableName, item.getKey());
                    continue;
                }
                builder.serializerGroup(serializerKey.getMetaKeyStr(), stateDescriptor.getSerializer());
            }

            return builder;
        } catch (Exception e) {
            LOG.error("method : buildSerializerInfo -> taskKey : {}, stateTableName : {}, exception",
                    taskKey, stateTableName, e);
            throw new GeneralRuntimeException(e);
        }
    }

    /**
     * build state descriptor
     *
     * @param taskKey             task key
     * @param stateTableName      state table name
     * @param key                 key
     * @param jsonStr             json str
     * @param executionConfig     execution config
     * @param userCodeClassLoader userCodeClassLoader
     * @return StateDescriptor<?, ?>
     */
    public static StateDescriptor<?, ?> buildStateDescriptor(String taskKey,
                                                             String stateTableName,
                                                             String key,
                                                             String jsonStr,
                                                             ExecutionConfig executionConfig,
                                                             ClassLoader userCodeClassLoader) {
        try {
            if (StringUtils.isEmpty(jsonStr)) {
                return null;
            }
            if (null == userCodeClassLoader) {
                return null;
            }
            if (null == executionConfig) {
                return null;
            }
            OmniNativeSerializerJsonInfo info = convert(jsonStr, userCodeClassLoader, DEPTH_START);
            if (null == info) {
                LOG.warn("method : buildStateDescriptor -> taskKey : {}, stateTableName : {}, key : {} convert fail.",
                        taskKey, stateTableName, key);
                return null;
            }
            OmniParseFactory factory = OmniParseFactory.build(info.getSerializerType());
            if (null == factory) {
                LOG.warn("method : buildStateDescriptor -> taskKey : {}, stateTableName : {}, key : {}, type : {} can't build factory.",
                        taskKey, stateTableName, key, info.getSerializerType());
                return null;
            }
            StateDescriptor<?, ?> stateDescriptor = factory.buildDescriptorBy(stateTableName, info);
            if (null == stateDescriptor) {
                LOG.warn("method : buildStateDescriptor -> taskKey : {}, stateTableName : {}, key : {}, type : {} stateDescriptor is null.",
                        taskKey, stateTableName, key, info.getSerializerType());
                return null;
            }
            stateDescriptor.initializeSerializerUnlessSet(executionConfig);

            return stateDescriptor;
        } catch (Exception e) {
            LOG.error("method : buildStateDescriptor -> taskKey : {}, stateTableName : {}, key : {}, exception",
                    taskKey, stateTableName, key, e);
            throw new GeneralRuntimeException(e);
        }
    }

    /**
     * convert
     *
     * @param jsonStr             json str
     * @param userCodeClassLoader userCodeClassLoader
     * @param depth               depth
     */
    private static OmniNativeSerializerJsonInfo convert(String jsonStr,
                                                        ClassLoader userCodeClassLoader,
                                                        int depth) {
        if (depth > DEPTH_MAX) {
            LOG.warn("method : buildTypeInformationBy -> max recursion depth ({}) exceeded. Input may be malformed or malicious.", DEPTH_MAX);
            return null;
        }
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        OmniNativeSerializerJsonInfo info = new OmniNativeSerializerJsonInfo();
        Map<String, Object> map = JsonHelper.fromJson(jsonStr, new TypeReference<Map<String, Object>>() {
        });
        if (null == map) {
            throw new GeneralRuntimeException(String.format("jsonStr : %s convert fail.", jsonStr));
        }

        if (null == map.get(OmniSerializerJson.TYPE.getKey())) {
            throw new GeneralRuntimeException(String.format("%s is null.", OmniSerializerJson.TYPE.getKey()));
        }
        info.setType((Integer) map.get(OmniSerializerJson.TYPE.getKey()));

        OmniSerializerType serializerType = OmniSerializerType.get(info.getType());
        if (null == serializerType) {
            throw new GeneralRuntimeException(String.format("type : %s undefined.", info.getType()));
        }
        info.setSerializerType(serializerType);

        if (null != map.get(OmniSerializerJson.ELEMENT_TYPE.getKey())) {
            info.setElementType((String) map.get(OmniSerializerJson.ELEMENT_TYPE.getKey()));
            if (StringUtils.isNotEmpty(info.getElementType())) {
                info.setElementType(info.getElementType().replaceAll(SC.UNDERSCORE, SC.DOT));
                try {
                    info.setElementTypeClazz(Class.forName(info.getElementType(), false, userCodeClassLoader));
                } catch (ClassNotFoundException e) {
                    throw new GeneralRuntimeException(String.format("Could not find class '%s' for unsafe operations.", info.getElementType()), e);
                }
            }
        }

        if (null != map.get(OmniSerializerJson.KEY_SERIALIZER.getKey())) {
            String keySerializerStr = (String) map.get(OmniSerializerJson.KEY_SERIALIZER.getKey());
            if (StringUtils.isNotEmpty(keySerializerStr)) {
                info.setKeySerializer(convert(keySerializerStr, userCodeClassLoader, depth + DEPTH_INTERVAL));
            }
        }

        if (null != map.get(OmniSerializerJson.VALUE_SERIALIZER.getKey())) {
            String valueSerializerStr = (String) map.get(OmniSerializerJson.VALUE_SERIALIZER.getKey());
            if (StringUtils.isNotEmpty(valueSerializerStr)) {
                info.setValueSerializer(convert(valueSerializerStr, userCodeClassLoader, depth + DEPTH_INTERVAL));
            }
        }

        // value is null, not deal
        // fieldNames
        // fieldSerializers

        return info;
    }

    /**
     * build serializer json info
     *
     * @param metaInfo meta info
     * @return Map<String, Object>
     */
    public static Map<String, Object> buildSerializerJsonInfo(StateMetaInfoSnapshot metaInfo) {
        Map<String, Object> metaInfoGroup = new HashMap<>();
        try {
            if (null == metaInfo) {
                LOG.warn("method : buildSerializerJsonInfo -> metaInfo is null.");
                return metaInfoGroup;
            }
            metaInfoGroup = JsonHelper.fromJson(JsonHelper.toJson(metaInfo), new TypeReference<Map<String, Object>>() {
            });
            if (null == metaInfoGroup) {
                LOG.warn("method : buildSerializerJsonInfo -> metaInfo convert fail.");
                return new HashMap<>();
            }
            Map<String, OmniSerializerJsonInfo> serializerGroup = new HashMap<>();
            for (Map.Entry<String, TypeSerializerSnapshot<?>> item : metaInfo.getSerializerSnapshotsImmutable().entrySet()) {// check
                if (null == item) {
                    LOG.warn("method : buildSerializerJsonInfo -> item is null.");
                    continue;
                }
                OmniSerializerKey serializerKey = OmniSerializerKey.getBy(item.getKey());
                if (null == serializerKey) {
                    LOG.warn("method : buildSerializerJsonInfo -> key : {} undefined.", item.getKey());
                    continue;
                }
                if (null == item.getValue()) {
                    // special deal
                    if (StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.equals(metaInfo.getBackendStateType())
                            && OmniSerializerKey.NAMESPACE_SERIALIZER.equals(serializerKey)) {
                        serializerGroup.put(OmniSerializerKey.NAMESPACE_SERIALIZER.getKey(), buildJsonInfo(VoidNamespaceSerializer.INSTANCE));
                        LOG.warn("method : buildSerializerJsonInfo -> key : {}, item value is empty, item value is empty, default VoidNamespaceSerializer.", item.getKey());
                    } else {
                        LOG.warn("method : buildSerializerJsonInfo -> key : {}, item value is empty.", item.getKey());
                    }
                    continue;
                }
                OmniSerializerJsonInfo jsonInfo = buildJsonInfo(item.getValue().restoreSerializer());
                if (null != jsonInfo) {
                    String key = OmniSerializerKey.STATE_SERIALIZER.equals(serializerKey.getMetaKey())
                            ? OmniSerializerKey.STATE_SERIALIZER.getKey()
                            : serializerKey.getKey();
                    serializerGroup.put(key, jsonInfo);
                }
            }

            metaInfoGroup.put("serializer", serializerGroup);
            metaInfoGroup.put("keySerializer", new OmniSerializerJsonInfo());

            return metaInfoGroup;
        } catch (Exception e) {
            LOG.error("method : buildSerializerJsonInfo -> exception", e);
            throw new GeneralRuntimeException(e);
        }
    }

    /**
     * build json info
     *
     * @param typeSerializer type serializer
     * @return OmniSerializerJsonInfo
     */
    public static OmniSerializerJsonInfo buildJsonInfo(TypeSerializer<?> typeSerializer) {
        try {
            if (typeSerializer == null) {
                LOG.warn("method : buildJsonInfo -> typeSerializer is null.");
                return null;
            }
            OmniSerializerType serializerType = OmniSerializerType.get(typeSerializer.getClass());
            if (null == serializerType) {
                LOG.warn("method : buildJsonInfo -> serializerClazz : {} undefined.", typeSerializer.getClass());
                return null;
            }
            OmniParseFactory factory = OmniParseFactory.build(serializerType);
            if (null == factory) {
                LOG.warn("method : buildJsonInfo -> type : {} can't build factory.", serializerType);
                return null;
            }
            return factory.buildSerializerJsonBy(typeSerializer, serializerType);
        } catch (Exception e) {
            LOG.error("method : buildJsonInfo -> exception", e);
            throw new GeneralRuntimeException("buildSerializerInfo exception ; " + e.getMessage(), e);
        }
    }

    /**
     * get state backend key serializer
     *
     * @param headOperator head operator
     * @return TypeSerializer<?>
     */
    private static TypeSerializer<?> getStateBackendKeySerializer(StreamOperator<?> headOperator) {
        if (null == headOperator) {
            LOG.warn("method : getStateBackendKeySerializer -> headOperator is null.");
            return null;
        }
        KeyedStateBackend<?> keyedBackend = ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
        if (null == keyedBackend) {
            LOG.warn("method : getStateBackendKeySerializer -> keyedBackend is null.");
            return null;
        }
        return keyedBackend.getKeySerializer();
    }

    /**
     * get state backend key serializer
     *
     * @param streamTask stream task
     * @return TypeSerializer<?>
     */
    public static TypeSerializer<?> getStateBackendKeySerializer(StreamTask<?, ?> streamTask) {
        if (null == streamTask) {
            LOG.warn("method : getStateBackendKeySerializer -> streamTask is null.");
            return null;
        }
        StreamOperator<?> headOperator = ReflectionUtils.retrievePrivateField(streamTask, STREAM_TASK_PRIVATE_KEY_MAIN_OPERATOR);

        return getStateBackendKeySerializer(headOperator);
    }

    /**
     * get state backend key serializer
     *
     * @param taskKey             task key
     * @param executionConfig     execution config
     * @param userCodeClassLoader userCodeClassLoader
     * @return TypeSerializer<?>
     */
    public static TypeSerializer<?> getStateBackendKeySerializer(String taskKey,
                                                                 Map<String, Object> metaInfo,
                                                                 ExecutionConfig executionConfig,
                                                                 ClassLoader userCodeClassLoader) {
        String name = (String) metaInfo.get("name");
        String keySerializerJsonStr = metaInfo.get("keySerializer").toString();
        String key = OmniSerializerKey.KEY_SERIALIZER.getMetaKeyStr();
        StateDescriptor<?, ?> stateDescriptor = OmniStateSerializerHelper.buildStateDescriptor(
                taskKey,
                name,
                OmniSerializerKey.KEY_SERIALIZER.getMetaKeyStr(),
                keySerializerJsonStr,
                executionConfig,
                userCodeClassLoader);
        if (null == stateDescriptor) {
            LOG.warn("method : getStateBackendKeySerializer -> taskKey : {}, stateTableName : {}, key : {}, stateDescriptor is null.",
                    taskKey, name, key);
            return null;
        }

        return stateDescriptor.getSerializer();
    }
}
