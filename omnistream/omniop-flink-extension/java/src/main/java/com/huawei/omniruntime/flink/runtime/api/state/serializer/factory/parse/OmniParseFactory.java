package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniSerializerJsonInfo;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.VoidNamespaceTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * OmniParseFactory
 *
 * @description omni parse factory
 */

public abstract class OmniParseFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OmniParseFactory.class);

    public static final String TYPE_SERIALIZER_PRIVATE_KEY_CLAZZ = "clazz";
    public static final String TYPE_SERIALIZER_PRIVATE_KEY_FIELDS = "fields";
    public static final String TYPE_SERIALIZER_PRIVATE_KEY_FIELD_SERIALIZERS = "fieldSerializers";

    // recursion depth max
    protected static final int DEPTH_MAX = 100;
    // recursion depth start
    protected static final int DEPTH_START = 0;
    // recursion depth interval
    protected static final int DEPTH_INTERVAL = 1;

    /**
     * build
     *
     * @param serializerType serializer type
     * @return OmniParseFactory
     */
    public static OmniParseFactory build(OmniSerializerType serializerType) {
        if (null == serializerType) {
            LOG.warn("method : build -> serializerType is null.");
            return null;
        }
        // default
        OmniParseFactory factory = null;
        if (serializerType.isBasic()) {
            factory = new OmniParseValueFactory();
        } else {
            switch (serializerType) {
                case LIST:
                    factory = new OmniParseListFactory();
                    break;
                case MAP:
                    factory = new OmniParseMapFactory();
                    break;
                case POJO:
                case TUPLE:
                case VOID_NAMESPACE:
                    factory = new OmniParseValueFactory();
                    break;
                case UN_KNOW:
                    break;
                default:
                    LOG.warn("method : build -> serializer type : {} has no deal.", serializerType);
                    break;
            }
        }

        return factory;
    }

    /**
     * build type information by
     *
     * @param info  OmniNativeSerializerJsonInfo
     * @param depth depth
     * @return TypeInformation<?>
     */
    protected TypeInformation<?> buildTypeInformationBy(OmniNativeSerializerJsonInfo info, int depth) {
        // check
        if (depth > DEPTH_MAX) {
            LOG.warn("method : buildTypeInformationBy -> max recursion depth ({}) exceeded. Input may be malformed or malicious.", DEPTH_MAX);
            return null;
        }
        if (null == info) {
            LOG.warn("method : buildTypeInformationBy -> info is null.");
            return null;
        }
        if (null == info.getSerializerType()) {
            LOG.warn("method : buildTypeInformationBy -> serializerType is null.");
            return null;
        }
        if (info.getSerializerType().isBasic()) {
            return BasicTypeInfo.getInfoFor(info.getSerializerType().getClazz());
        } else if (OmniSerializerType.LIST.equals(info.getSerializerType())) {
            // get
            OmniNativeSerializerJsonInfo valueSerializerInfo = info.getValueSerializer();
            // recursion
            TypeInformation<?> elementTypeInfo = (null == valueSerializerInfo)
                    ? TypeInformation.of(Object.class)
                    : buildTypeInformationBy(valueSerializerInfo, depth + DEPTH_INTERVAL);
            // return
            return Types.LIST(elementTypeInfo);
        } else if (OmniSerializerType.MAP.equals(info.getSerializerType())) {
            // get
            OmniNativeSerializerJsonInfo keySerializerInfo = info.getKeySerializer();
            OmniNativeSerializerJsonInfo valueSerializerInfo = info.getValueSerializer();
            // recursion
            TypeInformation<?> keyTypeInfo = (null == keySerializerInfo)
                    ? Types.STRING : buildTypeInformationBy(keySerializerInfo, depth + DEPTH_INTERVAL);
            TypeInformation<?> valueTypeInfo = (null == valueSerializerInfo)
                    ? TypeInformation.of(Object.class) : buildTypeInformationBy(valueSerializerInfo, depth + DEPTH_INTERVAL);
            // return
            return Types.MAP(keyTypeInfo, valueTypeInfo);
        } else if (OmniSerializerType.POJO.equals(info.getSerializerType())) {
            // return
            return Types.POJO(info.getElementTypeClazz());
        } else if (OmniSerializerType.TUPLE.equals(info.getSerializerType())) {
            // return
            return TypeExtractor.createTypeInfo(info.getElementTypeClazz());
        } else if (OmniSerializerType.VOID_NAMESPACE.equals(info.getSerializerType())) {
            // return
            return new VoidNamespaceTypeInfo();
        }

        return null;
    }

    /**
     * build json info by
     *
     * @param typeSerializer type serializer
     * @param serializerType serializer type
     * @param depth          depth
     * @return OmniSerializerJsonInfo
     */
    protected OmniSerializerJsonInfo buildJsonInfoBy(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType, int depth) {
        // check
        if (depth > DEPTH_MAX) {
            LOG.warn("method : buildJsonInfoBy -> max recursion depth ({}) exceeded. Input may be malformed or malicious.", DEPTH_MAX);
            return null;
        }
        if (null == typeSerializer) {
            LOG.warn("method : buildJsonInfoBy -> info is null.");
            return null;
        }
        if (null == serializerType) {
            LOG.warn("method : buildJsonInfoBy -> serializerType is null.");
            return null;
        }
        // build
        OmniSerializerJsonInfo jsonInfo = new OmniSerializerJsonInfo();
        jsonInfo.setSerializerName(typeSerializer.getClass().getName());
        if (serializerType.isBasic()) {
            return jsonInfo;
        } else if (OmniSerializerType.LIST.equals(serializerType)) {
            // convert
            ListSerializer<?> listSerializer = (ListSerializer<?>) typeSerializer;
            // recursion
            OmniSerializerJsonInfo elementSerializerJsonInfo = (null == listSerializer.getElementSerializer())
                    ? null
                    : buildJsonInfoBy(
                    listSerializer.getElementSerializer(),
                    OmniSerializerType.get(listSerializer.getElementSerializer().getClass()),
                    depth + DEPTH_INTERVAL);
            // set
            jsonInfo.setElementSerializer(elementSerializerJsonInfo);
            // return
            return jsonInfo;
        } else if (OmniSerializerType.MAP.equals(serializerType)) {
            // convert
            MapSerializer<?, ?> mapSerializer = (MapSerializer<?, ?>) typeSerializer;
            // recursion
            OmniSerializerJsonInfo keySerializerJsonInfo = (null == mapSerializer.getKeySerializer())
                    ? null
                    : buildJsonInfoBy(
                    mapSerializer.getKeySerializer(),
                    OmniSerializerType.get(mapSerializer.getKeySerializer().getClass()),
                    depth + DEPTH_INTERVAL);
            OmniSerializerJsonInfo valueSerializerJsonInfo = (null == mapSerializer.getValueSerializer())
                    ? null
                    : buildJsonInfoBy(
                    mapSerializer.getValueSerializer(),
                    OmniSerializerType.get(mapSerializer.getValueSerializer().getClass()),
                    depth + DEPTH_INTERVAL);
            // set
            jsonInfo.setKeySerializer(keySerializerJsonInfo);
            jsonInfo.setValueSerializer(valueSerializerJsonInfo);
            // return
            return jsonInfo;
        } else if (OmniSerializerType.POJO.equals(serializerType)) {
            // convert
            PojoSerializer<?> pojoSerializer = (PojoSerializer<?>) typeSerializer;
            // reflect get
            Class<?> clazz = ReflectionUtils.retrievePrivateField(pojoSerializer, TYPE_SERIALIZER_PRIVATE_KEY_CLAZZ);
            Field[] fields = ReflectionUtils.retrievePrivateField(pojoSerializer, TYPE_SERIALIZER_PRIVATE_KEY_FIELDS);
            TypeSerializer<?>[] fieldSerializers = ReflectionUtils.retrievePrivateField(pojoSerializer, TYPE_SERIALIZER_PRIVATE_KEY_FIELD_SERIALIZERS);
            // recursion
            List<String> fieldInfoList = new ArrayList<>();
            if (null != fields) {
                for (Field field : fields) {
                    fieldInfoList.add(field.getName());
                }
            }
            List<OmniSerializerJsonInfo> fieldSerializerInfoList = new ArrayList<>();
            if (null != fieldSerializerInfoList) {
                for (TypeSerializer<?> fieldSerializer : fieldSerializers) {
                    OmniSerializerJsonInfo fieldSerializerJsonInfo = (null == fieldSerializer)
                            ? null
                            : buildJsonInfoBy(
                            fieldSerializer,
                            OmniSerializerType.get(fieldSerializer.getClass()),
                            depth + DEPTH_INTERVAL);
                    fieldSerializerInfoList.add(fieldSerializerJsonInfo);
                }
            }
            // set
            jsonInfo.setClazz(null == clazz ? SC.EMPTY : clazz.getName());
            jsonInfo.setFields(fieldInfoList);
            jsonInfo.setFieldSerializers(fieldSerializerInfoList);
            // return
            return jsonInfo;
        } else if (OmniSerializerType.TUPLE.equals(serializerType)) {
            // convert
            TupleSerializer<?> tupleSerializer = (TupleSerializer<?>) typeSerializer;
            // reflect get
            TypeSerializer<?>[] fieldSerializers = ReflectionUtils.retrievePrivateField(tupleSerializer, TYPE_SERIALIZER_PRIVATE_KEY_FIELD_SERIALIZERS);
            // recursion
            List<OmniSerializerJsonInfo> fieldSerializerInfoList = new ArrayList<>();
            if (null != fieldSerializerInfoList) {
                for (TypeSerializer<?> fieldSerializer : fieldSerializers) {
                    OmniSerializerJsonInfo fieldSerializerJsonInfo = (null == fieldSerializer)
                            ? null
                            : buildJsonInfoBy(
                            fieldSerializer,
                            OmniSerializerType.get(fieldSerializer.getClass()),
                            depth + DEPTH_INTERVAL);
                    fieldSerializerInfoList.add(fieldSerializerJsonInfo);
                }
            }
            // set
            jsonInfo.setFieldSerializers(fieldSerializerInfoList);
            // return
            return jsonInfo;
        } else if (OmniSerializerType.VOID_NAMESPACE.equals(serializerType)) {
            // return
            return jsonInfo;
        }

        return null;
    }

    /**
     * check
     *
     * @param stateTableName state table tame
     * @param info           omniNativeSerializerJsonInfo
     * @return boolean
     */
    protected boolean check(String stateTableName, OmniNativeSerializerJsonInfo info) {
        if (StringUtils.isEmpty(stateTableName)) {
            LOG.warn("method : check -> stateTableName is null or empty.");
            return false;
        }
        if (null == info) {
            LOG.warn("method : check -> info is null.");
            return false;
        }

        return true;
    }

    /**
     * check
     *
     * @param typeSerializer type serializer
     * @param serializerType serializer type
     * @return boolean
     */
    protected boolean check(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType) {
        if (null == typeSerializer) {
            LOG.warn("method : check -> typeSerializer is null.");
            return false;
        }
        if (null == serializerType) {
            LOG.warn("method : check -> serializerType is null.");
            return false;
        }

        return true;
    }

    /**
     * build descriptor by
     *
     * @param stateTableName state table name
     * @param info           omniNativeSerializerJsonInfo
     * @return StateDescriptor
     */
    public abstract StateDescriptor<?, ?> buildDescriptorBy(String stateTableName, OmniNativeSerializerJsonInfo info);

    /**
     * build serializer json by
     *
     * @param typeSerializer type serializer
     * @param serializerType serializer type
     * @return OmniSerializerJsonInfo
     */
    public OmniSerializerJsonInfo buildSerializerJsonBy(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType) {
        // check
        if (!check(typeSerializer, serializerType)) {
            return null;
        }
        // build
        OmniSerializerJsonInfo jsonInfo = buildJsonInfoBy(typeSerializer, serializerType, DEPTH_START);
        if (null == jsonInfo) {
            LOG.warn("method : buildSerializerJsonBy -> serializer type : {}, jsonInfo is null.", serializerType);
            return null;
        }
        return jsonInfo;
    }
}
