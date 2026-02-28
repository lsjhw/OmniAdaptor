/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.restore;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;

class ByteArrayToIntArraySerializer extends JsonSerializer<byte[]> {
    @Override
    public void serialize(byte[] value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        int[] intArray = new int[value.length];
        for (int i = 0; i < value.length; i++) {
            intArray[i] = value[i];
        }
        gen.writeArray(intArray, 0, intArray.length);
    }
}

public class KeyGroupEntry {
    private final int kvStateId;

    @JsonSerialize(using = ByteArrayToIntArraySerializer.class)
    private final byte[] key;

    @JsonSerialize(using = ByteArrayToIntArraySerializer.class)
    private final byte[] value;

    public KeyGroupEntry(int kvStateId, byte[] key, byte[] value) {
        this.kvStateId = kvStateId;
        this.key = key;
        this.value = value;
    }

    public int getKvStateId() {
        return this.kvStateId;
    }

    public byte[] getKey() {
        return this.key;
    }

    public byte[] getValue() {
        return this.value;
    }
}