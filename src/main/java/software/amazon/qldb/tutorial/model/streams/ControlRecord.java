/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package software.amazon.qldb.tutorial.model.streams;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a control record on the QLDB stream. QLDB stream writes control
 * records to indicate its start and completion events.
 */
@JsonDeserialize(using = ControlRecord.Deserializer.class)
public final class ControlRecord implements StreamRecord.StreamRecordPayload {

    private String recordType;

    public ControlRecord(String recordType) {
        this.recordType = recordType;
    }

    @Override
    public String toString() {
        return "ControlRecord{" +
                "recordType='" + recordType + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ControlRecord that = (ControlRecord) o;

        return Objects.equals(recordType, that.recordType);
    }

    @Override
    public int hashCode() {
        int result = recordType != null ? recordType.hashCode() : 0;
        return result * 31;
    }

    public String getRecordType() {
        return recordType;
    }

    public static class Deserializer extends StdDeserializer<ControlRecord> {
        public Deserializer() {
            this(null);
        }

        private Deserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public ControlRecord deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException {
            ObjectCodec codec = jsonParser.getCodec();
            JsonNode node = codec.readTree(jsonParser);
            String controlRecordType = node.get("controlRecordType").textValue();

            return new ControlRecord(controlRecordType);
        }
    }
}
