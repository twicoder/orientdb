package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.util.Date;

public class DateSerializer implements ValueSerializer {
  @Override
  public void toJSON(JsonGenerator generator, Object value) throws IOException {
    generator.writeNumber(((Date) value).getTime());
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    final long ts = parser.getLongValue();
    return new Date(ts);
  }

  @Override
  public String typeId() {
    return SerializerIDs.DATE;
  }
}
