package com.epam.bdcc.serde;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

public class SparkKryoHTMRegistrator implements KryoRegistrator {

    public SparkKryoHTMRegistrator() {
    }

    @Override
    public void registerClasses(Kryo kryo) {
        // the rest HTM.java internal classes support Persistence API
        // (with preSerialize/postDeserialize methods),
        // therefore we'll create the serializers which will use HTMObjectInput/HTMObjectOutput
        // (wrappers on top of fast-serialization)
        // which WILL call the preSerialize/postDeserialize
        SparkKryoHTMSerializer.registerSerializers(kryo);
    }
}