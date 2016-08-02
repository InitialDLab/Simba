/*
 *  Copyright 2016 by Simba Project                                   
 *                                                                            
 *  Licensed under the Apache License, Version 2.0 (the "License");           
 *  you may not use this file except in compliance with the License.          
 *  You may obtain a copy of the License at                                   
 *                                                                            
 *    http://www.apache.org/licenses/LICENSE-2.0                              
 *                                                                            
 *  Unless required by applicable law or agreed to in writing, software       
 *  distributed under the License is distributed on an "AS IS" BASIS,         
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 *  See the License for the specific language governing permissions and       
 *  limitations under the License.                                            
 */

package org.apache.spark.sql.catalyst.expressions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.sql.spatial.*;

import java.io.*;

/**
 * Created by dong on 3/24/16.
 * This is a customized serializer for our Shape objects.
 */
public class ShapeSerializer {
    static private Kryo kryo = new Kryo();

    static {
        kryo.register(Shape.class, new KryoShapeSerializer());
        kryo.register(Point.class, new KryoShapeSerializer());
        kryo.register(MBR.class, new KryoShapeSerializer());
        kryo.register(Polygon.class, new KryoShapeSerializer());
        kryo.register(Circle.class, new KryoShapeSerializer());
        kryo.register(LineSegment.class, new KryoShapeSerializer());
        kryo.addDefaultSerializer(Shape.class, new KryoShapeSerializer());
    }

    public static Shape deserialize(byte[] data) {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        Input input = new Input(in);
        return kryo.readObject(input, Shape.class);
    }

    public static byte[] serialize(Shape o) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, o);
        output.flush();
        return out.toByteArray();
    }
}

