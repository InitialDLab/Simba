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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.*;
import org.apache.spark.sql.spatial.*;
import org.apache.spark.sql.spatial.LineSegment;
import org.apache.spark.sql.spatial.Point;
import org.apache.spark.sql.spatial.Polygon;

class KryoShapeSerializer extends Serializer<Shape> {
    private GeometryFactory gf = new GeometryFactory();
    private CoordinateSequenceFactory csFactory = gf.getCoordinateSequenceFactory();

    private static short getTypeInt(Shape o) {
        if (o instanceof Point) return 0;
        else if (o instanceof MBR) return 1;
        else if (o instanceof Circle) return 2;
        else if (o instanceof Polygon) return 3;
        else if (o instanceof LineSegment) return 4;
        else return -1;
    }

    @Override
    public void write(Kryo kryo, Output output, Shape shape) {
        output.writeShort(getTypeInt(shape));
        if (shape instanceof Point) {
            Point obj = (Point) shape;
            output.writeInt(obj.coord().length, true);
            for (double x: obj.coord())
                output.writeDouble(x);
        } else if (shape instanceof MBR) {
            MBR obj = (MBR) shape;
            Point low = obj.low();
            Point high = obj.high();
            output.writeInt(low.coord().length, true);
            for (double x: low.coord())
                output.writeDouble(x);
            for (double x: high.coord())
                output.writeDouble(x);
        } else if (shape instanceof Circle) {
            Circle obj = (Circle) shape;
            Point center = obj.center();
            output.writeInt(center.coord().length, true);
            for (double x : center.coord())
                output.writeDouble(x);
            output.writeDouble(obj.radius());
        } else if (shape instanceof Polygon) {
            Polygon obj = (Polygon) shape;
            com.vividsolutions.jts.geom.Polygon content = obj.content();
            writeCoordSequence(output, content.getExteriorRing().getCoordinateSequence());
            int num_interior_rings = content.getNumInteriorRing();
            output.writeInt(num_interior_rings, true);
            for (int i = 0; i < num_interior_rings; ++i)
                writeCoordSequence(output, content.getInteriorRingN(i).getCoordinateSequence());
        } else if (shape instanceof LineSegment) {
            LineSegment obj = (LineSegment) shape;
            for (double x: obj.start().coord())
                output.writeDouble(x);
            for (double x: obj.end().coord())
                output.writeDouble(x);
        }
    }

    @Override
    public Shape read(Kryo kryo, Input input, Class<Shape> type) {
        int type_int = input.readShort();
        if (type_int == 0) {
            int dim = input.readInt(true);
            double[] coords = new double[dim];
            for (int i = 0; i < dim; ++i)
                coords[i] = input.readDouble();
            return new Point(coords);
        } else if (type_int == 1) {
            int dim = input.readInt(true);
            double[] low = new double[dim];
            double[] high = new double[dim];
            for (int i = 0; i < dim; ++i)
                low[i] = input.readDouble();
            for (int i = 0; i < dim; ++i)
                high[i] = input.readDouble();
            return new MBR(new Point(low), new Point(high));
        } else if (type_int == 2) {
            int dim = input.readInt(true);
            double[] center = new double[dim];
            for (int i = 0; i < dim; ++i)
                center[i] = input.readDouble();
            return new Circle(new Point(center), input.readDouble());
        } else if (type_int == 3) {
            LinearRing exterior_ring = gf.createLinearRing(readCoordSequence(input));
            int num_interior_rings = input.readInt(true);
            if (num_interior_rings == 0) return new Polygon(gf.createPolygon(exterior_ring));
            else {
                LinearRing[] interior_rings = new LinearRing[num_interior_rings];
                for (int i = 0; i < num_interior_rings; ++i)
                    interior_rings[i] = gf.createLinearRing(readCoordSequence(input));
                return new Polygon(gf.createPolygon(exterior_ring, interior_rings));
            }
        } else if (type_int == 4) {
            double[] start = new double[2];
            double[] end = new double[2];
            for (int i = 0; i < 2; ++i)
                start[i] = input.readDouble();
            for (int i = 0; i < 2; ++i)
                end[i] = input.readDouble();
            return new LineSegment(new Point(start), new Point(end));
        }
        return null;
    }

    private CoordinateSequence readCoordSequence(Input input) {
        int n = input.readInt(true);
        CoordinateSequence coords = csFactory.create(n, 2);
        for (int i = 0; i < n; ++i) {
            coords.setOrdinate(i, 0, input.readDouble());
            coords.setOrdinate(i, 1, input.readDouble());
        }
        return coords;
    }

    private void writeCoordSequence(Output output, CoordinateSequence coords) {
        output.writeInt(coords.size(), true);
        for (int i = 0; i < coords.size(); ++i) {
            Coordinate coord = coords.getCoordinate(i);
            output.writeDouble(coord.getOrdinate(0));
            output.writeDouble(coord.getOrdinate(1));
        }
    }
}
