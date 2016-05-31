package org.apache.spark.sql.catalyst.expressions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.*;
import org.apache.spark.sql.spatial.*;
import org.apache.spark.sql.spatial.Point;
import org.apache.spark.sql.spatial.Polygon;

public class KryoShapeSerializer extends Serializer<Shape> {
    private GeometryFactory gf = new GeometryFactory();
    private CoordinateSequenceFactory csFactory = gf.getCoordinateSequenceFactory();

    public static short getTypeInt(Shape o) {
        if (o instanceof Point) return 0;
        else if (o instanceof MBR) return 1;
        else if (o instanceof Circle) return 2;
        else if (o instanceof Polygon) return 3;
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
