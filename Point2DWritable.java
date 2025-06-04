import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point2DWritable implements Writable {
    private Point2D.Double point;

    public Point2DWritable() {
        this.point = new Point2D.Double();
    }

    public Point2DWritable(double x, double y) {
        this.point = new Point2D.Double(x, y);
    }

    public Point2D.Double getPoint() {
        return point;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(point.getX());
        out.writeDouble(point.getY());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        point.setLocation(in.readDouble(), in.readDouble());
    }

    @Override
    public String toString() {
        return point.getX() + "\t" + point.getY();
    }
}
