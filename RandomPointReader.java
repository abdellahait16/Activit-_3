import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointReader extends RecordReader<Object, Point2DWritable> {

    private int numPoints;
    private int generatedPoints = 0;
    private Random random = new Random();
    private Point2DWritable currentPoint = new Point2DWritable();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        numPoints = ((FakeInputSplit) split).getNumPoints();
    }

    @Override
    public boolean nextKeyValue() {
        if (generatedPoints >= numPoints) return false;
        double x = random.nextDouble();
        double y = random.nextDouble();
        currentPoint = new Point2DWritable(x, y);
        generatedPoints++;
        return true;
    }

    @Override
    public Object getCurrentKey() {
        return null;
    }

    @Override
    public Point2DWritable getCurrentValue() {
        return currentPoint;
    }

    @Override
    public float getProgress() {
        return (float) generatedPoints / numPoints;
    }

    @Override
    public void close() {}
}
