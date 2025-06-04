import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointInputFormat extends InputFormat<Object, Point2DWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        int numSplits = context.getConfiguration().getInt("randompoint.splits", 1);
        int pointsPerSplit = context.getConfiguration().getInt("randompoint.pointspersplit", 1000);
        List<InputSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(new FakeInputSplit(pointsPerSplit));
        }
        return splits;
    }

    @Override
    public RecordReader<Object, Point2DWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RandomPointReader();
    }
}
