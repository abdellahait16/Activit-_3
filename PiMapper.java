import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PiMapper extends Mapper<Object, Point2DWritable, Text, LongWritable> {
    private static final Text IN = new Text("in");
    private static final Text TOTAL = new Text("total");
    private LongWritable one = new LongWritable(1);

    @Override
    protected void map(Object key, Point2DWritable value, Context context) throws IOException, InterruptedException {
        context.write(TOTAL, one);
        double x = value.getPoint().getX();
        double y = value.getPoint().getY();
        if (x * x + y * y <= 1.0) {
            context.write(IN, one);
        }
    }
}
