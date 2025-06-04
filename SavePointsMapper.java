import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SavePointsMapper extends Mapper<Object, Point2DWritable, NullWritable, Text> {
    @Override
    protected void map(Object key, Point2DWritable value, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), new Text(value.toString()));
    }
}
