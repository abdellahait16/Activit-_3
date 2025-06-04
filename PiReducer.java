import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PiReducer extends Reducer<Text, LongWritable, Text, Text> {
    private long inCircle = 0;
    private long total = 0;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        if (key.toString().equals("in")) {
            inCircle = sum;
        } else {
            total = sum;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double pi = 4.0 * inCircle / total;
        context.write(new Text("PI estimate"), new Text(String.valueOf(pi)));
    }
}
