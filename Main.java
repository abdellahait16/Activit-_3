import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Main <savepoints|estimatepi> <output>");
            System.exit(-1);
        }

        String jobType = args[0];
        String output = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        if (jobType.equals("savepoints")) {
            job.setJobName("Save Points");
            job.setJarByClass(Main.class);
            job.setInputFormatClass(RandomPointInputFormat.class);
            job.setMapperClass(SavePointsMapper.class);
            job.setReducerClass(SavePointsReducer.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job, new Path(output));
        } else if (jobType.equals("estimatepi")) {
            job.setJobName("Estimate PI");
            job.setJarByClass(Main.class);
            job.setInputFormatClass(RandomPointInputFormat.class);
            job.setMapperClass(PiMapper.class);
            job.setReducerClass(PiReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job, new Path(output));
        } else {
            System.err.println("Unknown job type");
            System.exit(-1);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
