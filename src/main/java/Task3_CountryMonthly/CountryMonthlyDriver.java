package Task3_CountryMonthly;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * CountryMonthlyDriver - Orchestrates the country-specific monthly sales job.
 */
public class CountryMonthlyDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountryMonthlyDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Country Monthly Sales");
        
        job.setJarByClass(CountryMonthlyDriver.class);
        job.setMapperClass(CountryMonthlyMapper.class);
        job.setCombinerClass(CountryMonthlyReducer.class); // Performance optimization
        job.setReducerClass(CountryMonthlyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
