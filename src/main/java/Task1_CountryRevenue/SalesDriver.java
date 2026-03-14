package Task1_CountryRevenue;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * SalesDriver - Stage 2
 * 
 * Entry point for the second stage of the pipeline which aggregates sales
 * data filtered in Stage 1. 
 */
public class SalesDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Validate command line arguments
        if (args.length != 2) {
            System.err.println("Usage: SalesDriver <clean_input_path> <final_output_path>");
            return -1;
        }

        // Initialize the Hadoop job configuration
        Job aggregationJob = Job.getInstance(getConf(), "Online Retail Analysis - Stage 2: Sales Aggregation");
        aggregationJob.setJarByClass(SalesDriver.class);

        // Assigning our Map and Reduce logic
        aggregationJob.setMapperClass(SalesMapper.class);
        aggregationJob.setReducerClass(SalesReducer.class);

        // Using the Reducer as a Combiner for local performance optimization
        aggregationJob.setCombinerClass(SalesReducer.class);

        // Defining output data types
        aggregationJob.setOutputKeyClass(Text.class);
        aggregationJob.setOutputValueClass(DoubleWritable.class);

        // Configuring HDFS input and output paths
        FileInputFormat.addInputPath(aggregationJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(aggregationJob, new Path(args[1]));

        // Set to a single reducer to get a consolidated final output file
        aggregationJob.setNumReduceTasks(1);

        // Execute job and print a completion message
        System.out.println("Starting Sales Aggregation job...");
        boolean success = aggregationJob.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Standard ToolRunner boilerplate for parsing configuration
        int exitCode = ToolRunner.run(new SalesDriver(), args);
        System.exit(exitCode);
    }
}
