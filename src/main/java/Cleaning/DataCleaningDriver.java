package Cleaning;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * DataCleaningDriver - Stage 1
 * 
 * Configures and launches the data cleaning MapReduce job.
 * This is a map-only job, as no aggregation is required at this stage.
 */
public class DataCleaningDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // Validation for input/output arguments
        if (args.length != 2) {
            System.err.println("Usage: DataCleaningDriver <raw_input_path> <clean_output_path>");
            return -1;
        }

        // Initialize the Hadoop job
        Job cleaningJob = Job.getInstance(getConf(), "Online Retail ETL - Stage 1: Data Cleaning");
        cleaningJob.setJarByClass(DataCleaningDriver.class);

        // Setting up the Mapper - no Reducer is needed for this simple ETL task
        cleaningJob.setMapperClass(DataCleaningMapper.class);
        cleaningJob.setNumReduceTasks(0); // Efficiently skips the shuffle/reduce phase

        // Defining the output key and value classes
        cleaningJob.setOutputKeyClass(NullWritable.class);
        cleaningJob.setOutputValueClass(Text.class);

        // Setting up input and output HDFS paths
        FileInputFormat.addInputPath(cleaningJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(cleaningJob, new Path(args[1]));

        // Execute the job and wait for its completion
        System.out.println("Starting Data Cleaning job...");
        boolean success = cleaningJob.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Runner for the Tool interface
        int exitCode = ToolRunner.run(new DataCleaningDriver(), args);
        System.exit(exitCode);
    }
}
