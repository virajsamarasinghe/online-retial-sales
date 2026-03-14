package Task1_CountryRevenue;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * SalesMapper - Stage 2
 * 
 * Processes the cleaned data from Stage 1. 
 * Expected Input: InvoiceNo,Country,Revenue
 * Emits: <Country, Revenue> pairs for aggregation.
 */
public class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    // Simple tracking for valid vs malformed rows
    public enum SalesCounter {
        VALID_ROWS,
        PARSE_ERRORS
    }

    private final Text countryKey = new Text();
    private final DoubleWritable revenueValue = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        // Skip any empty lines if present
        if (line.isEmpty()) {
            return;
        }

        // Parse fields: Stage 1 cleaned output always contains 3 fields
        String[] fields = line.split(",");
        if (fields.length < 3) {
            context.getCounter(SalesCounter.PARSE_ERRORS).increment(1);
            return;
        }

        try {
            // Country is at index 1, Revenue is at index 2
            String country = fields[1].trim();
            double revenue = Double.parseDouble(fields[2].trim());

            // Prepare key/value for emission
            countryKey.set(country);
            revenueValue.set(revenue);

            // Emit for the Reducer/Shuffle phase
            context.write(countryKey, revenueValue);
            context.getCounter(SalesCounter.VALID_ROWS).increment(1);

        } catch (NumberFormatException e) {
            // Handle any potential parsing errors gracefully
            context.getCounter(SalesCounter.PARSE_ERRORS).increment(1);
        }
    }
}
