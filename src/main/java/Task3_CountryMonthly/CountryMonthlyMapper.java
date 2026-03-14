package Task3_CountryMonthly;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * CountryMonthlyMapper - Processes cleaned data to extract Country-Month Revenue.
 */
public class CountryMonthlyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    
    private final Text compositeKey = new Text();
    private final DoubleWritable revenue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        
        // Expected from Stage 1: InvoiceNo,Country,Revenue,InvoiceDate
        if (parts.length >= 4) {
            String country = parts[1].trim();
            String rawDate = parts[3].trim();
            
            if (rawDate.length() >= 7) {
                String yearMonth = rawDate.substring(0, 7); 
                // Create a composite key like "United Kingdom_2010-12"
                compositeKey.set(country + "_" + yearMonth);
                
                try {
                    double revValue = Double.parseDouble(parts[2].trim());
                    revenue.set(revValue);
                    context.write(compositeKey, revenue);
                } catch (NumberFormatException e) {
                    // Skip invalid numbers
                }
            }
        }
    }
}
