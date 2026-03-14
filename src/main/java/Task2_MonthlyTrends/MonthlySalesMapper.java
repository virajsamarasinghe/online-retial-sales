package Task2_MonthlyTrends;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * MonthlySalesMapper - Processes cleaned data to extract Global Monthly Revenue.
 */
public class MonthlySalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    
    private final Text monthKey = new Text();
    private final DoubleWritable revenue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        
        // Expected from Stage 1: InvoiceNo,Country,Revenue,InvoiceDate
        // InvoiceDate sample: 2010-12-01 08:26
        if (parts.length >= 4) {
            String rawDate = parts[3].trim();
            if (rawDate.length() >= 7) {
                // Extract "YYYY-MM"
                String yearMonth = rawDate.substring(0, 7); 
                monthKey.set(yearMonth);
                
                try {
                    double revValue = Double.parseDouble(parts[2].trim());
                    revenue.set(revValue);
                    context.write(monthKey, revenue);
                } catch (NumberFormatException e) {
                    // Skip invalid numbers
                }
            }
        }
    }
}
