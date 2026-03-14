package Cleaning;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * DataCleaningMapper - Stage 1
 * 
 * This mapper handles the initial preprocessing of the raw Online Retail dataset.
 * It reads the raw CSV and filters out invalid records like cancellations,
 * entries with zero or negative prices, and those with missing country data.
 */
public class DataCleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    // Counters to track the quality of data and identify potential issues
    public enum CleaningCounter {
        HEADER_ROWS,
        VALID_ROWS,
        CANCELLED_ORDERS,
        INVALID_PRICE,
        MISSING_COUNTRY,
        MALFORMED_LINE
    }

    private final Text outputLine = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        // Basic check for empty lines
        if (line.isEmpty()) {
            return;
        }

        // Identify and skip the header row
        if (line.startsWith("InvoiceNo")) {
            context.getCounter(CleaningCounter.HEADER_ROWS).increment(1);
            return;
        }

        // Split the CSV fields - we use a limit of 8 to capture all columns correctly
        String[] fields = line.split(",", 8);
        if (fields.length < 8) {
            context.getCounter(CleaningCounter.MALFORMED_LINE).increment(1);
            return;
        }

        try {
            // Extracting relevant fields: InvoiceNo, Quantity, UnitPrice, Country
            String invoiceNo = fields[0].trim();
            int quantity = Integer.parseInt(fields[3].trim());
            double unitPrice = Double.parseDouble(fields[5].trim());
            String country = fields[7].trim();

            // 1. Filter out cancellations (quantity <= 0 or starts with 'C')
            if (quantity <= 0 || invoiceNo.startsWith("C")) {
                context.getCounter(CleaningCounter.CANCELLED_ORDERS).increment(1);
                return;
            }

            // 2. Filter out invalid prices (we only want positive values)
            if (unitPrice <= 0) {
                context.getCounter(CleaningCounter.INVALID_PRICE).increment(1);
                return;
            }

            // 3. Filter out records missing country information
            if (country.isEmpty() || country.equalsIgnoreCase("unspecified")) {
                context.getCounter(CleaningCounter.MISSING_COUNTRY).increment(1);
                return;
            }

            // Calculate revenue for the line item
            double revenueValue = quantity * unitPrice;
            // Rounding to 2 decimal places for consistent currency formatting
            double roundedRevenue = Math.round(revenueValue * 100.0) / 100.0;

            // Preparation for temporal analysis: Extract InvoiceDate (Year and Month)
            String invoiceDate = fields[4].trim(); // Format: 2010-12-01 08:26:00
            
            // Prepare the output: Now includes InvoiceDate for downstream temporal tasks
            // Format: InvoiceNo,Country,Revenue,InvoiceDate
            outputLine.set(invoiceNo + "," + country + "," + roundedRevenue + "," + invoiceDate);
            
            // NullWritable as key since we're just filtering and don't need sorting/shuffling yet
            context.write(NullWritable.get(), outputLine);
            context.getCounter(CleaningCounter.VALID_ROWS).increment(1);

        } catch (NumberFormatException e) {
            // Log malformed lines where numeric fields couldn't be parsed
            context.getCounter(CleaningCounter.MALFORMED_LINE).increment(1);
        }
    }
}
