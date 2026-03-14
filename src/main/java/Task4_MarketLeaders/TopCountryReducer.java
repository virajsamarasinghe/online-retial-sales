package Task4_MarketLeaders;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * TopCountryReducer - Finds the country with the maximum revenue for each month.
 */
public class TopCountryReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String topCountry = "";
        double maxRevenue = -1.0;

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length >= 2) {
                String country = parts[0];
                try {
                    double revenue = Double.parseDouble(parts[1]);
                    if (revenue > maxRevenue) {
                        maxRevenue = revenue;
                        topCountry = country;
                    }
                } catch (NumberFormatException e) {
                    // Skip invalid numbers
                }
            }
        }

        if (!topCountry.isEmpty()) {
            context.write(key, new Text(topCountry + "\t" + maxRevenue));
        }
    }
}
