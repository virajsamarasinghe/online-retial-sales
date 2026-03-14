package Task4_MarketLeaders;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * TopCountryMapper - Processes the output of CountryMonthly analysis.
 * Input format: Country_YYYY-MM \t Revenue
 * Output: Key = YYYY-MM, Value = Country:Revenue
 */
public class TopCountryMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private final Text monthKey = new Text();
    private final Text countryValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");
        
        if (parts.length >= 2) {
            String composite = parts[0]; // Country_YYYY-MM
            String revenue = parts[1];
            
            int lastUnderscore = composite.lastIndexOf('_');
            if (lastUnderscore != -1) {
                String country = composite.substring(0, lastUnderscore);
                String month = composite.substring(lastUnderscore + 1);
                
                monthKey.set(month);
                countryValue.set(country + ":" + revenue);
                context.write(monthKey, countryValue);
            }
        }
    }
}
