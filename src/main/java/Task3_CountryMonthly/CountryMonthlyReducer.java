package Task3_CountryMonthly;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * CountryMonthlyReducer - Aggregates total revenue per country per month.
 */
public class CountryMonthlyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    
    private final DoubleWritable totalRevenue = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        
        // Round for final presentation
        double roundedSum = Math.round(sum * 100.0) / 100.0;
        totalRevenue.set(roundedSum);
        context.write(key, totalRevenue);
    }
}
