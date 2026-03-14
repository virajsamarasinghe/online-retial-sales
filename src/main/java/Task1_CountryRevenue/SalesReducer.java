package Task1_CountryRevenue;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * SalesReducer - Stage 2
 * 
 * Aggregates all revenue amounts for each specific country.
 * This class also serves as the Combiner for localized aggregation on the Map node.
 */
public class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable totalRevenue = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;

        // Iterate through all revenue values for the given country (key)
        for (DoubleWritable val : values) {
            sum += val.get();
        }

        // Round to 2 decimal places for financial reporting accuracy
        double roundedSum = Math.round(sum * 100.0) / 100.0;
        
        totalRevenue.set(roundedSum);
        context.write(key, totalRevenue);
    }
}
