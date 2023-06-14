import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class _8AverageReducer extends Reducer<Text, Text, Text, Text> {
	private String titel = new String();
	private Text result = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double Avg = 0.0;
		int count = 0;
		for (Text val : values) {
			String[] str = val.toString().split(",");
			Avg += Integer.parseInt(str[1].trim());
			titel = str[0];
			++count;
		}
		result.set(key.toString() + "," + titel + "," + (Avg / (count * 1.0)));
		context.write(result, null);
	}
}
