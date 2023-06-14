import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class _3RatingsMapper extends Mapper<Object, Text, Text, Text> {
	private final Text Key = new Text(), val = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String fields[] = value.toString().split(",");

		if (Integer.parseInt(fields[2].replaceAll(" ", "")) > 2) {

			Key.set(fields[1].trim());
			val.set("R," + fields[0].trim() + "," + fields[1].trim() + ","
					+ fields[2].trim());
			context.write(Key, val);

		}
	}
}