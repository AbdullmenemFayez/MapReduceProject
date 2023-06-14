import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class _7AverageMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String fields[] = value.toString().split(",");

		context.write(new Text(fields[0].trim()), new Text(fields[1].trim()
				+ "," + fields[2].trim()));

	}
}
