import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class _2MoviesMapper extends Mapper<Object, Text, Text, Text> {
	private final Text Key = new Text(), val = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String fields[] = value.toString().split(",");
		if (fields[2].toLowerCase().contains("children")
				&& fields[2].toLowerCase().contains("comedy")) {
			Key.set(fields[0].trim());
			val.set("M," + fields[0].trim() + "," + fields[1].trim() + ","
					+ fields[2].trim());
			context.write(Key, val);
		}

	}

}
