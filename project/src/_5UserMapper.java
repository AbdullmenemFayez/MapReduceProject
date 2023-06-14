import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class _5UserMapper extends Mapper<Object, Text, Text, Text> {

	private final Text Key = new Text(), val = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String fields[] = value.toString().split(",");
		if (fields.length == 3) {
			if (Integer.parseInt(fields[2].trim()) > 25) {
				Key.set(fields[0].trim());
				val.set("U," + fields[0].trim());
				context.write(Key, val);
			}
		} else {
			context.write(new Text(fields[0].trim()), new Text(fields[0].trim()
					+ "," + fields[1] + "," + fields[2] + "," + fields[3] + ","
					+ fields[4]));
		}

	}

}
