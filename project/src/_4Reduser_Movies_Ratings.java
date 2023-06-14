import java.io.IOException;
import java.util.ArrayList;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class _4Reduser_Movies_Ratings extends Reducer<Text, Text, Text, Text> {

	private List<String> Movies = new ArrayList<>(),
			ratings = new ArrayList<>();
	
	private HashSet<String> hm = new HashSet<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {
			String[] fields = value.toString().split(",");
			if (fields[0].equals("M")) {
				Movies.add(value.toString());
			} else if (fields[0].equals("R")) {
				ratings.add(value.toString());
			}
		}
		for (String movie : Movies) {
			String[] s = movie.split(",");
			for (String rat : ratings) {
				String[] str = rat.split(",");
				if (str[2].equals(s[1])) {
					if (!hm.contains(str[1] + "-" + str[2])) {
						hm.add(str[1] + "-" + s[1]);
						context.write(null, new Text("MR," + str[1] + ","
								+ str[2] + "," + str[3] + "," + s[2]));
					}
				}
			}

		}
	}
}