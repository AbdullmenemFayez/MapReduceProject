import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class _6Reduser_MR_User extends Reducer<Text, Text, Text, Text> {

	private List<String> MR = new ArrayList<>(), Users = new ArrayList<>();

	private HashSet<String> hm = new HashSet<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {
			String[] fields = value.toString().split(",");
			if (fields[0].trim().equals("MR")) {
				MR.add(value.toString());
			} else if (fields[0].trim().equals("U")) {
				Users.add(value.toString());
			}
		}

		for (String user : Users) {
			String[] s = user.split(",");
			for (String mr : MR) {
				String[] str = mr.split(",");
				String ss = s[1].trim(), sss = str[1].trim();
				if (ss.equals(sss)) {

					if (!hm.contains(ss + "-" + str[2])) {
						hm.add(ss + "-" + str[2]);
						context.write(null, new Text(str[2] + ","
								+ str[4] + "," + str[3]));
					}
				}
			}
		}

	}
}
