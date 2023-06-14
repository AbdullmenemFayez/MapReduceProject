import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class _1Main {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job firstJob = Job.getInstance(conf, "Rating-Movies");
		firstJob.setJarByClass(_1Main.class);
		// job.setCombinerClass(Reduser.class);
		MultipleInputs.addInputPath(firstJob, new Path(args[0]),
				TextInputFormat.class, _2MoviesMapper.class);

		MultipleInputs.addInputPath(firstJob, new Path(args[1]),
				TextInputFormat.class, _3RatingsMapper.class);

		firstJob.setReducerClass(_4Reduser_Movies_Ratings.class);
		// job.setNumReduceTasks(0);
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(firstJob, new Path(args[4]));
		boolean first_Job = firstJob.waitForCompletion(true);

		/***************************************************************/

		Job secondJob = Job.getInstance(conf, "MR-Users");
		secondJob.setJarByClass(_1Main.class);
		MultipleInputs.addInputPath(secondJob, new Path(args[2]),
				TextInputFormat.class, _5UserMapper.class);

		MultipleInputs.addInputPath(secondJob, new Path(args[3]),
				TextInputFormat.class, _5UserMapper.class);

		secondJob.setReducerClass(_6Reduser_MR_User.class);
		// secondJob.setNumReduceTasks(0);

		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(secondJob, new Path(args[5]));

		boolean second_Job = secondJob.waitForCompletion(true);
		/***************************************************************/
		Job thirdJob = Job.getInstance(conf, "Average");
		thirdJob.setJarByClass(_1Main.class);

		MultipleInputs.addInputPath(thirdJob, new Path(args[6]),
				TextInputFormat.class, _7AverageMapper.class);

		thirdJob.setReducerClass(_8AverageReducer.class);

		thirdJob.setOutputKeyClass(Text.class);
		thirdJob.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(thirdJob, new Path(args[7]));

		boolean third_Job = thirdJob.waitForCompletion(true);
		System.exit((second_Job && first_Job && third_Job) ? 0 : 1);

	}
}