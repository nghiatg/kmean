package kmean;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {
	static ArrayList<Point> center;
	static int duplicate;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		center = new ArrayList<Point>();
		center.add(new Point());
		center.add(new Point());
		center.add(new Point());
//		System.out.println(center.get(0) + "\t" + center.get(1) + "\t" + center.get(2) + "\t");
		while (true) {
			boolean flag = true;
			duplicate = 0;
			String inPath = args[0];
			String outPath = args[1];
//			String inPath = "E:\\study\\Eclipse\\kmean\\output4.txt";
//			String outPath = "E:\\study\\Eclipse\\kmean\\output";
			File f = new File(outPath);
			FileSystem fs = FileSystem.get(new Configuration());
			if(fs.exists(new Path(outPath))) {
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(outPath + "/part-r-00000"))));
				String line = br.readLine();
				while (line != null) {
					Point p = new Point(line.substring(1, line.length() - 1));
					if (isContain(p)) {
						duplicate++;
						if (duplicate == 3) {
							flag = false;
							break;
						}
					} else {
						duplicate = 0;
					}
					center.remove(0);
					center.add(p);
					line = br.readLine();
				}
				br.close();
				if(flag == true) {
					fs.delete(new Path(outPath),true);
				}
			}
			 
//			if (f.isDirectory()) {
////				BufferedReader br = new BufferedReader(new FileReader(outPath + "/part-r-00000"));
//				String line = br.readLine();
//				while (line != null) {
//					Point p = new Point(line.substring(1, line.length() - 1));
//					if (isContain(p)) {
//						duplicate++;
//						if (duplicate == 3) {
//							flag = false;
//							break;
//						}
//					} else {
//						duplicate = 0;
//					}
//					center.remove(0);
//					center.add(p);
//					line = br.readLine();
//				}
//				br.close();
//				if(flag == true) {
//					FileUtils.deleteDirectory(f);
//				}
//			}
			if(flag == false) {
				break;
			}
			Configuration c = new Configuration();
			c.set("center0", center.get(0).toString());
			c.set("center1", center.get(1).toString());
			c.set("center2", center.get(2).toString());
			c.set("duplicate", String.valueOf(duplicate));
			Path in = new Path(inPath);
			Path out = new Path(outPath);

			Job job = Job.getInstance(c, "job");
			job.setJarByClass(MainClass.class);
			job.setMapperClass(MapForPoint.class);
			job.setReducerClass(ReduceForPoint.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, in);
			TextOutputFormat.setOutputPath(job, out);
			job.waitForCompletion(true);
			System.out.println("done 1 loop\n\n\n");
		}
	}

	public static class MapForPoint extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			Point p = getPointFromLine(value.toString());
			Point center0 = new Point(con.getConfiguration().get("center0"));
			Point center1 = new Point(con.getConfiguration().get("center1"));
			Point center2 = new Point(con.getConfiguration().get("center2"));
			double distanceToFirstCenter = p.getDistance(center0);
			double distanceToSecondCenter = p.getDistance(center1);
			double distanceToThirdCenter = p.getDistance(center2);
			double smallestDistance = Math.min(Math.min(distanceToFirstCenter, distanceToSecondCenter),
					distanceToThirdCenter);
			if (smallestDistance == distanceToFirstCenter) {
				con.write(new Text(center0.toString()), new Text(p.toString()));
			} else if (smallestDistance == distanceToSecondCenter) {
				con.write(new Text(center1.toString()), new Text(p.toString()));
			} else {
				con.write(new Text(center2.toString()), new Text(p.toString()));
			}
		}
	}

	public static class ReduceForPoint extends Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
			int dup = Integer.parseInt(con.getConfiguration().get("duplicate"));
			int count = 0;
			int sum_x = 0;
			int sum_y = 0;
			for (Text t : values) {
				Point p = new Point(t.toString());
				count++;
				sum_x += p.getX();
				sum_y += p.getY();
			}
			Point newPoint = new Point(sum_x / count, sum_y / count);
			System.out.println(newPoint.toString());
			con.write(new Text("(" + sum_x / count + ";" + sum_y / count + ")"), NullWritable.get());
		}
	}

	public static Point getPointFromLine(String line) {
		int index = 0;
		while (line.charAt(index) != ')')
			index++;
		String[] pos = line.substring(1, index).split(",");
		return new Point(Integer.parseInt(pos[0]), Integer.parseInt(pos[1]));
	}

	public static boolean isContain(Point p) {
		for (Point pt : center) {
			if (pt.getX() == p.getX() && pt.getY() == p.getY()) {
				return true;
			}
		}
		return false;
	}
}
