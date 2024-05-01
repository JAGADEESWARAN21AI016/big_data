import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    public static class MapperClass extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens[0].equals("A")) {
                for (int k = 0; k < Integer.parseInt(context.getConfiguration().get("n")); k++) {
                    context.write(new Text(tokens[1] + "," + k), new Text("A," + tokens[2] + "," + tokens[3]));
                }
            } else {
                for (int i = 0; i < Integer.parseInt(context.getConfiguration().get("m")); i++) {
                    context.write(new Text(i + "," + tokens[1]), new Text("B," + tokens[2] + "," + tokens[3]));
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] a = new int[Integer.parseInt(context.getConfiguration().get("m"))];
            int[] b = new int[Integer.parseInt(context.getConfiguration().get("m"))];
            int result = 0;
            for (Text val : values) {
                String[] value = val.toString().split(",");
                if (value[0].equals("A")) {
                    a[Integer.parseInt(value[1])] = Integer.parseInt(value[2]);
                } else {
                    b[Integer.parseInt(value[1])] = Integer.parseInt(value[2]);
                }
            }
            for (int i = 0; i < Integer.parseInt(context.getConfiguration().get("m")); i++) {
                result += a[i] * b[i];
            }
            context.write(key, new Text(Integer.toString(result)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("m", args[2]); // Number of rows in matrix A
        conf.set("n", args[3]); // Number of columns in matrix B

        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
