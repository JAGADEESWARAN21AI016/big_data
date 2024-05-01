package org.matrixmulti;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse input: matrix, row, column, value
            String[] tokens = value.toString().split(",");
            String matrix = tokens[0];
            int row = Integer.parseInt(tokens[1]);
            int col = Integer.parseInt(tokens[2]);
            int val = Integer.parseInt(tokens[3]);

            if (matrix.equals("A")) {
                for (int k = 0; k < context.getConfiguration().getInt("colsA", 1); k++) {
                    outputKey.set(row + "," + k);
                    outputValue.set("A," + col + "," + val);
                    context.write(outputKey, outputValue);
                }
            } else if (matrix.equals("B")) {
                for (int k = 0; k < context.getConfiguration().getInt("rowsB", 1); k++) {
                    outputKey.set(k + "," + col);
                    outputValue.set("B," + row + "," + val);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int[] a = new int[context.getConfiguration().getInt("colsA", 1)];
            int[] b = new int[context.getConfiguration().getInt("rowsB", 1)];

            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                if (tokens[0].equals("A")) {
                    a[Integer.parseInt(tokens[1])] = Integer.parseInt(tokens[2]);
                } else if (tokens[0].equals("B")) {
                    b[Integer.parseInt(tokens[1])] = Integer.parseInt(tokens[2]);
                }
            }

            int sum = 0;
            for (int i = 0; i < a.length; i++) {
                sum += a[i] * b[i];
            }
            outputValue.set(Integer.toString(sum));
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix multiply");

        job.setJarByClass(Main.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set number of reducers to 1 for simplicity
        job.setNumReduceTasks(1);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set dimensions of matrices A and B
        job.getConfiguration().setInt("rowsA", Integer.parseInt(args[2]));
        job.getConfiguration().setInt("colsA", Integer.parseInt(args[3]));
        job.getConfiguration().setInt("rowsB", Integer.parseInt(args[3]));
        job.getConfiguration().setInt("colsB", Integer.parseInt(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
