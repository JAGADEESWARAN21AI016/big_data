package org.weather;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherMR {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable>{

        private Text yearMonth = new Text();
        private FloatWritable temperature = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(","); // Assuming CSV format

            // Assuming year-month is in the first column and temperature is in the second column
            yearMonth.set(data[0]);
            temperature.set(Float.parseFloat(data[1]));

            context.write(yearMonth, temperature);
        }
    }

    public static class FloatAverageReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {

        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float average = sum / count;
            result.set(average);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather mr");
        job.setJarByClass(WeatherMR.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(FloatAverageReducer.class);
        job.setReducerClass(FloatAverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
