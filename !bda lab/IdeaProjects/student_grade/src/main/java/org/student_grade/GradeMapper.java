package org.student_grade;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GradeMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text studentName = new Text();
    private IntWritable score = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            studentName.set(itr.nextToken()); // Assuming student name is the first token
            if (itr.hasMoreTokens()) {
                score.set(Integer.parseInt(itr.nextToken())); // Assuming score is the second token
                context.write(studentName, score);
            }
        }
    }
}

