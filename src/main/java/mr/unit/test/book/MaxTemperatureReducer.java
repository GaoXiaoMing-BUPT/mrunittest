/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/11
 * Time: 19:03
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.test.book;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int maxValue = Integer.MIN_VALUE;
        for(IntWritable val : values)
            maxValue = Math.max(maxValue, val.get());

        context.write(key, new IntWritable(maxValue));
    }
}