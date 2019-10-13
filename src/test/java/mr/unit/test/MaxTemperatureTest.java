/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/11
 * Time: 19:04
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.test;

import java.io.IOException;
import java.util.Arrays;

import mr.unit.test.book.MaxTemperatureMapper;
import mr.unit.test.book.MaxTemperatureReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

public class MaxTemperatureTest {
    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text("0057332130999991950010103004+51317+028783FM-12+017199999V0203201N00721004501CN0100001N9-01281-01391102681");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-128))
                .runTest();
    }
    @Test
    public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("1950"), new IntWritable(10))
                .runTest();
    }

}