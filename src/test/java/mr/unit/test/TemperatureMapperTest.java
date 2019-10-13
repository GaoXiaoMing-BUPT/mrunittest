/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/11
 * Time: 19:07
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.test;

import mr.unit.test.com.unit.TemperatureMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class TemperatureMapperTest {
    @Test
    public void testMapper(){
        Text value = new Text("2019/10/11 25 75");
        new MapDriver<LongWritable, Text, Text, Text>()
                .withMapper(new TemperatureMapper())
                .withInput(new LongWritable(0),value)
                .withOutput(new Text("2019/10/11"),new Text("50"));
    }
}
