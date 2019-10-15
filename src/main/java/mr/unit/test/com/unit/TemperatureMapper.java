/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/11
 * Time: 16:51
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.test.com.unit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
    //测试数据输入 line:  <date todayTemperature yesterdayTemperature>
    //测试数据需要的输出 （yesterdayTemperature-todayTemperature）/ averageTemperature *100%
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = StringUtils.split(line, "\\s");
        String date = data[0];
        float todayTemperature = Float.parseFloat(data[1]);
        float yesterdayTemperature = Float.parseFloat(data[2]);
        float averageTemperature = (yesterdayTemperature + todayTemperature) / 2;
        String outputRate = (int) ((yesterdayTemperature - todayTemperature) / averageTemperature * 100) + "";
        context.write(new Text(date), new Text(outputRate));
    }

}