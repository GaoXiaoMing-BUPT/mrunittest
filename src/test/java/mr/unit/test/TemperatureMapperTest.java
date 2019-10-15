/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/11
 * Time: 19:07
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.test;

import mr.unit.avro.AvroGenericMaxTemperature;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TemperatureMapperTest {
    @Test
    public void testMapper() throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src\\main\\avro\\Temperature.avsc"));
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("date", "2019/10/11");
        genericRecord.put("maxTemp", (float) 75);
        genericRecord.put("minTemp", (float) 25);
        genericRecord.put("averageTemp", (float) 50);
        Text value = new Text("2019/10/11\t75\t25");
        new MapDriver<LongWritable, Text, AvroKey<String>, AvroValue<GenericRecord>>()
                .withMapper(new AvroGenericMaxTemperature.AvroMapper())
                .withInput(new LongWritable(0),value)
                .withOutput(new AvroKey<>("2019/10/11"),
                        new AvroValue<>(genericRecord)
                );
    }
}
