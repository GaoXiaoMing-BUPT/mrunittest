/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/13
 * Time: 22:01
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;

public class AvroGenericMaxTemperature {
    private static Schema SCHEMA = new Schema.Parser().parse("{\"namespace\": \"mr.unit.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Temperature\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"date\", \"type\": \"string\"},\n" +
            "     {\"name\": \"maxTemp\",  \"type\": \"float\", \"order\":\"descending\"},\n" +
            "     {\"name\": \"minTemp\", \"type\": \"float\"},\n" +
            "     {\"name\": \"averageTemp\",\"type\":\"float\" }\n" +
            " ]\n" +
            "}");
    public static class AvroMapper extends Mapper<LongWritable,Text,AvroKey<String>, AvroValue<GenericRecord>> {
        private GenericRecord genericRecord = new GenericData.Record(SCHEMA);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();//new String(value.getBytes(),0,value.getLength(),"UTF-16");
            String[] fields = line.split("\\t");
            if (fields.length != 3)
                System.out.println(line);
            String date = fields[0];
            float maxTemp = Float.parseFloat(fields[1]);
            float minTemp = Float.parseFloat(fields[2]);
            genericRecord.put("date",date);
            genericRecord.put("maxTemp",maxTemp);
            genericRecord.put("minTemp",minTemp);
            genericRecord.put("averageTemp",(maxTemp+minTemp)/2);
            /* 进行排序时直接当做key,然后在schema文件中设定想要的顺序即可 */
            context.write(new AvroKey<>(genericRecord.toString()), new AvroValue<>(genericRecord));
        }
    }
    /* reduce人输出 AvroKey 及 Null */
    public static class AvroReducer extends Reducer<AvroKey<String>, AvroValue<GenericRecord>,AvroKey<GenericRecord>, NullWritable>{
        @Override
        protected void reduce(AvroKey<String> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
            GenericRecord max = null;
            for (AvroValue<GenericRecord> value : values) {
                GenericRecord genericRecord = value.datum();
                if (max == null || (float)genericRecord.get("maxTemp") > (float) max.get("maxTemp")){
                    max = GenericRecordCopy(genericRecord);
                }
            }
            context.write(new AvroKey<>(max),NullWritable.get());
        }
        private GenericRecord GenericRecordCopy(GenericRecord genericRecord){
            GenericRecord gen = new GenericData.Record(SCHEMA) ;
            gen.put("date",genericRecord.get("date"));
            gen.put("maxTemp",genericRecord.get("maxTemp"));
            gen.put("minTemp",genericRecord.get("minTemp"));
            gen.put("averageTemp",genericRecord.get("averageTemp"));
            return gen;
        }
    }
}
