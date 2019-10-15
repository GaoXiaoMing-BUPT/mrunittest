/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/13
 * Time: 23:15
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class TempTest {
    @Test
    public void testTemp() throws IOException {
        File schemaFile = new File("src\\main\\avro\\Temperature.avsc");
        File file = new File("target\\test_dataout\\temperature.axro");
        Schema schema = new Schema.Parser().parse(schemaFile);//加载avro的schema文件

        GenericRecord temp1 = new GenericData.Record(schema);
        temp1.put("date","gxm");
        temp1.put("maxTemp",(float)25.1);
        temp1.put("minTemp",(float)21.2);
        temp1.put("averageTemp",(float)24.3);
        


        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(temp1.getSchema(),file);
        dataFileWriter.append(temp1);

        dataFileWriter.close();

        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,datumReader);

        Iterator<GenericRecord> iterator = dataFileReader.iterator();

        GenericRecord temp2;
        temp2 = iterator.next();
        if (temp2 instanceof User){
            System.out.println("属于Temp类型");
        }else
            System.out.println("不属于Temp类型");
        if(temp2.equals(temp1))
            System.out.println("不生成代码序列化测试成功");
    }
}
