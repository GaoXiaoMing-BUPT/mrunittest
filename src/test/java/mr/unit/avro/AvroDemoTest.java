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

import static org.junit.Assert.*;

public class AvroDemoTest {
    @Test
    public void avroTest1() throws IOException {
        File file = new File("target\\test_dataout\\user1.axro");
        if (file.exists() == false){
            file.createNewFile();
        }
        User user1 = new User("gxm",456,"red");
        User user2 = User.newBuilder()
                .setName("xyz")
                .setFavoriteNumber(456)
                .setFavoriteColor("black")
                .build();
        DatumWriter<User> datumWriter = new SpecificDatumWriter<>();
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(user1.getSchema(),file);
        dataFileWriter.append(user1);
        dataFileWriter.close();

        DatumReader<User> datumReader = new SpecificDatumReader<>();
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file,datumReader);
        Iterator<User> iterator = dataFileReader.iterator();
        User[] users = new User[2];
        int i = 0;
        while(iterator.hasNext()){
            users[i++] = iterator.next();
        }
        if(user1.equals(users[0]) || user2.equals(users[1]))
            System.out.println("生成代码的序列化测试成功");
    }
    @Test
    public void avroTest2() throws IOException{
        File schemaFile = new File("src\\main\\avro\\user.avsc");
        File file = new File("target\\test_dataout\\user2.axro");
        Schema schema = new Schema.Parser().parse(schemaFile);//加载avro的schema文件

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name","gxm");
        user1.put("favorite_number",456);
        user1.put("favorite_color","orange");

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name","xyz");
        user2.put("favorite_number",1243);
        user2.put("favorite_color","yellow");


        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(user1.getSchema(),file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();


        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,datumReader);

        Iterator<GenericRecord> iterator = dataFileReader.iterator();

        GenericRecord user3,user4;
        user3 = iterator.next();
        user4 = iterator.next();
        if (user3 instanceof User){
            System.out.println("属于User类型");
        }else
            System.out.println("不属于User类型");
        if(user3.equals(user1) && user4.equals(user2))
            System.out.println("不生成代码序列化测试成功");
    }
    @Test
    public void test() throws IOException {
        avroTest1();
        avroTest2();
    }
}