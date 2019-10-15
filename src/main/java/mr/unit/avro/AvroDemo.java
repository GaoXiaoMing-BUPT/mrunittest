package mr.unit.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
/*
*   通过生成代码的方法序列化
* */
public class AvroDemo {
    public static void main(String[] args) throws Exception {
        avroSerializeGenerateCode();
        avroDeSerializeGenerateCode();

        avroSerializeNoneGenerateCode();
        avroDeSerializeNoneGenerateCode();
    }
    private static void avroSerializeGenerateCode() throws IOException {
        File file = new File("src\\main\\avro\\users1.avro");
        if(file.exists() == false)
            file.createNewFile();
        //schema文件已经被编写到User类中
        User user1 = new User("gxm",589,"red");//直接调用生成代码的对象
        User user2 = User.newBuilder()
                .setName("xyz")
                .setFavoriteNumber(456)
                .setFavoriteColor("blue")
                .build();
        DatumWriter<User> datumWriter = new SpecificDatumWriter();
        //DatumWriter<User> datumWriter = new GenericDatumWriter<>();
        //DatumWriter<User> datumWriter = new ReflectDatumWriter<>();
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(datumWriter);
        dataFileWriter.create(user1.getSchema(),file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }
    private static void avroDeSerializeGenerateCode() throws IOException {
        File file = new File("src\\main\\avro\\users1.avro");
        if(file.exists() == false)
        {
            throw new FileNotFoundException();
        }
        //DatumReader<User> datumReader = new SpecificDatumReader<>();
        //DatumReader<User> datumReader = new GenericDatumReader<>();
        DatumReader<User> datumReader = new ReflectDatumReader<>();
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file,datumReader);
        Iterator<User> iterator = dataFileReader.iterator();
        while(iterator.hasNext()){
            User user = iterator.next();
            System.out.println(user);
        }
    }
    private static void avroSerializeNoneGenerateCode() throws IOException{
        File acscFile = new File("src\\main\\avro\\user.avsc");//此处读入avsc的json文件
        File file = new File("src\\main\\avro\\user2.avro");
        if (file.exists() == false){
            file.createNewFile();
        }
        Schema schema = new Schema.Parser().parse(acscFile);

        GenericRecord user1 = new GenericData.Record(schema);//使用record对象类型
        user1.put("name","gxm");
        user1.put("favorite_number",896);
        user1.put("favorite_color","white");

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name","xyz");
        user2.put("favorite_number",987);
        user2.put("favorite_color","black");

        //DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter();
        //DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
        DatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter(datumWriter);
        dataFileWriter.create(user1.getSchema(),file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();


    }
    private static void avroDeSerializeNoneGenerateCode() throws IOException {
        File file = new File("src\\main\\avro\\user2.avro");
        if(file.exists() == false){
            throw new FileNotFoundException();
        }
        //DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>();
        //DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DatumReader<GenericRecord> datumReader = new ReflectDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,datumReader);
        Iterator<GenericRecord> iterator = dataFileReader.iterator();
        while(iterator.hasNext()){
            GenericRecord genericRecord = iterator.next();
            System.out.println(genericRecord);
        }

    }
}
