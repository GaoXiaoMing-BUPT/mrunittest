/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/13
 * Time: 22:45
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.mapredavro;

import hdfsutil.HdfsUtil;
import mr.unit.avro.AvroGenericMaxTemperature;
import org.apache.avro.Schema;


import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

public class AvroTemperatureRunner extends Configured implements Tool {
    private static boolean isLocal = false;
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
    private static final Log logger = LogFactory.getLog(AvroTemperatureRunner.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AvroTemperatureRunner(), args);
        if (res == 0) {
            if (isLocal)
                avroDeSerializeNoneGenerateCode("output\\part-r-00000.avro");
        }
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        if (isLocal) {
            conf.set("mapreduce.framework.name", "local");
            conf.set("fs.defaultFS", "file:///");//本地调试
            //删除本地文件
            if (delFile(new File("output")))
                logger.info("delete local output success");
            else {
                logger.error("delete local output fail");
                //System.exit(1);
            }
        }

        conf.setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST,true);
        /* 导入外部依赖项 */
        addTmpJar("E:/IDEAProject/mrunittest/out/artifacts/mrunittest_jar/avro-1.8.2.jar", conf);
        addTmpJar("E:/IDEAProject/mrunittest/out/artifacts/mrunittest_jar/avro-mapred-1.8.2-hadoop2.jar", conf);
        addTmpJar("E:/IDEAProject/mrunittest/out/artifacts/mrunittest_jar/avro-ipc-1.8.2.jar", conf);
        Job job = Job.getInstance(conf);
        job.setJar("out\\artifacts\\mrunittest_jar\\mrunittest.jar");
        job.setJarByClass(AvroTemperatureRunner.class);

        job.setJobName("learning avro");

        job.setMapperClass(AvroGenericMaxTemperature.AvroMapper.class);
        job.setReducerClass(AvroGenericMaxTemperature.AvroReducer.class);

        job.setUser("gxm");
        /* 区别 */
        /* 设置 任务的Avro map key-value输出格式 设置reducer的key输入Schema  即avsc文件的解析 */
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job,SCHEMA);
        AvroJob.setOutputKeySchema(job,SCHEMA);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setOutputValueClass(AvroGenericMaxTemperature.AvroReducer.class);

        if (isLocal) {
            FileInputFormat.setInputPaths(job, new Path("E:\\IDEAProject\\mrunittest\\temperature.txt"));
            FileOutputFormat.setOutputPath(job, new Path("E:\\IDEAProject\\mrunittest\\output"));
        } else
        {
            HdfsUtil hdfsUtil = new HdfsUtil();
            String filename = "\\avroOutput";
            Path path = new Path(filename);
            if (hdfsUtil.rmdir(filename)) {
                logger.info(filename + " delete success");
            }

            FileInputFormat.setInputPaths(job, new Path("\\avroInput"));
            FileOutputFormat.setOutputPath(job, path);
        }

        int exitCode = job.waitForCompletion(true)?0:1;

        return exitCode;
    }
    public static void addTmpJar(String jarPath, Configuration conf) throws IOException {
        System.setProperty("path.separator", ":");
        FileSystem fs = FileSystem.getLocal(conf);
        String newJarPath = fs.makeQualified(new Path(jarPath)).toString();
        String tmpjars = conf.get("tmpjars");
        if (tmpjars == null || tmpjars.length() == 0) {
            conf.set("tmpjars", newJarPath);
        } else {
            conf.set("tmpjars", tmpjars + "," + newJarPath);
        }
    }

    /*
     *   Temperature 序列化数据读取
     * */
    private static void avroDeSerializeNoneGenerateCode(String filename) throws IOException {
        File file = new File(filename);
        if (!file.exists()) {
            throw new FileNotFoundException();
        }
        //DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>();
        //DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DatumReader<GenericRecord> datumReader = new ReflectDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        Iterator<GenericRecord> iterator = dataFileReader.iterator();
        while (iterator.hasNext()) {
            GenericRecord genericRecord = iterator.next();
            System.out.println(genericRecord);
        }
    }

    private static boolean delFile(File file) {
        if (!file.exists()) {
            logger.error(file + "not exist");
            return false;
        }

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                delFile(f);
            }
        }
        return file.delete();
    }
}
