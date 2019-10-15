/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/13
 * Time: 22:45
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.avro;

import hdfsutil.HdfsUtil;
import org.apache.avro.Schema;


import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
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

import java.io.IOException;

public class AvroTemperatureRunner extends Configured implements Tool {
    private static Schema SCHEMA = new Schema.Parser().parse("{\"namespace\": \"mr.unit.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Temperature\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"date\", \"type\": \"string\"},\n" +
            "     {\"name\": \"maxTemp\",  \"type\": \"float\"},\n" +
            "     {\"name\": \"minTemp\", \"type\": \"float\"},\n" +
            "     {\"name\": \"averageTemp\",\"type\":\"float\" }\n" +
            " ]\n" +
            "}");
    private static final Log logger = LogFactory.getLog(AvroTemperatureRunner.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AvroTemperatureRunner(), args);
        HdfsUtil hdfsUtil = new HdfsUtil();
        if (res == 0)
            hdfsUtil.printFileContent("\\avroOutput\\part-r-00000.avro",10);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
/*        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS","file:///");//本地调试*/
        conf.setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST,true);
        /* 导入外部依赖项 */
        addTmpJar("G:/IDEAProject/mrunittest/out/artifacts/mrunittest_jar/avro-1.8.2.jar",conf);
        addTmpJar("G:/IDEAProject/mrunittest/out/artifacts/mrunittest_jar/avro-mapred-1.8.2-hadoop2.jar",conf);
        addTmpJar( "G:/IDEAProject/mrunittest/out/artifacts/mrunittest_jar/avro-ipc-1.8.2.jar" , conf);
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

/*        FileInputFormat.setInputPaths(job,new Path("G:\\IDEAProject\\mrunittest\\temperature.txt"));
        FileOutputFormat.setOutputPath(job,new Path("G:\\IDEAProject\\mrunittest\\ouput"));*/
        HdfsUtil hdfsUtil = new HdfsUtil();
        String filename = "\\avroOutput";
        Path path = new Path(filename);
        if (hdfsUtil.rmdir(filename))
        {
            logger.info(filename+" delete success");
        }

        FileInputFormat.setInputPaths(job,new Path("\\avroInput"));
        FileOutputFormat.setOutputPath(job,path);

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
}
