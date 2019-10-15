/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/13
 * Time: 21:47
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package mr.unit.generatedata;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class GenerateData {
    public static void main(String[] args) throws IOException {
        File file = new File("temperature.txt");
        if(file.exists() == false)
            file.createNewFile();
        PrintWriter printWriter = new PrintWriter(file);
        String word = "abcdefghijklmnopqrstuvwxyz";

        for (int i = 0; i < 10000; i++) {
            String name = "";
            double maxT = -1;
            double minT = 0;
            for(int j = 0; j < 3 ; j++)
                name += word.charAt((int) (Math.random() * 3));
            while(maxT < minT){
                maxT = (int)(Math.random()*500)/10.0;
                minT =  (int)(Math.random()*300)/10.0;
            }
            printWriter.println(name+"\t"+maxT+"\t"+minT);
        }
    }
}
