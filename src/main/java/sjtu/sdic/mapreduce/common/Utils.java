package sjtu.sdic.mapreduce.common;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Cachhe on 2019/4/19.
 */
public class Utils {
    public static boolean debugEnabled = false;

    public static void debug(String msg) {
        if (debugEnabled)
            System.out.println(msg);
    }

    /**
     * reduceName constructs the name of the intermediate file which map task
     * <mapTask> produces for reduce task <reduceTask>.
     *
     * @param jobName
     * @param mapTask map task id
     * @param reduceTask reduce task id
     * @return
     */
    public static String reduceName(String jobName, int mapTask, int reduceTask) {
        return "mrtmp." + jobName + "-" + mapTask+ "-" + reduceTask;
    }

    /**
     * mergeName constructs the name of the output file of reduce task <reduceTask>
     *
     * @param jobName
     * @param reduceTask reduce task id
     * @return
     */
    public static String mergeName(String jobName, int reduceTask) {
        return "mrtmp." + jobName + "-res-" + reduceTask;
    }
}



//        try
//        {
//            String encoding = "UTF-8";
//            File file = new File(fileName);
//            if (file.isFile() && file.exists())
//            { // 判断文件是否存在
//                InputStreamReader read = new InputStreamReader(
//                        new FileInputStream(file), encoding);// 考虑到编码格式
//                BufferedReader bufferedReader = new BufferedReader(read);
//                StringBuffer buffer = new StringBuffer();
//                String lineTxt = null;
//
//                while ((lineTxt = bufferedReader.readLine()) != null)
//                {
//                    buffer.append(lineTxt);
//                    buffer.append("\n");
//                }
//                bufferedReader.close();
//                read.close();
//                return buffer.toString();
//            }
//            else
//            {
//                System.out.println("找不到指定的文件");
//                throw new IOException("找不到指定的文件");
//            }
//        }
//        catch (Exception e)
//        {
//            System.out.println("读取文件内容出错");
//            e.printStackTrace();
//        }
//        return "";