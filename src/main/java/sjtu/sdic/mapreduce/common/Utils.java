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