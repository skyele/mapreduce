package sjtu.sdic.mapreduce.core;

import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import sjtu.sdic.mapreduce.common.Channel;
import sjtu.sdic.mapreduce.common.DoTaskArgs;
import sjtu.sdic.mapreduce.common.JobPhase;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.rpc.Call;
import sjtu.sdic.mapreduce.rpc.MasterRpcService;
import sjtu.sdic.mapreduce.rpc.WorkerRpcService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sjtu.sdic.mapreduce.common.JobPhase.MAP_PHASE;

/**
 * Created by Cachhe on 2019/4/22.
 */
public class Scheduler {

    /**
     * schedule() starts and waits for all tasks in the given phase (mapPhase
     * or reducePhase). the mapFiles argument holds the names of the files that
     * are the inputs to the map phase, one per map task. nReduce is the
     * number of reduce tasks. the registerChan argument yields a stream
     * of registered workers; each item is the worker's RPC address,
     * suitable for passing to {@link Call}. registerChan will yield all
     * existing registered workers (if any) and new ones as they register.
     *
     * @param jobName job name
     * @param mapFiles files' name (if in same dir, it's also the files' path)
     * @param nReduce the number of reduce task that will be run ("R" in the paper)
     * @param phase MAP or REDUCE
     * @param registerChan register info channel
     */

    public static void schedule(String jobName, String[] mapFiles, int nReduce, JobPhase phase, Channel<String> registerChan) throws InterruptedException {
        int nTasks = -1; // number of map or reduce tasks
        int nOther = -1; // number of inputs (for reduce) or outputs (for map)
        LinkedList<String> availWorker = new LinkedList<>();
        switch (phase) {
            case MAP_PHASE:
                nTasks = mapFiles.length;
                nOther = nReduce;
                break;
            case REDUCE_PHASE:
                nTasks = nReduce;
                nOther = mapFiles.length;
                break;
        }

        System.out.println(String.format("Schedule: %d %s tasks (%d I/Os)", nTasks, phase, nOther));

        /**
        // All ntasks tasks have to be scheduled on workers. Once all tasks
        // have completed successfully, schedule() should return.
        //
        // Your code here (Part III, Part IV).
        //
        */
        CountDownLatch countDownLatch = new CountDownLatch(nTasks);
        for(int i = 0; i < nTasks; i++){
            DoTaskArgs doTaskArgs;
            if(phase == MAP_PHASE)
                doTaskArgs = new DoTaskArgs(jobName, mapFiles[i], phase, i, nOther);
            else
                doTaskArgs = new DoTaskArgs(jobName, null, phase, i, nOther);
            String channel = registerChan.read();
            Thread run = new Thread(() -> {
                String finalChannel = channel;
                WorkerRpcService workerRpcService;
                while (true){
                    workerRpcService = Call.getWorkerRpcService(finalChannel);
                    try {
                        workerRpcService.doTask(doTaskArgs);
                        break;
                    }catch (SofaRpcException e){
                        try {
                            registerChan.write(finalChannel);
                            finalChannel = registerChan.read();
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
                try {
                    countDownLatch.countDown();
                    registerChan.write(finalChannel);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            run.start();
        }
        countDownLatch.await();
        System.out.println(String.format("Schedule: %s done", phase));
    }
}
