package com.morgan.workcount;

import com.morgan.workcount.component.RandomSentenceSpout;
import com.morgan.workcount.component.SplitSentence;
import com.morgan.workcount.component.WordCount;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 单词计算拓扑
 * @Description Strom拓扑入门程序
 * @Author Morgan
 * @Date 2020/11/6 11:13
 **/
public class LocalClusterWordCountTopology {

    public static void main(String[] args) {
        // 在main方法中，会去将Spout和bolts组合起来，构建一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 第一个参数设置一个名字，第二个创建一个相应的对象，第三个参数设置相应的executor个数
        builder.setSpout("RandomSentence",new RandomSentenceSpout(),2);
        builder.setBolt("SplitSentence",new SplitSentence(),5).setNumTasks(10).shuffleGrouping("RandomSentence");
        builder.setBolt("WordCount",new WordCount(),10).setNumTasks(20).fieldsGrouping("SplitSentence",new Fields("word"));

        Config config = new Config();
        if(args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 说明是在eclipse里面本地运行
            config.setMaxTaskParallelism(20);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCountTopology", config, builder.createTopology());
            Utils.sleep(60000);
            cluster.shutdown();
        }
    }
}
