package com.morgan.workcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 单词计算拓扑
 * @Description Strom拓扑入门程序
 * @Author Morgan
 * @Date 2020/11/6 11:13
 **/
public class WordCountTopology {

    /**
     * spout
     * spout,继承一个基类，实现接口，主要负责从数据源获取数据
     * 我们这里作为一个简化，不从外部的数据源获取数据，只能内部不断发射一些句子
     */
    public static class RandomSentenceSpout extends BaseRichSpout{

        private static final long serialVersionUID = 3699352201538354417L;

        private SpoutOutputCollector collector;
        private Random random;

        private static final Logger logger = LoggerFactory.getLogger(RandomSentenceSpout.class);

        /**
         * 对spout进行初始化
         * @param conf
         * @param context
         * @param collector
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            // 在open方法初始化的时候，会传入进来一个东西，叫做SpoutOutputCollector
            // 这个SpoutOutputCollector是用来发送数据的
            this.collector = collector;
            this.random = new Random();
        }

        /**
         * 这个Spout类，之前说过，最终会运行在task中，某个worker进程的某个executor线程内部的某个task中
         * 那个task会负责无限循环调用nextTuple()方法
         * 这样就可以不断发射最新的数据出去，形成一个数据流
         */
        @Override
        public void nextTuple() {
            Utils.sleep(100);
            String[] sentences = new String[]{
                    "Blowing in the wind", "How many roads must a man walk down", "Before they call him a man", "How many seas must a white dove sail",
                    "Before she sleeps in the sand", "How many times must the cannon balls fly", "Before they're forever banned",
                    "The answer, my friend, is blowing in the wind", "The answer is blowing in the wind"
            };
            String sentence = sentences[random.nextInt(sentences.length)];
            logger.info("发射句子：" + sentence);
            // 这个Values,可以认为就是构建一个tuple
            // tuple是最小的数据单位，无限个tuple组成的流就是一个Stream
            collector.emit(new Values(sentence));
        }

        /**
         * declareOutputFields很重要
         * 是定义一个你发射出去的每个tuple中的每个Field的名称是什么
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    /**
     * bolt,每个bolt代码，同样是发送到某个worker的某个executor的某个task中执行
     */
    public static class SplitSentence extends BaseRichBolt {

        private static final long serialVersionUID = 6604009953652729483L;

        private OutputCollector collector;

        /**
         * 初始化方法，OutputCollector为这个bolt的发射器
         * @param conf
         * @param context
         * @param collector
         */
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 每接收到一条数据，就又executor方法去执行
         * @param tuple
         */
        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] words = sentence.split(" ");
            for (String word:words){
                collector.emit(new Values(word));
            }
        }

        /**
         * 定义每个发射出去的tuple，每个Field的名称
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /**
     * 单词计算blot
     */
    public static class WordCount extends BaseRichBolt {

        private static final long serialVersionUID = 7208077706057284643L;

        private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

        private OutputCollector collector;
        private Map<String,Long> wordCounts = new HashMap<String,Long>();

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");

            Long count = wordCounts.get(word);
            if (count == null){
                count = 0L;
            }
            count++;
            wordCounts.put(word,count);
            logger.info("单词[" + word + "],出现次数" + count);
            collector.emit(new Values(word,count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
        }
    }

    public static void main(String[] args) {
        // 在main方法中，会去将Spout和bolts组合起来，构建一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 第一个参数设置一个名字，第二个创建一个相应的对象，第三个参数设置相应的executor个数
        builder.setSpout("RandomSentence",new RandomSentenceSpout(),2);
        builder.setBolt("SplitSentence",new SplitSentence(),5).setNumTasks(10).shuffleGrouping("RandomSentence");
        builder.setBolt("WordCount",new WordCount(),10).setNumTasks(20).fieldsGrouping("SplitSentence",new Fields("word"));

        Config config = new Config();
        if (args !=null && args.length > 0){
            // 说明是在Strom集群上执行
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else{
            // 说明在本地运行
            config.setMaxTaskParallelism(20);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("WordCountTopology",config,builder.createTopology());

            Utils.sleep(60000);
            localCluster.shutdown();
        }
    }
}
