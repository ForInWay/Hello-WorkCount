package com.morgan.workcount.component;

/**
 * @Description
 * @Author Morgan
 * @Date 2020/12/8 16:16
 **/

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * spout
 * spout,继承一个基类，实现接口，主要负责从数据源获取数据
 * 我们这里作为一个简化，不从外部的数据源获取数据，只能内部不断发射一些句子
 */
public class RandomSentenceSpout extends BaseRichSpout {

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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
