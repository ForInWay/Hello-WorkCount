package com.morgan.workcount.component;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * bolt,每个bolt代码，同样是发送到某个worker的某个executor的某个task中执行
 */
public class SplitSentence extends BaseRichBolt {

    private static final long serialVersionUID = 6604009953652729483L;

    private OutputCollector collector;

    /**
     * 初始化方法，OutputCollector为这个bolt的发射器
     * @param conf
     * @param context
     * @param collector
     */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 每接收到一条数据，就又executor方法去执行
     * @param tuple
     */
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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
