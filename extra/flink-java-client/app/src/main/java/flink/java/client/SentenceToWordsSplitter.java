package flink.java.client;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class SentenceToWordsSplitter implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String sentence, Collector<String> out) throws Exception {
        // 수신한 문자열을 공백을 기준으로 단어로 분할
        for (String word : sentence.split(" ")) {
            out.collect(word); // 각 단어를 개별 스트림 요소로 수집
        }
    }
}