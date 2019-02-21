package com.boa.training.streaming;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

public class StreamingTest {

	 public static void main(String[] args) {
	        // TODO Auto-generated method stub
	         Properties props = new Properties();
	            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test app");
	            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

	            StreamsConfig streamsConfig = new StreamsConfig(props);

	            Serde<String> stringSerde = Serdes.String();

	            StreamsBuilder builder = new StreamsBuilder();

	            KStream<String, String> simpleFirstStream = builder.stream("src-topic");


	            KStream<String, String> upperCasedStream = simpleFirstStream.mapValues(new ValueMapper<String, String>() {

	                @Override
	                public String apply(String s) {
	                    // TODO Auto-generated method stub
	                    return s.toUpperCase();
	                }
	            });

	            upperCasedStream.to( "out-topic", Produced.with(stringSerde, stringSerde));
	            

	            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
	        
	            kafkaStreams.start();
	            try {
	                Thread.sleep(35000);
	            } catch (InterruptedException e) {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
	            }

	    }

}
