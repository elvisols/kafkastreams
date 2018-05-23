package com.example.kafkastreams;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
@Slf4j
public class KafkastreamsApplication {

	@Component
	public static class PageViewEventSource implements ApplicationRunner {
		private final MessageChannel pageViewsOut;
		public PageViewEventSource(AnalyticsBinding binding) {
			this.pageViewsOut = binding.pageViewsOut();
		}
		@Override
		public void run(ApplicationArguments args) throws Exception {
			List<String> names = Arrays.asList("mfisher", "dyser", "John");
			List<String> pages = Arrays.asList("pmfisher", "pdyser", "pJohn");
			Runnable runnable = () -> {
				String rPage = pages.get(new Random().nextInt(pages.size()));
				String rName = names.get(new Random().nextInt(names.size()));
				PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);
				Message<PageViewEvent> message = MessageBuilder
						.withPayload(pageViewEvent)
						.setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes()) //message type is String (from userId)
						.build();
				try {
					this.pageViewsOut.send(message);
					log.info("message sent!...");
				} catch(Exception e) {
					log.error(e.getMessage());
				}
			};
			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
		}
	}
	
	// A spring cloud stream processing sink 
	@Component
	public static class PageViewEventProcessor {
		@StreamListener
//		@SendTo (AnalyticsBinding.PAGE_COUNT_OUT) // this is an array, so you can send to more than one topic
//		public KStream<?, Long> process(@Input(AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, PageViewEvent> events) {
		public void process(@Input(AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, PageViewEvent> events) {
			/*
			 * A KStream is a continous flow/sequence of messages and may contain duplicate data.
			 * A KTable is always the latest value for the same key. And we can work more on it - e.g query.
			 * Note: A join can be done between KStream and KTable e.g events.leftJoin(kTable, new ValueJoiner...)
			 */
//			KTable<String, Long> kTable = events //=> without WindowedBy
//			KTable<Windowed<String>, Long> kTable = events
			events
				.filter((key, value) -> value.getDuration() > 10)
				.map((key, value) -> new KeyValue<>(value.getPage(), "0")) // repartition to get a new k,v. manage the page and count. i.e the new key=page and value=0. Is 0 coz we won't use it
				.groupByKey()
//				.windowedBy(TimeWindows.of(1000 * 60)) // for last 60 seconds
				.count(Materialized.as(AnalyticsBinding.PAGE_COUNT_MV)); // The Materialized View (Queriable State Store) returns a KTable. This returns the count for everything ever seen. Else you may want to use "WindowedBy" which returns a KTable<Windowed<String>, Long> chunks for a specified time
//				.count(); // remember count is a state!
//				.toStream(); // turn KTable to a stream of "pages, count" to send to another topic
		}
	}
	
//	@Component
//	public static class PageCountSink {
//		@StreamListener
//		public void process(@Input(AnalyticsBinding.PAGE_COUNT_IN) KTable<?, Long> counts) {
//			counts
//				.toStream()
//				.foreach((key, value) -> log.info(key + "=" + value));
//		}
//	}
	
	@RestController
	public static class CountRestController {
		private final QueryableStoreRegistry registry;
		
		public CountRestController(QueryableStoreRegistry registry) {
			this.registry = registry;
		}
		@GetMapping("/counts")
		@Synchronized
		Map<String, Long> counts() throws InterruptedException {
			Map<String, Long> counts = new HashMap<>();
			Thread.sleep(1L);
			ReadOnlyKeyValueStore<String, Long> queryableStoreType = this.registry.getQueryableStoreType(AnalyticsBinding.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore());
			KeyValueIterator<String, Long> all = queryableStoreType.all();
			while(all.hasNext()) {
				KeyValue<String, Long> value = all.next();
				counts.put(value.key, value.value);
			}
			return counts;
		}
	}
	
	public static void main(String[] args) {SpringApplication.run(KafkastreamsApplication.class, args);}
}

interface AnalyticsBinding {
	String PAGE_VIEWS_OUT = "pvout";
	String PAGE_VIEWS_IN = "pvin";
	String PAGE_COUNT_MV = "pcmv";
	String PAGE_COUNT_OUT = "pcout";
	String PAGE_COUNT_IN = "pcin";
	//process messages using kafka streams
	@Input(PAGE_VIEWS_IN)
	KStream<String, PageViewEvent> pageViewsIn(); // userId, Payload
	// Used to send streams to kafka
	@Output(PAGE_VIEWS_OUT)
	MessageChannel pageViewsOut();
//	@Output(PAGE_COUNT_OUT)
//	KStream<String, Long> pageCountOut();
//	@Input(PAGE_COUNT_IN)
//	KTable<String, Long> pageCountIn();
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent {
	private String userId, page;
	private long duration;
}
