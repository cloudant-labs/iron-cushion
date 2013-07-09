package co.adhoclabs.ironcushion;

import java.util.concurrent.CountDownLatch;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;

/**
 * Abstract superclass for channel pipelines used in the benchmark.
 * 
 * @author Michael Parker (michael.g.parker@gmail.com)
 */
public abstract class AbstractBenchmarkPipelineFactory implements ChannelPipelineFactory {
	protected final CountDownLatch countDownLatch;

	protected AbstractBenchmarkPipelineFactory(int numConnections) {
		this.countDownLatch = new CountDownLatch(numConnections);
	}
	
	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}
}
