package co.adhoclabs.ironcushion.bulkinsert;

import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLEngine;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.ssl.SslHandler;

import co.adhoclabs.ironcushion.AbstractBenchmarkPipelineFactory;
import co.adhoclabs.ironcushion.securechat.SecureChatSslContextFactory;
/**
 * The {@link ChannelPipelineFactory} for connections that perform bulk inserts.
 * 
 * @author Michael Parker (michael.g.parker@gmail.com)
 */
public class BulkInsertPipelineFactory extends AbstractBenchmarkPipelineFactory {
	private final List<BulkInsertConnectionStatistics> allConnectionStatistics;
	private final List<BulkInsertDocumentGenerator> allBulkInsertDocumentGenerators;
	private final String bulkInsertPath;

	private int connectionNum;
	private final String authString;
	private final String host;
	private final boolean https;

	public BulkInsertPipelineFactory(int numConnections,
			List<BulkInsertDocumentGenerator> allBulkInsertDocumentGenerators, String bulkInsertPath, String authString, String host, boolean https) {
		super(numConnections);

		this.allConnectionStatistics = new ArrayList<BulkInsertConnectionStatistics>();
		for (int i = 0; i < numConnections; ++i) {
			allConnectionStatistics.add(new BulkInsertConnectionStatistics());
		}
		this.https = https;
		this.allBulkInsertDocumentGenerators = allBulkInsertDocumentGenerators;
		this.bulkInsertPath = bulkInsertPath;
		this.authString = authString;
		this.host = host;
		connectionNum = 0;
	}

	/**
	 * @return the {@link BulkInsertConnectionStatistics} used by connections
	 */
	public List<BulkInsertConnectionStatistics> getAllConnectionStatistics() {
		return allConnectionStatistics;
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		BulkInsertConnectionStatistics connectionStatistics = allConnectionStatistics.get(connectionNum);
		BulkInsertDocumentGenerator documentGenerator = allBulkInsertDocumentGenerators.get(connectionNum);
		connectionNum++;

		if (https) {
		SSLEngine engine =
				SecureChatSslContextFactory.getClientContext().createSSLEngine();
		engine.setUseClientMode(true);

		ChannelPipeline pipeline = Channels.pipeline(
				new SslHandler(engine),
				new HttpClientCodec(),
				// new HttpContentDecompressor(),
				new BulkInsertHandler(connectionStatistics, documentGenerator, bulkInsertPath, countDownLatch, authString, host, true)
				);
		return pipeline;
		} else {
			ChannelPipeline pipeline = Channels.pipeline(
					new HttpClientCodec(),
					// new HttpContentDecompressor(),
					new BulkInsertHandler(connectionStatistics, documentGenerator, bulkInsertPath, countDownLatch, authString, host, false)
					);
			return pipeline;
		}
	}
}
