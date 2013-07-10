package co.adhoclabs.ironcushion.crud;

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
 * The {@link ChannelPipelineFactory} for connections that perform CRUD operations.
 * 
 * @author Michael Parker (michael.g.parker@gmail.com)
 */
public class CrudPipelineFactory extends AbstractBenchmarkPipelineFactory {
	private final List<CrudConnectionStatistics> allConnectionStatistics;
	private final List<CrudOperations> allCrudOperations;
	private final String crudPath;

	private int connectionNum;
	private final String authString;
	private final String host;
	private final boolean https;

	public CrudPipelineFactory(int numConnections,
			List<CrudOperations> allCrudOperations, String crudPath, String authString, String host, boolean https) {
		super(numConnections);

		this.allConnectionStatistics = new ArrayList<CrudConnectionStatistics>(numConnections);
		for (int i = 0; i < numConnections; ++i) {
			this.allConnectionStatistics.add(new CrudConnectionStatistics());
		}
		this.allCrudOperations = allCrudOperations;
		this.crudPath = crudPath;
		this.authString = authString;
		this.host = host;
		this.https = https;
		connectionNum = 0;
	}

	/**
	 * @return the {@link CrudConnectionStatistics} used by connections
	 */
	public List<CrudConnectionStatistics> getAllConnectionStatistics() {
		return allConnectionStatistics;
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		CrudConnectionStatistics connectionStatistics = allConnectionStatistics.get(connectionNum);
		CrudOperations crudOperations = allCrudOperations.get(connectionNum);
		connectionNum++;

		if (https) {
			SSLEngine engine =
					SecureChatSslContextFactory.getClientContext().createSSLEngine();
			engine.setUseClientMode(true);

			ChannelPipeline pipeline =  Channels.pipeline(
					new SslHandler(engine),
					new HttpClientCodec(),
					// new HttpContentDecompressor(),
					new CrudHandler(connectionStatistics, crudOperations, crudPath, countDownLatch, authString, host, true)
					);

			return pipeline;
		} else {
			ChannelPipeline pipeline =  Channels.pipeline(
					new HttpClientCodec(),
					// new HttpContentDecompressor(),
					new CrudHandler(connectionStatistics, crudOperations, crudPath, countDownLatch, authString, host, false)
					);

			return pipeline;
		}
	}
}
