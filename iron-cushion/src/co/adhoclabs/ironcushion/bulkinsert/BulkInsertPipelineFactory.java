package co.adhoclabs.ironcushion.bulkinsert;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpClientCodec;

import co.adhoclabs.ironcushion.AbstractBenchmarkPipelineFactory;
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
	
	public BulkInsertPipelineFactory(int numConnections,
			List<BulkInsertDocumentGenerator> allBulkInsertDocumentGenerators, String bulkInsertPath, String authString, String host) {
		super(numConnections);
		
		this.allConnectionStatistics = new ArrayList<BulkInsertConnectionStatistics>();
		for (int i = 0; i < numConnections; ++i) {
			allConnectionStatistics.add(new BulkInsertConnectionStatistics());
		}
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
		
		ChannelPipeline pipeline = Channels.pipeline(new HttpClientCodec(),
				// new HttpContentDecompressor(),
				new BulkInsertHandler(connectionStatistics, documentGenerator, bulkInsertPath, countDownLatch, authString, host)
				);
        return pipeline;
	}
}
