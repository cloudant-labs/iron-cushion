package co.adhoclabs.ironcushion;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import co.adhoclabs.ironcushion.bulkinsert.BulkInsertConnectionStatistics;
import co.adhoclabs.ironcushion.bulkinsert.BulkInsertDocumentGenerator;
import co.adhoclabs.ironcushion.bulkinsert.BulkInsertPipelineFactory;
import co.adhoclabs.ironcushion.crud.CrudConnectionStatistics;
import co.adhoclabs.ironcushion.crud.CrudOperations;
import co.adhoclabs.ironcushion.crud.CrudPipelineFactory;

/**
 * The networking engine that asynchronously executes HTTP requests.
 *
 * @author Michael Parker (michael.g.parker@gmail.com)
 */


public class HttpReactor {
	private final int numConnections;
	private final InetSocketAddress databaseAddress;
	private final String authString;
	private final String host;
	private final boolean https;
	
	private final int timeoutDelay;
	
	public HttpReactor(ParsedArguments parsedArguments, InetSocketAddress databaseAddress, String authString, boolean https) {
		this.numConnections = parsedArguments.numConnections;
		this.databaseAddress = databaseAddress;
		this.host = databaseAddress.getHostName();
		this.authString = authString;
		this.https = https;
		this.timeoutDelay = parsedArguments.timeoutDelay;
	}

	private void run(AbstractBenchmarkPipelineFactory channelPipelineFactory)
			throws BenchmarkException {
		try {
			// Create the connections to the server.
			ClientBootstrap clientBootstrap = new ClientBootstrap(
					new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));
			clientBootstrap.setPipelineFactory(channelPipelineFactory);
		    
			//Timeout control
			clientBootstrap.setOption("connectTimeoutMillis", timeoutDelay);
			ChannelFuture future = null;
			
		    for (int i = 0; i < numConnections; ++i) {
		    	future = clientBootstrap.connect(databaseAddress);
		    }
		    
		 // Wait until the connection attempt succeeds or fails.
	        future.awaitUninterruptibly();
	        if (!future.isSuccess()) {
	            future.getCause().printStackTrace();
	            clientBootstrap.releaseExternalResources();
	            return;
	        }

			// Wait for all connections to complete their tasks.
			channelPipelineFactory.getCountDownLatch().await();

			// Shut down executor threads to exit.
			clientBootstrap.releaseExternalResources();
		} catch (InterruptedException e) {
			throw new BenchmarkException(e);
		}
	}

	public List<BulkInsertConnectionStatistics> performBulkInserts(
			List<BulkInsertDocumentGenerator> allBulkInsertDocumentGenerators,
			String bulkInsertPath) throws BenchmarkException {
		// Run the bulk inserts.
		BulkInsertPipelineFactory bulkInsertPipelineFactory = new BulkInsertPipelineFactory(
				numConnections, allBulkInsertDocumentGenerators, bulkInsertPath, authString, host, https);
		run(bulkInsertPipelineFactory);

		// Return the times for each connection.
		return bulkInsertPipelineFactory.getAllConnectionStatistics();
	}

	public List<CrudConnectionStatistics> performCrudOperations(List<CrudOperations> allCrudOperations,
			String crudPath) throws BenchmarkException {
		// Run the CRUD operations.
		CrudPipelineFactory crudPipelineFactory = new CrudPipelineFactory(
				numConnections, allCrudOperations, crudPath, authString, host, https);
		run(crudPipelineFactory);

		// Return the times for each connection.
		return crudPipelineFactory.getAllConnectionStatistics();
	}
}
