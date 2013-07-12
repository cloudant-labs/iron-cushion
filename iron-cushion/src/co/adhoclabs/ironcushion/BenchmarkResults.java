package co.adhoclabs.ironcushion;

import java.util.Arrays;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import co.adhoclabs.ironcushion.bulkinsert.BulkInsertConnectionStatistics;
import co.adhoclabs.ironcushion.crud.CrudConnectionStatistics;
import co.adhoclabs.ironcushion.crud.CrudOperations;

/**
 * The results of the benchmark.
 * 
 * @author Michael Parker (michael.g.parker@gmail.com)
 */
public abstract class BenchmarkResults {
	public final long timeTaken;
	public final long totalJsonBytesSent;
	public final long totalJsonBytesReceived;

	private BenchmarkResults(long timeTaken, long totalJsonBytesSent, long totalJsonBytesReceived) {
		this.timeTaken = timeTaken;
		this.totalJsonBytesSent = totalJsonBytesSent;
		this.totalJsonBytesReceived = totalJsonBytesReceived;
	}

	private static final double MILLIS_PER_SEC = 1000.0;

	protected static Formatter getFormatter(StringBuilder sb) {
		return new Formatter(sb, Locale.getDefault());
	}

	protected static String format(long value) {
		// Inefficient, but this isn't done while the benchmark is running.
		return new Formatter().format("%,d", value).toString();
	}

	protected static String format(double value) {
		// Inefficient, but this isn't done while the benchmark is running.
		return new Formatter().format("%,.3f", value).toString();
	}

	/**
	 * Benchmark results for bulk insertions.
	 */
	public static final class BulkInsertBenchmarkResults extends BenchmarkResults {
		public final SampleStatistics localProcessingStatistics;
		public final SampleStatistics sendDataStatistics;
		public final SampleStatistics remoteProcessingStatistics;
		public final SampleStatistics receiveDataStatistics;

		public final double remoteProcessingRate;
		public final double localInsertRate;
		public final int timeouts;

		private BulkInsertBenchmarkResults(long timeTaken,
				long totalJsonBytesSent,
				long totalJsonBytesReceived,
				SampleStatistics localProcessingStatistics,
				SampleStatistics sendDataStatistics,
				SampleStatistics remoteProcessingStatistics,
				SampleStatistics receiveDataStatistics,
				double remoteProcessingRate,
				double localInsertRate,
				int timeouts) {
			super(timeTaken, totalJsonBytesSent, totalJsonBytesReceived);

			this.localProcessingStatistics = localProcessingStatistics;
			this.sendDataStatistics = sendDataStatistics;
			this.remoteProcessingStatistics = remoteProcessingStatistics;
			this.receiveDataStatistics = receiveDataStatistics;
			this.remoteProcessingRate = remoteProcessingRate;
			this.localInsertRate = localInsertRate;
			this.timeouts = timeouts;
		}

		@Override
		public String toString() {
			return toString("");
		}

		public String toString(String indent) {
			StringBuilder sb = new StringBuilder();
			sb.append(indent).append("timeTaken=").append(format(timeTaken / MILLIS_PER_SEC)).append(" secs\n");
			sb.append(indent).append("connectionTimeouts=").append(format(timeouts)).append("\n");
			sb.append(indent).append("totalJsonBytesSent=").append(format(totalJsonBytesSent)).append(" bytes\n");
			sb.append(indent).append("totalJsonBytesReceived=").append(format(totalJsonBytesReceived)).append(" bytes\n");
			sb.append(indent).append("localProcessing={").append(localProcessingStatistics).append("}\n");
			sb.append(indent).append("sendData={").append(sendDataStatistics).append("}\n");
			sb.append(indent).append("remoteProcessing={").append(remoteProcessingStatistics).append("}\n");
			sb.append(indent).append("receiveData={").append(receiveDataStatistics).append("}\n");
			sb.append(indent).append("remoteProcessingRate=").append(format(remoteProcessingRate)).append(" docs/sec\n");
			sb.append(indent).append("localInsertRate=").append(format(localInsertRate)).append(" docs/sec");
			return sb.toString();
		}
	}

	/**
	 * Benchmark results for CRUD operations.
	 */
	public static final class CrudBenchmarkResults extends BenchmarkResults {
		public final SampleStatistics localProcessingStatistics;
		public final SampleStatistics sendDataStatistics;
		public final SampleStatistics remoteCreateProcessingStatistics;
		public final SampleStatistics remoteReadProcessingStatistics;
		public final SampleStatistics remoteUpdateProcessingStatistics;
		public final SampleStatistics remoteDeleteProcessingStatistics;

		public final double remoteCreateProcessingRate;
		public final double remoteReadProcessingRate;
		public final double remoteUpdateProcessingRate;
		public final double remoteDeleteProcessingRate;
		public final int timeouts;

		public CrudBenchmarkResults(long timeTaken,
				long totalJsonBytesSent,
				long totalJsonBytesReceived,
				SampleStatistics localProcessingStatistics,
				SampleStatistics sendDataStatistics,
				SampleStatistics remoteCreateProcessingStatistics,
				SampleStatistics remoteReadProcessingStatistics,
				SampleStatistics remoteUpdateProcessingStatistics,
				SampleStatistics remoteDeleteProcessingStatistics,
				double remoteCreateProcessingRate,
				double remoteReadProcessingRate,
				double remoteUpdateProcessingRate,
				double remoteDeleteProcessingRate,
				int timeouts) {
			super(timeTaken, totalJsonBytesSent, totalJsonBytesReceived);

			this.localProcessingStatistics = localProcessingStatistics;
			this.sendDataStatistics = sendDataStatistics;
			this.remoteCreateProcessingStatistics = remoteCreateProcessingStatistics;
			this.remoteReadProcessingStatistics = remoteReadProcessingStatistics;
			this.remoteUpdateProcessingStatistics = remoteUpdateProcessingStatistics;
			this.remoteDeleteProcessingStatistics = remoteDeleteProcessingStatistics;
			this.remoteCreateProcessingRate = remoteCreateProcessingRate;
			this.remoteReadProcessingRate = remoteReadProcessingRate;
			this.remoteUpdateProcessingRate = remoteUpdateProcessingRate;
			this.remoteDeleteProcessingRate = remoteDeleteProcessingRate;
			this.timeouts = timeouts;
		}

		@Override
		public String toString() {
			return toString("");
		}

		public String toString(String indent) {
			StringBuilder sb = new StringBuilder();
			sb.append(indent).append("timeTaken=").append(format(timeTaken / MILLIS_PER_SEC)).append(" secs\n");
			sb.append(indent).append("connectionTimeouts=").append(format(timeouts)).append("\n");
			sb.append(indent).append("totalJsonBytesSent=").append(format(totalJsonBytesSent)).append(" bytes\n");
			sb.append(indent).append("totalJsonBytesReceived=").append(format(totalJsonBytesReceived)).append(" bytes\n");
			sb.append(indent).append("localProcessing={").append(localProcessingStatistics).append("}\n");
			sb.append(indent).append("sendData={").append(sendDataStatistics).append("}\n");
			sb.append(indent).append("remoteCreateProcessing={").append(remoteCreateProcessingStatistics).append("}\n");
			sb.append(indent).append("remoteReadProcessing={").append(remoteReadProcessingStatistics).append("}\n");
			sb.append(indent).append("remoteUpdateProcessing={").append(remoteUpdateProcessingStatistics).append("}\n");
			sb.append(indent).append("remoteDeleteProcessing={").append(remoteDeleteProcessingStatistics).append("}\n");
			sb.append(indent).append("remoteCreateProcessingRate=").append(format(remoteCreateProcessingRate)).append(" docs/sec\n");
			sb.append(indent).append("remoteReadProcessingRate=").append(format(remoteReadProcessingRate)).append(" docs/sec\n");
			sb.append(indent).append("remoteUpdateProcessingRate=").append(format(remoteUpdateProcessingRate)).append(" docs/sec\n");
			sb.append(indent).append("remoteDeleteProcessingRate=").append(format(remoteDeleteProcessingRate)).append(" docs/sec");
			return sb.toString();
		}
	}

	private static long getTimeTaken(
			List<? extends AbstractConnectionStatistics> allConnectionStatistics) {
		// The time taken is the maximum time taken by any connection.
		long maxTimeTaken = 0;
		for (AbstractConnectionStatistics connectionStatistics : allConnectionStatistics) {
			long timeTaken = connectionStatistics.getTotalTimeMillis();
			if (timeTaken > maxTimeTaken) {
				maxTimeTaken = timeTaken;
			}
		}
		return maxTimeTaken;
	}

	private static long getTotalJsonBytesSent(
			List<? extends AbstractConnectionStatistics> allConnectionStatistics) {
		long totalJsonBytesSent = 0;
		for (AbstractConnectionStatistics connectionStatistics : allConnectionStatistics) {
			totalJsonBytesSent += connectionStatistics.getJsonBytesSent();
		}
		return totalJsonBytesSent;
	}

	private static long getTotalJsonBytesReceived(
			List<? extends AbstractConnectionStatistics> allConnectionStatistics) {
		long totalJsonBytesReceived = 0;
		for (AbstractConnectionStatistics connectionStatistics : allConnectionStatistics) {
			totalJsonBytesReceived += connectionStatistics.getJsonBytesReceived();
		}
		return totalJsonBytesReceived;
	}

	private static SampleStatistics getLocalProcessingStatistics(
			List<? extends AbstractConnectionStatistics> allConnectionStatistics) {
		long[] values = new long[allConnectionStatistics.size()];
		for (int i = 0; i < allConnectionStatistics.size(); ++i) {
			AbstractConnectionStatistics connectionStatistics = allConnectionStatistics.get(i);
			values[i] = connectionStatistics.getLocalProcessingTimeMillis();
		}
		return SampleStatistics.statisticsForPopulation(values);
	}

	private static SampleStatistics getSendDataStatistics(
			List<? extends AbstractConnectionStatistics> allConnectionStatistics) {
		long[] values = new long[allConnectionStatistics.size()];
		for (int i = 0; i < allConnectionStatistics.size(); ++i) {
			AbstractConnectionStatistics connectionStatistics = allConnectionStatistics.get(i);
			values[i] = connectionStatistics.getSendDataTimeMillis();
		}
		return SampleStatistics.statisticsForPopulation(values);
	}

	/**
	 * Returns benchmark results for the connection statistics for bulk inserts.
	 * 
	 * @param allConnectionStatistics the bulk insert connection statistics
	 * @return the benchmark results
	 */
	public static BulkInsertBenchmarkResults getBulkInsertResults(
			ParsedArguments parsedArguments,
			List<BulkInsertConnectionStatistics> allConnectionStatistics) {
		long timeTaken = getTimeTaken(allConnectionStatistics);
		long totalJsonBytesSent = getTotalJsonBytesSent(allConnectionStatistics);
		long totalJsonBytesReceived = getTotalJsonBytesReceived(allConnectionStatistics);

		long[] values = new long[allConnectionStatistics.size()];
		// Get statistics for local processing.
		SampleStatistics localProcessingStatistics = getLocalProcessingStatistics(allConnectionStatistics);
		// Get statistics for sending data.
		SampleStatistics sendDataStatistics = getSendDataStatistics(allConnectionStatistics);
		// Get statistics for remote processing.
		for (int i = 0; i < allConnectionStatistics.size(); ++i) {
			BulkInsertConnectionStatistics connectionStatistics = allConnectionStatistics.get(i);
			values[i] = connectionStatistics.getRemoteProcessingTimeMillis();
		}
		SampleStatistics remoteProcessingStatistics = SampleStatistics.statisticsForPopulation(values);
		// Get statistics for receiving data.
		for (int i = 0; i < allConnectionStatistics.size(); ++i) {
			BulkInsertConnectionStatistics connectionStatistics = allConnectionStatistics.get(i);
			values[i] = connectionStatistics.getReceivedDataTimeMillis();
		}
		SampleStatistics receiveDataStatistics = SampleStatistics.statisticsForPopulation(values);

		// Calculate the rate of documents inserted per second.
		long numBulkInsertedDocs = (parsedArguments.numDocumentsPerBulkInsert *
				parsedArguments.numBulkInsertOperations);
		double remoteProcessingRate = 0;
		double localInsertRate = 0;

		//Calculate timeouts
		int timeouts = 0;
		for (BulkInsertConnectionStatistics connectionStatistics : allConnectionStatistics) {
			if (connectionStatistics.getTotalTimeMillis()==0)
				timeouts++;
		}		

		for (BulkInsertConnectionStatistics connectionStatistics : allConnectionStatistics) {
			if (connectionStatistics.getRemoteProcessingTimeMillis() != 0)
				remoteProcessingRate += (MILLIS_PER_SEC * numBulkInsertedDocs /
						connectionStatistics.getRemoteProcessingTimeMillis());
			long nonLocalProcessingTime = connectionStatistics.getSendDataTimeMillis() +
					connectionStatistics.getRemoteProcessingTimeMillis() +
					connectionStatistics.getReceivedDataTimeMillis();
			if (nonLocalProcessingTime != 0)
				localInsertRate += (MILLIS_PER_SEC * numBulkInsertedDocs / nonLocalProcessingTime);
		}

		return new BulkInsertBenchmarkResults(timeTaken,
				totalJsonBytesSent,
				totalJsonBytesReceived,
				localProcessingStatistics,
				sendDataStatistics,
				remoteProcessingStatistics,
				receiveDataStatistics,
				remoteProcessingRate,
				localInsertRate,
				timeouts);
	}

	/**
	 * Returns benchmark results for the connection statistics for CRUD operations.
	 * 
	 * @param allConnectionStatistics the CRUD connection statistics
	 * @return the benchmark results
	 */
	public static CrudBenchmarkResults getCrudResults(
			int numConnections, CrudOperations.CrudOperationCounts operationCounts,
			List<CrudConnectionStatistics> allConnectionStatistics) {
		long timeTaken = getTimeTaken(allConnectionStatistics);
		long totalJsonBytesSent = getTotalJsonBytesSent(allConnectionStatistics);
		long totalJsonBytesReceived = getTotalJsonBytesReceived(allConnectionStatistics);

		long[] values = new long[allConnectionStatistics.size()];
		// Get statistics for local processing.
		SampleStatistics localProcessingStatistics = getLocalProcessingStatistics(allConnectionStatistics);
		// Get statistics for sending data.
		SampleStatistics sendDataStatistics = getSendDataStatistics(allConnectionStatistics);
		// Get statistics for remote processing of create operations.
		for (int i = 0; i < allConnectionStatistics.size(); ++i) {
			CrudConnectionStatistics connectionStatistics = allConnectionStatistics.get(i);
			values[i] = connectionStatistics.getRemoteCreateProcessingTimeMillis();
		}
		SampleStatistics remoteCreateProcessingStatistics = SampleStatistics.statisticsForPopulation(values);
		// Get statistics for remote processing of read operations.
		for (int i = 0; i < allConnectionStatistics.size(); ++i) {
			CrudConnectionStatistics connectionStatistics = allConnectionStatistics.get(i);
			values[i] = connectionStatistics.getRemoteReadProcessingTimeMillis();
		}
		SampleStatistics remoteReadProcessingStatistics = SampleStatistics.statisticsForPopulation(values);
		// Get statistics for remote processing of update operations.
		for (int i = 0; i < allConnectionStatistics.size(); ++i) {
			CrudConnectionStatistics connectionStatistics = allConnectionStatistics.get(i);
			values[i] = connectionStatistics.getRemoteUpdateProcessingTimeMillis();
		}
		SampleStatistics remoteUpdateProcessingStatistics = SampleStatistics.statisticsForPopulation(values);
		// Get statistics for remote processing of delete operations.
		for (int i = 0; i < allConnectionStatistics.size(); ++i) {
			CrudConnectionStatistics connectionStatistics = allConnectionStatistics.get(i);
			values[i] = connectionStatistics.getRemoteDeleteProcessingTimeMillis();
		}
		SampleStatistics remoteDeleteProcessingStatistics = SampleStatistics.statisticsForPopulation(values);

		// Calculate timeouts
		int timeouts = 0;
		for (CrudConnectionStatistics connectionStatistics : allConnectionStatistics) {
			if (connectionStatistics.getTotalTimeMillis()==0)
				timeouts++;
		}

		// Calculate the rate of documents created per second.
		double createRate = 0;
		for (CrudConnectionStatistics connectionStatistics : allConnectionStatistics) {
			if (connectionStatistics.getRemoteCreateProcessingTimeMillis()!=0)
				createRate += (operationCounts.numCreateOperations /
						(connectionStatistics.getRemoteCreateProcessingTimeMillis() / MILLIS_PER_SEC));
		}
		// Calculate the rate of documents read per second.
		double readRate = 0;
		for (CrudConnectionStatistics connectionStatistics : allConnectionStatistics) {
			if (connectionStatistics.getRemoteReadProcessingTimeMillis()!=0)
				readRate += (operationCounts.numReadOperations /
						(connectionStatistics.getRemoteReadProcessingTimeMillis() / MILLIS_PER_SEC));
		}
		// Calculate the rate of documents updated per second.
		double updateRate = 0;
		for (CrudConnectionStatistics connectionStatistics : allConnectionStatistics) {
			if (connectionStatistics.getRemoteUpdateProcessingTimeMillis()!=0)
				updateRate += (operationCounts.numUpdateOperations /
						(connectionStatistics.getRemoteUpdateProcessingTimeMillis() / MILLIS_PER_SEC));
		}
		// Calculate the rate of documents deleted per second.
		double deleteRate = 0;
		for (CrudConnectionStatistics connectionStatistics : allConnectionStatistics) {
			if (connectionStatistics.getRemoteDeleteProcessingTimeMillis()!=0)
				deleteRate += (operationCounts.numDeleteOperations /
						(connectionStatistics.getRemoteDeleteProcessingTimeMillis() / MILLIS_PER_SEC));
		}

		return new CrudBenchmarkResults(timeTaken,
				totalJsonBytesSent,
				totalJsonBytesReceived,
				localProcessingStatistics,
				sendDataStatistics,
				remoteCreateProcessingStatistics,
				remoteReadProcessingStatistics,
				remoteUpdateProcessingStatistics,
				remoteDeleteProcessingStatistics,
				createRate,
				readRate,
				updateRate,
				deleteRate,
				timeouts);
	}

	/**
	 * Essential statistics about a data set.
	 */
	public static final class SampleStatistics {
		public final double min;
		public final double max;
		public final long sum;
		public final double mean;
		public final double median;
		public final double deviation;

		public SampleStatistics(double min, double max, long sum,
				double mean, double median, double deviation) {
			this.min = min;
			this.max = max;
			this.sum = sum;
			this.mean = mean;
			this.median = median;
			this.deviation = deviation;
		}

		private static SampleStatistics statisticsForPopulation(long[] values) {
			// Make a copy of the array before sorting as a courtesy.
			values = Arrays.copyOf(values, values.length);
			Arrays.sort(values);

			// Find the minimum and maximum.
			long min = values[0];
			long max = values[values.length - 1];
			// Find the median.
			double median;
			if ((values.length % 2) == 1) {
				median = values[(values.length - 1) / 2];
			} else {
				int firstMedianIndex = values.length / 2;
				int secondMedianIndex = firstMedianIndex - 1;
				median = ((double) (values[firstMedianIndex] + values[secondMedianIndex])) / 2; 
			}
			// Compute the mean.
			long sum = 0;
			for (int i = 0; i < values.length; ++i) {
				sum += values[i];
			}
			double mean = ((double) sum) / values.length;
			// Compute the standard deviation.
			double numerator = 0;
			for (int i = 0; i < values.length; ++i) {
				double difference = values[i] - mean;
				numerator += (difference * difference);
			}
			double variance = numerator / values.length;
			double deviation = Math.sqrt(variance);

			return new SampleStatistics(min, max, sum, mean, median, deviation);
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("min=").append(format(min / MILLIS_PER_SEC)).append(" secs, ");
			sb.append("max=").append(format(max / MILLIS_PER_SEC)).append(" secs, ");
			sb.append("median=").append(format(median / MILLIS_PER_SEC)).append(" secs, ");
			sb.append("sd=").append(format(deviation / MILLIS_PER_SEC)).append(" secs");
			return sb.toString();
		}

		public String toString(String title) {
			StringBuilder sb = new StringBuilder();
			sb.append(title).append(": ").append(toString());
			return sb.toString();
		}
	}
}
