package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class UpdateDateInHBase {
	
	private static void asyncBatchUpdate(Configuration conf)
		throws IOException {
		// Connection to the cluster.
		Connection connection = null;
		// a async batch handler
		BufferedMutator bufferedMutator = null;
		
		// establish the connection to the cluster.
		try {
			connection = ConnectionFactory.createConnection(conf);
			bufferedMutator = connection.getBufferedMutator(TableName.valueOf(TableInformation.TABLE_NAME));
			// describe the data
			Put put = new Put(Bytes.toBytes("row002"));
			put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1),
				Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_1),
				Bytes.toBytes(59));
			put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1),
				Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_2),
				Bytes.toBytes(60));
			
			// add data to buffer
			bufferedMutator.mutate(put);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			// close
			if (bufferedMutator != null)
				bufferedMutator.close();
			if (connection != null)
				connection.close();
		}
	}
	
	public static void main(String[] args)
		throws Exception {
		UpdateDateInHBase.asyncBatchUpdate(TableInformation.getHBaseConfiguration());
	}
	
}
