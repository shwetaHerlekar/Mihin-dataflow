package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.opencsv.CSVParser;
import java.io.IOException;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
public class Mihin
{
  private static final byte[] FAMILY = Bytes.toBytes("patient-entry");
  private static final byte[] name = Bytes.toBytes("name");
    private static final byte[] city = Bytes.toBytes("city");
    private static final byte[] state = Bytes.toBytes("state");
    private static final byte[] postal_code = Bytes.toBytes("postal_code");
    private static final byte[] birth_date = Bytes.toBytes("birth_date");
    private static final byte[] gender = Bytes.toBytes("gender");
    private static final byte[] patient_id = Bytes.toBytes("patient_id");
   private static final byte[] all_json = Bytes.toBytes("all_json");

   private static long row_id = 1;

static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
  private static final long serialVersionUID = 1L;

  @Override
  public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {

  	String line = c.element();
		 	CSVParser csvParser = new CSVParser();
 		String[] parts = csvParser.parseLine(line);

      			// Output each word encountered into the output PCollection.
       			
         			// c.output(part);
       			
   				Put put_object = new Put(Bytes.toBytes(row_id));
				row_id = row_id +1;	
     			    	byte[] data = Bytes.toBytes( parts[0]);
			if(data!="null")
   				put_object.addColumn(FAMILY, name,data);
			if(parts[1]!="null")
 			put_object.addColumn(FAMILY, city, Bytes.toBytes(parts[1]));
			if(parts[2]!="null")
			put_object.addColumn(FAMILY, state, Bytes.toBytes(parts[2]));
			if(parts[3]!="null")
			put_object.addColumn(FAMILY, postal_code, Bytes.toBytes(parts[3]));
			if(parts[4]!="null")
			put_object.addColumn(FAMILY, birth_date, Bytes.toBytes(parts[4]));
			if(parts[5]!="null")
			put_object.addColumn(FAMILY, gender, Bytes.toBytes(parts[5]));
			if(parts[6]!="null")
			put_object.addColumn(FAMILY, patient_id, Bytes.toBytes(parts[6]));
			if(parts[7]!="null")
			put_object.addColumn(FAMILY, all_json, Bytes.toBytes(parts[7]));
			c.output(put_object);
  }
};
		
	

	public static void main(String[] args) 
	{
		// config object for writing to bigtable

		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder().withProjectId("healthcare-12").withInstanceId("hc-dataset").withTableId("mihin-data").build();

		// Start by defining the options for the pipeline.
		
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		
		// The 'gs' URI means that this is a Google Cloud Storage path
		options.setStagingLocation("gs://mihin-data/staging1");

		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);
			CloudBigtableIO.initializeForWrite(p);
p.apply(TextIO.Read.named("Reading from File").from("gs://mihin-data/Patient_entry.csv")).apply(ParDo.named("Processing Mihin data").of(MUTATION_TRANSFORM)).apply(CloudBigtableIO.writeToTable(config));
	
		p.run();

		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	}

}