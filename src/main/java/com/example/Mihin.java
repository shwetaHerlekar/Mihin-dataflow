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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.Preconditions;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import java.util.Collections;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.io.FileWriter;

class Patient{
        public String name,city,state, postal_code,bdate,gender,patient_id,all_json;
}

public class Mihin
{
	String file="";
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
	JSONParser parser = new JSONParser();
                  Object obj1 = parser.parse(line);
	JSONObject jsonObject = (JSONObject) obj1;
              	JSONArray resource = (JSONArray) jsonObject.get("resources");
	Patient[] patients = new Patient[resource.size()];
	for (int i = 0; i<resource.size(); i++) {
            		Patient p = new Patient();
                		JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(i).toString());
    		//System.out.println(jsonObject);
			
    		HashMap map = (HashMap) jsonObject1.get("resource");
		p.all_json=String.valueOf(map);
    		JSONArray FullnameArray  = (JSONArray) map.get("name");
    		 JSONObject nameObject  = (JSONObject) parser.parse(FullnameArray.get(0).toString());
    		JSONArray nameArray = (JSONArray)(nameObject.get("given"));
    		for(int j=0;j<nameArray.size();j++)
		{
			if(j==0)
				p.name=String.valueOf(nameArray.get(j))+" ";
			else if(j==(nameArray.size()-1))
				p.name+=nameArray.get(j);
			else
				p.name+=nameArray.get(j)+" ";	
		}
    		if ( map.get("address") != null) {
    			  JSONObject addressObject  = (JSONObject) parser.parse(((JSONArray) map.get("address")).get(0).toString());
        			p.city=String.valueOf(addressObject.get("city"));
        			p.state=String.valueOf(addressObject.get("state"));
       			p.postal_code=String.valueOf(addressObject.get("postalCode"));
		}
    		p.bdate=String.valueOf(map.get("birthDate"));
    		p.gender=String.valueOf(map.get("gender"));
    		p.patient_id=String.valueOf(map.get("id"));
		patients[i]=p;
	}
       	for(int i=0;i<patients.length;i++)
	{
		Put put_object = new Put(Bytes.toBytes(row_id));
		row_id = row_id +1;	
     		byte[] data = Bytes.toBytes(patients[i].name);
		if(!patients[i].name.equals("null"))
   		put_object.addColumn(FAMILY, name,data);
		if(!patients[i].city.equals("null"))
 		put_object.addColumn(FAMILY, city, Bytes.toBytes(patients[i].city));
		if(!patients[i].state.equals("null"))
		put_object.addColumn(FAMILY, state, Bytes.toBytes(patients[i].state));
		if(!patients[i].postal_code.equals("null"))
		put_object.addColumn(FAMILY, postal_code, Bytes.toBytes(patients[i].postal_code));
		if(!patients[i].bdate.equals("null"))
		put_object.addColumn(FAMILY, birth_date, Bytes.toBytes(patients[i].bdate));
		if(!patients[i].gender.equals("null"))
		put_object.addColumn(FAMILY, gender, Bytes.toBytes(patients[i].gender));
		if(!patients[i].patient_id.equals("null"))
		put_object.addColumn(FAMILY, patient_id, Bytes.toBytes(patients[i].patient_id));
		if(!patients[i].all_json.equals("null"))
		put_object.addColumn(FAMILY, all_json, Bytes.toBytes(patients[i].all_json));
		c.output(put_object);	
	}				

			
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
		try
		{		
		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
        		GoogleCredential credential = GoogleCredential.getApplicationDefault();
        		Storage storage = new Storage.Builder(httpTransport, jsonFactory, credential)
        		.setApplicationName("Google-ObjectsListExample/1.0").build();
         		String BUCKET_NAME = "mihin-data";
         		String objectFileName = "Patient_entry.txt";
         		Storage.Objects.Get obj = storage.objects().get(BUCKET_NAME, objectFileName);
         		HttpResponse response = obj.executeMedia();
		file=response.parseAsString();
		}
		catch(Exception e){}
		p.apply(Create.of(file)).setCoder(StringUtf8Coder.of()).apply(ParDo.named("Processing Mihin data").of(MUTATION_TRANSFORM)).apply(CloudBigtableIO.writeToTable(config));

		//p.apply(TextIO.Read.named("Reading from File").from("gs://mihin-data/Patient_entry.csv")).apply(ParDo.named("Processing Mihin data").of(MUTATION_TRANSFORM)).apply(CloudBigtableIO.writeToTable(config));
	
		p.run();

		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	}

}