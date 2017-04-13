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
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.Combine.*;
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
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;


public class Mihin{

	public static String file="";

	static class Patient{
		public String name,patient_id,city,state,postal_code,email,gender,bdate,all_json;

		public Patient()
		{
			name="";
			patient_id="";
			city="";
			state="";
			postal_code="";
			email="";
			gender="";
			bdate="";
			all_json="";
		}
	}

    public static void main(String[] args) {
		
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
		PCollection<String> lines=p.apply(TextIO.Read.named("Reading from File").from("gs://mihin-data/Patient_entry.txt"));
		lines.apply(
			 ParDo
      			.named("Collect Data")            // the transform name
      			.of(new DoFn<String, String>() {       // a DoFn as an anonymous inner class instance
        			@Override
        			public void processElement(ProcessContext c) {
          				String line = c.element();
				file+=line;
				c.output(line);
        			}
      		}));
		p.apply(Create.of(file)).setCoder(StringUtf8Coder.of()).apply(TextIO.Write.to("gs://mihin-data/patients.txt"));
		
		//.apply(ParDo.named("Processing Synpuf data").of(MUTATION_TRANSFORM))
		//.apply(CloudBigtableIO.writeToTable(config));
	
		p.run();






         /*JSONParser parser = new JSONParser();

        try {

            Object obj = parser.parse(new FileReader("src/main/resources/Patient_entry.txt"));

            JSONObject jsonObject = (JSONObject) obj;
          

	Patient[] patients = new Patient[10];
            JSONArray resource = (JSONArray) jsonObject.get("resources");
        
            for (int i = 0; i < 10; i++) {
            	
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
				if(j==(nameArray.size()-1))
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
			System.out.println(patients[i].name);
			System.out.println(patients[i].city);
			System.out.println(patients[i].state);
			System.out.println(patients[i].postal_code);
			System.out.println(patients[i].bdate);
			System.out.println(patients[i].gender);
			System.out.println(patients[i].patient_id);
			System.out.println(patients[i].all_json);
		}


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }*/
    }

}