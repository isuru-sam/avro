package com.example.avrotest.controller;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64.Decoder;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.example.avrotest.avroschema.User;

import ch.qos.logback.core.encoder.Encoder;

@RestController
//@RequestMapping("books")
public class AvroController {
	
	@GetMapping("/test")
    public @ResponseBody String getBook() {
		/*User user3 = User.newBuilder()
	             .setName("Charlie")
	             .setFavoriteColor("blue")
	             .setFavoriteNumber(null)
	             .build();*/
		User u = new User();
		u.setFavoriteColor("blue");
		u.setName("ni");
		u.setFavoriteNumber(null);
		u.setAddress(null);
		//testit();
	String b=null;
	try {
		b = serealizeAvroHttpRequestJSON(u);
		//vlaidate();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	System.out.println(b);
        return "test";
    }
	
	public String serealizeAvroHttpRequestJSON(
			User user1) throws IOException {
		ByteArrayOutputStream b=new ByteArrayOutputStream();
			  
			 // Serialize user1, user2 and user3 to disk
			    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
			    DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
			    dataFileWriter.create(user1.getSchema(),b);
			    dataFileWriter.append(user1);
			   
			    dataFileWriter.close();
			    String finalString = new String(b.toByteArray());
			    return finalString;
}
	

public void testit() {
	
	Schema schema=null;
	try {
		schema = new Schema.Parser().parse(new File("src/main/avro/test.avsc"));
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
	

	GenericRecord user1 = new GenericData.Record(schema);
	user1.put("name", "Alyssa");
	user1.put("favorite_number", 256);
	// Leave favorite color null

	GenericRecord user2 = new GenericData.Record(schema);
	user2.put("name", "Ben");
	user2.put("favorite_number", 7);
	user2.put("favorite_color", "red");
	
}

public void vlaidate() throws Exception{
	Schema schema=null;
	try {
		schema = new Schema.Parser().parse(new File("src/main/avro/test.avsc"));
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	 String json = "{\"name\":\"Nish\",\"favorite_number\":7,\"favorite_color\":\"bl\"}";
     System.out.println(validateJson(json, schema));
    
}
public  boolean validateJson(String json, Schema schema) throws Exception {
    InputStream input = new ByteArrayInputStream(json.getBytes());
    DataInputStream din = new DataInputStream(input);

    try {
        DatumReader reader = new GenericDatumReader(schema);
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
        reader.read(null, decoder);
        return true;
    } catch (AvroTypeException e) {
        System.out.println(e.getMessage());
        return false;
    }
}

}
