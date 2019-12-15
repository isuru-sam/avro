package com.example.avrotest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.example.avrotest.avroschema.User;
import com.fasterxml.jackson.databind.ObjectMapper;

class MUser{
	String name;
	String favorite_color;
	int favorite_number;
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFavorite_color() {
		return favorite_color;
	}
	public void setFavorite_color(String favorite_color) {
		this.favorite_color = favorite_color;
	}
	public int getFavorite_number() {
		return favorite_number;
	}
	public void setFavorite_number(int favorite_number) {
		this.favorite_number = favorite_number;
	}
	
}
public class Test {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		ObjectMapper mapper = new ObjectMapper();
		MUser m=new MUser();
		m.setName("Nish");
		m.setFavorite_number(7);
		m.setFavorite_color("bl");
	String s = mapper.writeValueAsString(m)	;
	
	User user = mapper.readValue("{\"name\":\"Nish\",\"favorite_color\":\"bl\",\"favorite_number\":7}",User.class);
System.out.println(user.getFavoriteColor());

	System.out.println(serealizeAvroHttpRequestJSON(user));
	}
	public static String serealizeAvroHttpRequestJSON(
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
}
