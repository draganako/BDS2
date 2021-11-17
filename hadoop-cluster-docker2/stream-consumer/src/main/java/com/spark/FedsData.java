package com.spark;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class FedsData implements Serializable {

    private static final long serialVersionUID = -8808172610346770608L;//??????
    
    public String adshex;
    public String flight_id;
    public Float latitude;
    public Float longitude;
    public Integer altitude;
    public Integer speed;
    public Integer track;
    public Integer squawk;
    public String type;
    //public LocalDateTime timestamp;
    public String timestamp;
    public String name;
    public String other_names1;
    public String other_names2;
    public String n_number;
    public String serial_number;
    public Integer mfr_mdl_code;
    public String mfr;
    public String model;
    public Integer year_mfr;
    public Integer type_aircraft;
    public String agency;
 

    public static FedsData createFedsDataFromLine(String line) throws java.text.ParseException {
        String[] data = line.split(";");
        return new FedsData(data);
    }

    public FedsData(String[] data) throws java.text.ParseException
    {
    	adshex=data[0];          
    	flight_id=data[1];
    	latitude=Float.parseFloat(data[2]);
    	longitude = Float.parseFloat(data[3]);
    	altitude= Integer.parseInt(data[4]);
    	speed= Integer.parseInt(data[5]);
    	track= Integer.parseInt(data[6]);
    	squawk = Integer.parseInt(data[7]);
    	type= data[8];
    	//Instant instant = Instant.parse(data[9]);
    	////DateTimeFormatter ft=DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss'Z'");
    	//timestamp = java.util.Date.from(instant);
    	////timestamp=LocalDateTime.parse(data[9],ft);
    	timestamp=data[9];
    	name  = data[10];
    	other_names1= data[11];
    	other_names2 = data[12];
    	n_number = data[13];
    	serial_number = data[14];
    	mfr_mdl_code = Integer.parseInt(data[15]);
    	mfr = data[16];
    	model = data[17];
    	year_mfr= Integer.parseInt(data[18]);
    	type_aircraft= Integer.parseInt(data[19]);
    	agency= data[20];
       
     }

    @Override
    public String toString() {
        return String.format("%s,%s,%e,%e,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%d,%d,%s", adshex, 
        		flight_id,latitude, longitude, altitude, speed,track,squawk, type, timestamp, name,other_names1,
        		other_names2,  n_number, serial_number, mfr_mdl_code, mfr, model, year_mfr, type_aircraft, agency);
    }
    
    

}