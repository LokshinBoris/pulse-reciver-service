package telran.monitoring.pulse;
import java.net.*;
import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.*;

public class PulseReciverAppl
{
	private static final int PORT = 5000;
	private static final int MAX_BUFFER_SIZE = 1500;
	static DatagramSocket socket;
	static Logger logger=Logger.getLogger("LoggerAppl");
	
	static Level LOGGING_LEVEL=Level.parse(System.getenv("LOGGING_LEVEL"));
	static Integer MAX_THRESHOLD_PULSE_VALUE=Integer.parseInt(System.getenv("MAX_THRESHOLD_PULSE_VALUE"));
	static Integer MIN_THRESHOLD_PULSE_VALUE=Integer.parseInt(System.getenv("MIN_THRESHOLD_PULSE_VALUE"));
	static Integer WARN_MAX_PULSE_VALUE=Integer.parseInt(System.getenv("WARN_MAX_PULSE_VALUE"));
	static Integer WARN_MIN_PULSE_VALUE=Integer.parseInt(System.getenv("WARN_MIN_PULSE_VALUE"));
	
	private static final String TABLE_NAME="pulse_values";
	static AmazonDynamoDB client=AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	static DynamoDB dynamo= new DynamoDB(client);
	static Table table=dynamo.getTable(TABLE_NAME);
	
	public static void main(String[] args) throws Exception
	{
		LogManager.getLogManager().reset();
		logger.setLevel(LOGGING_LEVEL);
		Handler handler=new ConsoleHandler();
		handler.setLevel(Level.FINER);
		logger.addHandler(handler);
		logginConfig();
		logginInfo();
		socket=new DatagramSocket(PORT);
		byte[] buffer=new byte[MAX_BUFFER_SIZE];
		while(true)
		{
			DatagramPacket packet=new DatagramPacket(buffer,MAX_BUFFER_SIZE);
			socket.receive(packet);
			String resievData=loginRecivedData(packet);
			logger.fine("recived: " + resievData);
			processRecivedData(buffer, packet);
		}
	}
	
	private static String loginRecivedData(DatagramPacket packet) 
	{
		String json=new String(Arrays.copyOf(packet.getData(), packet.getLength()));
		JSONObject obj=new JSONObject(json);
		Long patientIdLong=obj.getLong("patientId");
		Long timestampLong=obj.getLong("timestamp");
		Integer valueInt=obj.getInt("value");
		Long seqNumberLong=obj.getLong("seqNumber");
		return String.format("seqNumber = %d patientId = %d value = %d timestamp = %d",
				              seqNumberLong, patientIdLong, valueInt, timestampLong);
	}



	private static void logginInfo() 
	{
		logger.info("Reciving data write to table:"+TABLE_NAME);
	}

	private static void logginConfig()
	{
		logger.config("LOGGING_LEVEL = " + LOGGING_LEVEL);
		logger.config("MAX_THRESHOLD_PULSE_VALUE = " + MAX_THRESHOLD_PULSE_VALUE);
		logger.config("MIN_THRESHOLD_PULSE_VALUE = " + MIN_THRESHOLD_PULSE_VALUE);
		logger.config("WARN_MAX_PULSE_VALUE = " + WARN_MAX_PULSE_VALUE);
		logger.config("WARN_MIN_PULSE_VALUE = " + WARN_MIN_PULSE_VALUE);	
	}

	private static void processRecivedData(byte[] buffer, DatagramPacket packet)
	{
		String json=new String(Arrays.copyOf(buffer, packet.getLength()));
		JSONObject obj=new JSONObject(json);
		Long patientIdLong=obj.getLong("patientId");
		Long timestampLong=obj.getLong("timestamp");
		Integer value=obj.getInt("value");
		Item item=Item.fromJSON(json);
		PutItemSpec putItemSpec = new PutItemSpec().withItem(item);
		table.putItem(putItemSpec);
	
		logger.finer("write to DB patientId = " + patientIdLong + " timestamp = " + timestampLong);
		controlValue(value,patientIdLong);
	}

	private static void controlValue(int value, long patientId) 
	{
		if(value>WARN_MAX_PULSE_VALUE && value<=MAX_THRESHOLD_PULSE_VALUE)
			logger.warning(" warning patientId = " + patientId + ": value of pulse = " + value);
		else if(value<WARN_MIN_PULSE_VALUE && value>=MIN_THRESHOLD_PULSE_VALUE)
			logger.warning(" warning patientId = " + patientId + ": value of pulse = " + value);
		else if(value > MAX_THRESHOLD_PULSE_VALUE)
			logger.severe(" alarm patientId = " + patientId + ": value of pulse = " + value);
		else if(value < MIN_THRESHOLD_PULSE_VALUE)
			logger.severe(" alarm patientId = " + patientId + ": value of pulse = " + value);		
	}

}
