package telran.monitoring.pulse;
import java.net.*;
import java.util.Arrays;

import org.json.JSONObject;
public class PulseReciverAppl
{
	private static final int PORT = 5000;
	private static final int MAX_BUFFER_SIZE = 1500;
	private static final Long PATIENT_ID_PRINTED = 3L;
	static DatagramSocket socket;
	public static void main(String[] args) throws Exception
	{
		socket=new DatagramSocket(PORT);
		byte[] buffer=new byte[MAX_BUFFER_SIZE];
		while(true)
		{
			DatagramPacket packet=new DatagramPacket(buffer,MAX_BUFFER_SIZE);
			socket.receive(packet);
			processRecivedData(buffer, packet);
		}
	}
	
	private static void processRecivedData(byte[] buffer, DatagramPacket packet)
	{
		String json=new String(Arrays.copyOf(buffer, packet.getLength()));
		JSONObject obj=new JSONObject(json);
		Long patientIdLong=obj.getLong("patientId");
		if(patientIdLong==PATIENT_ID_PRINTED) System.out.println(json);		
	}

}
