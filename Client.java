package Client;

import java.net.*;
import java.nio.file.Files;
import java.io.*;
import java.util.*;
import java.io.IOException;


public class Client extends Thread {
	
	Socket reqServSocket;
	ObjectOutputStream out;
 	DataInputStream inNodeInfo;
 	ObjectInputStream inFile;
 	FileOutputStream fileOutStream;
 	BufferedOutputStream bufferOutStream;
 	DataInputStream inData;
 	static ArrayList<String> receivedFiles=new ArrayList<>();
 	ArrayList<File> files=new  ArrayList<>();
 	String extension;
	public static void main(String args[]) throws InterruptedException
	{
		Thread receiveThread=new Client();
		receiveThread.start();
		Thread.sleep(10000);
		Thread sendThread=new startClientServer();
		sendThread.start();		
	}
	
	public void run()
	{	
		int server_socket=50560;//50566
		int clientCounter=0;
		int chunks=0;
		boolean connActive=true;
		int newDownPort=0;
		String folderPath="";
		boolean success=false;
		
		try
		{
		while(connActive)
		 {	
						
			int bytesRead=0;
			int current=0;
			int receivedElements=0;
			int node_id=0;
			int flag=0;
			String chunkName="";
			reqServSocket = new Socket("localhost", server_socket);
			System.out.println("Connected to server localhost at port : "+newDownPort);
			inNodeInfo = new DataInputStream(reqServSocket.getInputStream());
			
			String nodeInfo=inNodeInfo.readUTF();
			String[] info=nodeInfo.split("\\s");
			/*int[] infoArray= new int[info.length];
			for (int i=0; i < info.length; i++) 
			{
				infoArray[i] = Integer.parseInt(info[i]);
				System.out.print(i +" element : " +infoArray[i]+"_");
		    }*/
			receivedElements=info.length;
			
			if(receivedElements==7)
			{	
				System.out.println("Getting Data from Server");
				newDownPort=Integer.parseInt(info[3]);
				node_id=Integer.parseInt(info[0]);
				startClientServer.node_id=node_id;
				startClientServer.Listen_Port=Integer.parseInt(info[1]);
				chunkName="chunk"+info[0];
				receivedFiles.add(chunkName);
				startClientServer.fileContained.add(chunkName);
				clientCounter++;
				chunks=Integer.parseInt(info[5]);
				startClientServer.total_chunks=chunks;
				extension=info[6];
			}
			else if (info.length==2)
			{
				System.out.println("Getting Data from Client");
				chunkName=info[1];
				System.out.println("File : " + chunkName +" sent by node : "+info[0]);
				receivedFiles.add(chunkName);
				startClientServer.fileContained.add(chunkName);
				clientCounter++;
			}
			
			
			if(!success)
			{
			 folderPath="C:/Users/Sumeet/Desktop/CN/Client_"+info[0];
			 startClientServer.folderPath=folderPath;	
			 success=new File(folderPath).mkdir();
			 success=true;
			}
			String fileName=folderPath+"/"+chunkName;
			inFile = new ObjectInputStream(reqServSocket.getInputStream());
			inData=new DataInputStream(inFile);
			byte[] chunkArray= new byte[1048576];
			bytesRead=inData.read(chunkArray,0,chunkArray.length);
			current=bytesRead;
			
			do{
				bytesRead=inData.read(chunkArray,current,chunkArray.length-current);
				if(bytesRead==-1)
				{
					break;
				}
				current+=bytesRead;
			} while(current <  1048576);
			FileOutputStream fos = new FileOutputStream(fileName);
			System.out.println("Downloading : "+fileName);
			fos.write(chunkArray,0,chunkArray.length);
			fos.close();
			System.out.println("The files received are : "+receivedFiles);
			if(new File(folderPath).list().length==chunks)
			{		
				connActive=false;
				File f = new File(folderPath);
				files = new ArrayList<File>(Arrays.asList(f.listFiles()));
				mergefiles(files,new File(folderPath+"/final."+extension));
			}
			flag++;
			if (flag==1)
			{
				server_socket=newDownPort;
				System.out.println("New Downloading Port is : "+server_socket);
				Thread.sleep(20000);
			}
		  }
		}
		catch (ConnectException e) {
			System.err.println("Connection refused. You need to initiate a server first.");
	} 
	catch(UnknownHostException unknownHost){
		System.err.println("You are trying to connect to an unknown host!");
	}
	catch(IOException ioException){
		ioException.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	finally{
		//Close connections
		try{
			inNodeInfo.close();
			inFile.close();
			reqServSocket.close();
		}
		catch(IOException ioException){
			ioException.printStackTrace();
		}
	  }
	}
	
	public static void mergefiles(List<File> files, File finalFile) throws IOException 
		{
		    try (BufferedOutputStream mergingStream = new BufferedOutputStream(
		            new FileOutputStream(finalFile))) {
		        for (File f : files) {
		            Files.copy(f.toPath(), mergingStream);
		        }
		    }
		    System.out.println("All Chunks are now merged");
	}
}	


class startClientServer extends Thread{
	
	static int total_chunks;
	static int node_id;
	static int Listen_Port;
	static String folderPath;
	static ArrayList<String> fileContained=new ArrayList<String>();
	ArrayList<String> filesSent=new ArrayList<String>();
	Socket connection = null;
	DataOutputStream outFileInfo = null;  //stream write to the socket
	ObjectOutputStream outFile = null;
	ServerSocket servSkt = null;
	public void run(){
		try{
		
		servSkt = new ServerSocket(Listen_Port);		
		String fileToSend="";
		System.out.println("Listening Port for Client Activated : "+Listen_Port);
		boolean allSent=true;
		while (allSent)
		{	
			for(int i=0;i<fileContained.size();i++)
			{
				if(!filesSent.contains(fileContained.get(i)))
				{
					fileToSend=fileContained.get(i);
					break;
				}
			}
			filesSent.add(fileToSend);
			System.out.println("Peer Server side folder path : "+folderPath);
			FileInputStream fileStream= null;
			BufferedInputStream  buffStream= null;
			String fileInfo=node_id+" " +fileToSend;
			System.out.println("File to be sent : "+fileInfo);
			try{
				connection = servSkt.accept();
				
				outFileInfo = new DataOutputStream(connection.getOutputStream());
				outFileInfo.writeUTF(fileInfo);
				
				String filePath=folderPath+"/"+fileToSend;
				File newChunkFile= new File(filePath);
				byte[] chunkArray=new byte[(int)newChunkFile.length()];
				fileStream = new FileInputStream(newChunkFile);
				buffStream = new BufferedInputStream(fileStream);
		        buffStream.read(chunkArray, 0, chunkArray.length);
		        outFile = new ObjectOutputStream(connection.getOutputStream());
		        outFile.write(chunkArray,0,chunkArray.length);
		        
		        outFileInfo.flush();
		        outFile.flush();
		}
			finally
			{
				if (fileStream!=null) fileStream.close();
				if (buffStream!=null) buffStream.close();
			}
			
			if(filesSent.size()==total_chunks)
			{
				System.out.println("All Parts Received");
			}
		}
	}
		catch(IOException ioException){
			ioException.printStackTrace();
		}
		finally{
			//Close connections
			try{
				if (servSkt!=null) servSkt.close();
				if (outFileInfo!=null) outFileInfo.close();
				if (outFile!=null) outFile.close();
				if (connection!=null) connection.close();
			}
			catch(IOException ioException){
				ioException.printStackTrace();
			}
		}
	}
}
