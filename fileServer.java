package Server;

import java.net.*;
import java.io.*;
import java.util.*;

public class fileServer {
	public static void main (String [] args ) throws IOException {
		
	int serverListPort=0;
	Socket connection = null;
	DataOutputStream outNodeInfo = null;  //stream write to the socket
	ObjectOutputStream outFile = null;
	ServerSocket servSkt = null;
	FileInputStream fileStream= null;
	BufferedInputStream  buffStream= null;
	int noOfNodes=0;
	
	//Input arguements
	Scanner fileReader  = new Scanner(System.in);  // Reading from System.in
	System.out.println("Enter a file path to be downloaded : ");
	final String inputFilePath = fileReader.next();
	String extensionRemoved = inputFilePath.split("\\.")[1];
	
	System.out.println("The file extension is : "+extensionRemoved);
	
	ArrayList<String> chunkFile = split(new File(inputFilePath));
	noOfNodes=chunkFile.size();
	System.out.println("Total Chunks created : " + noOfNodes);
	
	ArrayList<Node> nodeNetwork=new ArrayList<>();
	System.out.println("Enter the configuration file path : ");
	final String confFilePath = fileReader.next();
	BufferedReader myReader = new BufferedReader(new FileReader(confFilePath));
	String newLine=myReader.readLine();
	System.out.println("Initializing Peer tp Peer Network");
	
		while (newLine!=null)
		{
			String[] lineContents = newLine.split("\\s");
			if(Integer.parseInt(lineContents[0])==0)
			{
				//Server Info
				serverListPort=Integer.parseInt(lineContents[1]);
				System.out.println("The Server Listening Port is : " +serverListPort);
			}
			else
			{
				Node myNode=new Node();
				myNode.setNodeId(Integer.parseInt(lineContents[0]));
				myNode.setListPort(Integer.parseInt(lineContents[1]));
				myNode.setUploadNode(Integer.parseInt(lineContents[2]));
				myNode.setDownloadNode(Integer.parseInt(lineContents[3]));
				nodeNetwork.add(myNode);
				System.out.println("Node ID :"+ myNode.getNodeId()+ " Listening Port : " +myNode.getListPort()+
						" Upload to : "+myNode.getUploadNode()+" Download From : " +myNode.getDownloadNode());
			}			
			newLine=myReader.readLine();
		}
	
	try{	
	boolean connActive=true;	
	int nodeCounter=0;
	servSkt = new ServerSocket(serverListPort)	;
	System.out.println("Waiting for connection .....");
	
	while(connActive)
		{	
			int i=nodeCounter;
			int downloadFrom=nodeNetwork.get(i).getDownloadNode();
			int downLoadPort=nodeNetwork.get(downloadFrom-1).getListPort();
			String nodeInfo=nodeNetwork.get(i).getNodeId()+" "+nodeNetwork.get(i).getListPort()+" "+
					nodeNetwork.get(i).getDownloadNode()+" "+downLoadPort+" "+
					nodeNetwork.get(i).getUploadNode()+" "+	noOfNodes+" "+extensionRemoved;
			try{
			connection = servSkt.accept();
			outNodeInfo = new DataOutputStream(connection.getOutputStream());
			outNodeInfo.writeUTF(nodeInfo);
			
			String chunkName=inputFilePath+"_"+(i+1);
			System.out.println("Chunk Sent : " + chunkName);
			File newChunkFile= new File(chunkName);
			byte[] chunkArray=new byte[(int)newChunkFile.length()];
			System.out.println("The lenght of chunk array is : "+chunkArray.length);
			fileStream = new FileInputStream(newChunkFile);
			buffStream = new BufferedInputStream(fileStream);
	        buffStream.read(chunkArray, 0, chunkArray.length);
	        outFile = new ObjectOutputStream(connection.getOutputStream());
	        outFile.write(chunkArray,0,chunkArray.length);
			
	        outNodeInfo.flush();
			outFile.flush();
			
			if(nodeCounter==noOfNodes-1)
			{
				connActive=false;
			}
			nodeCounter++;
			}
			finally
			{
				if (outNodeInfo!=null) outNodeInfo.close();
				if (outFile!=null) outFile.close();
				if (fileStream!=null) fileStream.close();
				if (buffStream!=null) buffStream.close();
				if (connection!=null) connection.close();
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
		}
		catch(IOException ioException){
			ioException.printStackTrace();
		}
	}
		
	}
	
	public static ArrayList<String> split(File inputFile) throws IOException {
		
		long fileSize=inputFile.getTotalSpace();
		System.out.println("The size of the file is " + fileSize);
		int part=1;
		int sizeOfChunks=1024*1024; // each chunk of  1 MB
		byte[] buffer = new byte[sizeOfChunks];
		ArrayList<String> fileArray = new ArrayList<>();
		
		@SuppressWarnings("resource")
		BufferedInputStream myStream=new BufferedInputStream(new FileInputStream(inputFile));
		String fileName=inputFile.getName();
		int temp=0;
		while(((temp = myStream.read(buffer)) > 0))
		{
			String chunkName=fileName+"_"+part;
			fileArray.add(chunkName);
			part++;
			File chunkFile=new File(inputFile.getParent(),chunkName);
			try (FileOutputStream out = new FileOutputStream(chunkFile)) {
                out.write(buffer, 0, temp);            
            }			
		}
		return fileArray;
	}
}

class Node {
	
	private int nodeid;
	private int listPort;
	private int uploadNode;
	private int downloadNode;
	
	public int getNodeId() {
		return nodeid;
	}
	public void setNodeId(int node_Id) {
		this.nodeid = node_Id;
	}
	public int getListPort() {
		return listPort;
	}
	public void setListPort(int list_Port) {
		this.listPort = list_Port;
	}
	public int getUploadNode() {
		return uploadNode;
	}
	public void setUploadNode(int upload_Node) {
		this.uploadNode = upload_Node;
	}
	public int getDownloadNode() {
		return downloadNode;
	}
	public void setDownloadNode(int download_Node) {
		this.downloadNode = download_Node;
	}

}

