import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.Scanner;
import java.util.StringTokenizer;

public class adwords {
	
	public static void main(String[] args) throws SQLException, IOException, InterruptedException {
			File systemin = new File("system.in");
			Scanner sysinput;
			String[] readin = new String[8];
			int i = 0;
			try {
				sysinput = new Scanner(systemin);
				while (sysinput.hasNextLine()) {
					String[] input = sysinput.nextLine().split(" ");
					readin[i] = input[input.length - 1];
					i++;
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		    Connection conn =
		      DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",
		                                   readin[0], readin[1]);
		    File querytable = new File("Queries.dat");
		    File adtable = new File("Advertisers.dat");
		    File keytable = new File("Keywords.dat");
		    Scanner query;
		    Scanner advertiser;
		    Scanner keyword;
		    
		    PrintWriter out1 = new PrintWriter("system.out.1", "UTF-8");
			PrintWriter out2 = new PrintWriter("system.out.2", "UTF-8");
			PrintWriter out3 = new PrintWriter("system.out.3", "UTF-8");
			PrintWriter out4 = new PrintWriter("system.out.4", "UTF-8");
			PrintWriter out5 = new PrintWriter("system.out.5", "UTF-8");
			PrintWriter out6 = new PrintWriter("system.out.6", "UTF-8");
		    
		    Statement stmt = conn.createStatement ();
		    stmt.addBatch("CREATE TABLE Queries " + "(qid INTEGER, query VARCHAR(500), primary key(qid))");
		    stmt.addBatch("CREATE TABLE Advertisers " + "(advertiserID INTEGER, budget FLOAT, ctc FLOAT, "
				+"primary key(advertiserID))");
		    stmt.addBatch("CREATE TABLE Keywords " + "(advertiserID INTEGER, keyword VARCHAR(100), bid FLOAT, "
				+"primary key(advertiserID, keyword))");
		    PreparedStatement queryupdate = conn.prepareStatement("INSERT INTO Queries" + "(qid, query)"
				+ "VALUES(?,?)");
		    PreparedStatement adverupdate = conn.prepareStatement("INSERT INTO Advertisers" + "(advertiserID, budget, ctc)"
				+ "VALUES(?,?,?)");
		    PreparedStatement keyupdate = conn.prepareStatement("INSERT INTO Keywords" + "(advertiserID, keyword, bid)"
		    		+ "VALUES(?,?,?)");
		    try {
		    	query = new Scanner(querytable);
		    	advertiser = new Scanner(adtable);
		    	keyword = new Scanner(keytable);
		    	while (query.hasNextLine()) {
		    		StringTokenizer token = new StringTokenizer(query.nextLine(), "	");
		    		queryupdate.setInt(1, Integer.parseInt(token.nextToken()));
		    		queryupdate.setString(2, token.nextToken());
		    		queryupdate.addBatch();
		    	}
		    	while (advertiser.hasNextLine()) {
		    		StringTokenizer token = new StringTokenizer(advertiser.nextLine(), "	");
		    		adverupdate.setInt(1, Integer.parseInt(token.nextToken()));
		    		adverupdate.setFloat(2, Float.parseFloat(token.nextToken()));
		    		adverupdate.setFloat(3, Float.parseFloat(token.nextToken()));
		    		adverupdate.addBatch();
		    	}
		    	while (keyword.hasNextLine()) {
		    		StringTokenizer token = new StringTokenizer(keyword.nextLine(), "	");
		    		keyupdate.setInt(1, Integer.parseInt(token.nextToken()));
		    		keyupdate.setString(2, token.nextToken());
		    		keyupdate.setFloat(3, Float.parseFloat(token.nextToken()));
		    		keyupdate.addBatch();
		    	}
		    } catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    System.out.println("Uploading data.");
		    stmt.executeBatch();
		    queryupdate.executeBatch();
		    adverupdate.executeBatch();
		    keyupdate.executeBatch();
		    System.out.println("Data Updated. Preprocessing begin.");
		    Process p = Runtime.getRuntime().exec("sqlplus " + readin[0] + "@orcl/" + readin[1] + " @adwords.sql");
		    p.waitFor();
		    System.out.println("Preprocessing over. Begin computing");
		    ResultSet rs = stmt.executeQuery("select max(qid) from allinfo");
		    int qnum = 0;
		    if (rs.next()) {
		    	qnum = rs.getInt(1);
		    }
		    CallableStatement cstmt = conn.prepareCall("begin task1(?,"+qnum+"); task2(?,"+qnum+"); task3(?,+"+qnum+"); "
		    		+ "task4(?,"+qnum+"); task5(?,"+qnum+"); task6(?,"+qnum+"); end;");
		    for (int k = 1; k<7 ; k++) {
		    	cstmt.setInt(k, Integer.parseInt(readin[k+1])-1);
		    }
		    cstmt.execute();
		    System.out.println("Task is over. Begin writing out results.");
		    
		    
		    ResultSet rs1 = stmt.executeQuery("select qid, rank, advertiserID, balance, budget from adgreedy order by qid, rank");
		    String qid, rank, adID, balance, budget;
		    while (rs1.next()) {
		    	qid = rs1.getString(1);
		    	rank = rs1.getString(2);
		    	adID = rs1.getString(3);
		    	balance = rs1.getString(4);
		    	budget = rs1.getString(5);
		    	out1.println(String.format("%s, %s, %s, %s, %s", qid, rank, adID, balance, budget));
		    	out1.flush();
		    }
		    ResultSet rs2 = stmt.executeQuery("select qid, rank, advertiserID, balance, budget from adbalance order by qid, rank");
		    while (rs2.next()) {
		    	qid = rs2.getString(1);
		    	rank = rs2.getString(2);
		    	adID = rs2.getString(3);
		    	balance = rs2.getString(4);
		    	budget = rs2.getString(5);
		    	out2.println(String.format("%s, %s, %s, %s, %s", qid, rank, adID, balance, budget));
		    	out2.flush();
		    }
		    ResultSet rs3 = stmt.executeQuery("select qid, rank, advertiserID, balance, budget from gebalance order by qid, rank");
		    while (rs3.next()) {
		    	qid = rs3.getString(1);
		    	rank = rs3.getString(2);
		    	adID = rs3.getString(3);
		    	balance = rs3.getString(4);
		    	budget = rs3.getString(5);
		    	out3.println(String.format("%s, %s, %s, %s, %s", qid, rank, adID, balance, budget));
		    	out3.flush();
		    }
		    ResultSet rs4 = stmt.executeQuery("select qid, rank, advertiserID, balance, budget from secgreedy order by qid, rank");
		    while (rs4.next()) {
		    	qid = rs4.getString(1);
		    	rank = rs4.getString(2);
		    	adID = rs4.getString(3);
		    	balance = rs4.getString(4);
		    	budget = rs4.getString(5);
		    	out4.println(String.format("%s, %s, %s, %s, %s", qid, rank, adID, balance, budget));
		    	out4.flush();
		    }
		    ResultSet rs5 = stmt.executeQuery("select qid, rank, advertiserID, balance, budget from secbalance order by qid, rank");
		    while (rs5.next()) {
		    	qid = rs5.getString(1);
		    	rank = rs5.getString(2);
		    	adID = rs5.getString(3);
		    	balance = rs5.getString(4);
		    	budget = rs5.getString(5);
		    	out5.println(String.format("%s, %s, %s, %s, %s", qid, rank, adID, balance, budget));
		    	out5.flush();
		    }
		    ResultSet rs6 = stmt.executeQuery("select qid, rank, advertiserID, balance, budget from secgeblnc order by qid, rank");
		    while (rs6.next()) {
		    	qid = rs6.getString(1);
		    	rank = rs6.getString(2);
		    	adID = rs6.getString(3);
		    	balance = rs6.getString(4);
		    	budget = rs6.getString(5);
		    	out6.println(String.format("%s, %s, %s, %s, %s", qid, rank, adID, balance, budget));
		    	out6.flush();
		    }
		    out1.close();
		    out2.close();
		    out3.close();
		    out4.close();
		    out5.close();
		    out6.close();
		    
		    conn.close(); // ** IMPORTANT : Close connections when done **
		  }
		


	

}
