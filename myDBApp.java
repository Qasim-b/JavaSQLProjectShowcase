import java.io.*;
import java.sql.*;
import java.util.*;

public class myDBApp {
	
	// NOTE: You will need to change some variables from START to END.
	public static void main(String[] argv) throws SQLException {
		// START
		// Enter your username.
		System.out.println("enter your username");
		Scanner sc = new Scanner(System.in);
		String user = sc.nextLine();
		// Enter your database password, NOT your university password.
		System.out.println("enter your password");
		String password = sc.nextLine();
		/** IMPORTANT: If you are using NoMachine, you can leave this as it is.
		 * 
	 	 *  Otherwise, if you are using your OWN COMPUTER with TUNNELLING:
	 	 * 		1) Delete the original database string and 
	 	 * 		2) Remove the '//' in front of the second database string.
	 	 */
		String database = "teachdb.cs.rhul.ac.uk";
		//String database = "localhost";
		// END
		
		Connection connection = connectToDatabase(user, password, database);
		if (connection != null) {
			System.out.println("SUCCESS: You made it!"
					+ "\n\t You can now take control of your database!\n");
		} else {
			System.out.println("ERROR: \tFailed to make connection!");
			System.exit(1);
		}
		// Now we're ready to use the DB. You may add your code below this line.
		
		dropTable(connection, "airport");
		createTable(
				connection,
				"airport(airportCode varchar(5), "
				+ "airportName varchar(70), City varchar(35), State varchar(35), "
				+ "primary key(airportCode));");
		int rows = insertIntoTableFromFile(connection, "airport",
				"./src/airport"); //relative path to src folder
		System.out.println(rows + " rows inserted.");
		//String query = "SELECT * FROM airport;";
		//ResultSet rs = (executeQuery(connection,query));
		//try {
		//	while(rs.next()) {
		//		System.out.println(rs.getString(1)+" "+rs.getString(2)+" "+
		//				rs.getString(3)+" "+rs.getString(4));
		//	}
		//}catch(SQLException e) {
		//	e.printStackTrace();
		//}
		//System.out.println(rows + " rows inserted.");
		//String query2= "select count(*) from airport";
		//ResultSet rs2 = (executeQuery(connection,query2));
		//rs2.next();
		//int count = rs2.getInt(1);
		//System.out.println(count + " rows inserted.");
		//rs2.close();
		dropTable(connection,"delayedFlights");
		createTable(connection,"delayedFlights(IDofDelayedFlight numeric(10) primary key, Month numeric(10), "
				+ "DayofMonth numeric(10), DayOfWeek numeric(10), DepTime numeric(10), "
				+ "ScheduledDepTime numeric(10), ArrTime numeric(10), ScheduledArrTime numeric(10), UniqueCarrier varchar(5), "
				+ "FlightNum numeric(10), ActualFlightTime numeric(10), ScheduledFlightTime numeric(10), "
				+ "AirTime numeric(10), ArrDelay numeric(10), DepDelay numeric(10), Orig varchar(10), "
				+ "Dest varchar(10), Distance numeric(10),"
				+ "foreign key(Orig) references airport(airportCode),"
				+ "foreign key(Dest) references airport(airportCode));");
		int rows2 = insertIntoTableFromFile(connection,"delayedFlights",
				"./src/delayedFlights"); //relative path to src folder
		System.out.println(rows2+ " rows inserted.");
		
		System.out.println("------------query 1----------------");
		String query = "SELECT UniqueCarrier,COUNT(*) as Total"
				+ " FROM delayedFlights"
				+ " WHERE(DepDelay>0)"
				+ " GROUP BY UniqueCarrier"
				+ " ORDER BY Total DESC;";
		ResultSet rs = (executeQuery(connection,query));
			int count = 0;	
			try {
					while(rs.next() && count <5) {
						System.out.println(rs.getString(1)+" "+rs.getString(2));
						count++;
					}
				}catch(SQLException e) {
					e.printStackTrace();
				}
			rs.close();
			
		System.out.println("------------query 2----------------");
		String query2 = "SELECT a.City, COUNT(*) as Total2"
				+ " FROM delayedFlights AS df"
				+ " JOIN airport as a"
				+ "	ON df.Orig = a.airportCode"
				+ " WHERE(df.DepDelay > 0)"
				+ " GROUP BY a.City"
				+ " ORDER BY Total2 DESC;";
		ResultSet rs2 = (executeQuery(connection,query2));
			int count2 = 0;	
			try {
					while(rs2.next() && count2 <5) {
						System.out.println(rs2.getString(1)+" "+rs2.getString(2));
						count2++;
					}
				}catch(SQLException e) {
					e.printStackTrace();
				}
			rs2.close();
			
		System.out.println("------------query 3----------------");
		String query3 = "SELECT Dest,SUM(ArrDelay) as TotalArrDelay"
				+ " FROM delayedFlights"
				+ " GROUP BY Dest"
				+ " ORDER BY TotalArrDelay DESC;";
		ResultSet rs3 = (executeQuery(connection,query3));
			int count3 = 0;	
			try {
					while(rs3.next() && count3 <6) {
						if (count3!=0) {
						System.out.println(rs3.getString(1)+" "+rs3.getString(2));
					}
						count3++;
					}
				}catch(SQLException e) {
					e.printStackTrace();
				}
			rs3.close();
	
			System.out.println("------------query 4----------------INCOMPLETE");
			String query4 = "SELECT Distinct(a.State), COUNT(*) as Total4"
					+ " FROM delayedFlights AS df"
					+ " JOIN airport as a"
					+ "	ON df.Orig = a.airportCode"
					+ " WHERE(df.DepDelay > 0)"
					+ " GROUP BY a.State"
					+ " ORDER BY Total4 DESC;";
			ResultSet rs4 = (executeQuery(connection,query4));
			int count4 = 0;	
			try {
					while(rs4.next() && count4 <5) {
						System.out.println(rs4.getString(1)+" "+rs4.getString(2));
						count4++;
					}
				}catch(SQLException e) {
					e.printStackTrace();
				}
			rs4.close();
}
	// You can write your new methods here.
	public static ResultSet executeQuery(Connection connection, String query) { //taken from lab7 code
		System.out.println("DEBUG: Executing query...");
		try {
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery(query);
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
 }
	public static void createTable(Connection connection, //taken from lab7 code
			String tableDescription) {
		Statement st = null;
		try {
			st = connection.createStatement();
			st.execute("CREATE TABLE " + tableDescription);
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void dropTable(Connection connection, String table) { //taken from lab7 code and modified
		Statement st = null;
		try {
			st = connection.createStatement();
			st.execute("DROP TABLE IF EXISTS " + table + " CASCADE");
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static int insertIntoTableFromFile(Connection connection, //taken from lab7 code
			String table, String file) {
		BufferedReader br = null;
		int numRows = 0;
		try {
			Statement st = connection.createStatement();
			String sCurrentLine, brokenLine[], composedLine = "";
			br = new BufferedReader(new FileReader(file));

			while ((sCurrentLine = br.readLine()) != null) {
				// Insert each line to the DB
				brokenLine = sCurrentLine.split(",");
				composedLine = "INSERT INTO " + table + " VALUES (";
				int i;
				for (i = 0; i < brokenLine.length - 1; i++) {
					composedLine += "'" + brokenLine[i] + "',";
				}
				composedLine += "'" + brokenLine[i] + "')";
				numRows = st.executeUpdate(composedLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return numRows;
	}
	
	// ADVANCED: This method is for advanced users only. You should not need to change this!
	public static Connection connectToDatabase(String user, String password, String database) {
		System.out.println("------ Testing PostgreSQL JDBC Connection ------");
		Connection connection = null;
		try {
			String protocol = "jdbc:postgresql://";
			String dbName = "/CS2855/";
			String fullURL = protocol + database + dbName + user;
			connection = DriverManager.getConnection(fullURL, user, password);
		} catch (SQLException e) {
			String errorMsg = e.getMessage();
			if (errorMsg.contains("authentication failed")) {
				System.out.println("ERROR: \tDatabase password is incorrect. Have you changed the password string above?");
				System.out.println("\n\tMake sure you are NOT using your university password.\n"
						+ "\tYou need to use the password that was emailed to you!");
			} else {
				System.out.println("Connection failed! Check output console.");
				e.printStackTrace();
			}
		}
		return connection;
	}
}