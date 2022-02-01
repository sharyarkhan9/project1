import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project1 {
    def main(args: Array[String]): Unit = {
        // This block of code is all necessary for spark/hive/hadoop
        var scanner = new Scanner(System.in)
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // change if winutils.exe is in a different bin folder
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1") 
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._

        //This block to connect to mySQL
        val driver = "com.mysql.cj.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/p1"
        val username = "root"
        val password = "F@ith6193"
        var connection:Connection = null

        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement() 

        // Method to check login credentials
        val adminCheck = login(connection)
        if (adminCheck) {
            println("Welcome Admin!")
            var choice = 0
            while (choice != 4) {
            println("Please Choose an option using number keys 1 - 3: ")
            println("1: Delete user account\n2: Create a user account\n3: Change password \n4: exit")
            try {
                choice = scanner.nextInt()
            }catch {
                case e: Exception => print("Exception thrown - Non numeric key entered. ")
            }
            scanner.nextLine()
            if (choice == 1){
                println("enter the username which you would like to delete: ")
                var user = scanner.nextLine()
                var user2 = ""
                user2 += ("'"+user+"'")
                statement.executeUpdate("DELETE FROM user_accounts where username=("+user2+")")

            }
            else if (choice == 2){
                println("enter the username for the new user: ")
                var uname = scanner.nextLine()
                var un = ""
                un += ("'"+uname+"'")
                println("enter the password for the new user: ")
                var pword = scanner.nextLine()
                var pw = ""
                pw += ("'"+pword+"'")
                try {
                statement.executeUpdate("INSERT INTO user_accounts (username, password) values ("+un+", "+pw+")")
                }catch {
                case e: Exception => println("Unable to create new user account. Username already exists. ")
            }

            }
            else if (choice == 3){
                println("enter the username of the user whose password you would like to change: ")
                var uname = scanner.nextLine()
                var un = ""
                un += ("'"+uname+"'")
                println("enter the updated password: ")
                var pword = scanner.nextLine()
                var pw = ""
                pw += ("'"+pword+"'")
                statement.executeUpdate("UPDATE user_accounts set password = ("+pw+") where username = ("+un+")")

            }
            else if (choice == 4) {println("Thank you. Have a great day!")}
            else {println("Please Choose an option using number keys 1 - 3: ")}

        }
        }else {
            println("Welcome User!")
            var count = 0
            while (count != 7) {
            println("Please Choose an option using number keys 1 - 5 to run a query of your choice: ")
            println("1: Top 10 countries with the highest covid cases\n2: Top 10 countries with the highest deaths\n3: Top 10 countries with the highest cases/death ratio")
            println("4: Top 10 counties with the highest daily new cases\n5: Top 10 countries with the highest daily new deaths")
            println("6: Top 10 counties with the highest daily new deaths over new daily cases ratio\n7: Exit")
            try {
                count = scanner.nextInt()
            }catch {
                case e: Exception => print("Exception thrown - Non numeric key entered. ")
            }
            scanner.nextLine()
            if (count == 1){
                val summary = hiveCtx.sql("SELECT country, max(Tcases) MaxCases from data2 group by country order by MaxCases DESC LIMIT 10")
                summary.show()
                summary.write.mode("overwrite").csv("results/top10MaxCases")
            }
            else if (count == 2){
                val summary = hiveCtx.sql("SELECT country, max(Tdeaths) MaxDeaths from data2 group by country order by MaxDeaths DESC LIMIT 10")
                summary.show()
                summary.write.mode("overwrite").csv("results/top10MaxDeaths")
            }
            else if (count == 3){
                val summary = hiveCtx.sql("SELECT country, float(max(Tdeaths)/max(Tcases)) Death_Ratio from data2 group by country order by Death_Ratio DESC LIMIT 10")
                summary.show()
                summary.write.mode("overwrite").csv("results/top10MaxDeathRatio")
            }
            else if (count == 4){
                val summary = hiveCtx.sql("SELECT country, max(Newcases) Max_New_Cases from data2 group by country order by Max_New_Cases DESC LIMIT 10")
                summary.show()
                summary.write.mode("overwrite").csv("results/top10MaxNewCases")
            }
            else if (count == 5){
                val summary = hiveCtx.sql("SELECT country, max(dDeaths) Max_New_Deaths from data2 group by country order by Max_New_Deaths DESC LIMIT 10")
                summary.show()
                summary.write.mode("overwrite").csv("results/top10MaxNewDeaths")
            }
            else if (count == 6){
                val summary = hiveCtx.sql("SELECT country, float(max(NewCases)/max(dDeaths)) DailyCases_DailyDeaths from data2 group by country order by DailyCases_DailyDeaths DESC LIMIT 10")
                summary.show()
            }
            else if (count == 7){
                println("Thank you. Have a great day!")
            }
            else {println("Please only enter numbers 1 - 7")}
        }
        }
        // Run method to insert Covid data. Only needs to be ran initially, then table data1 will be persisted.
        insertCovidData(hiveCtx)
        /*
        * Here is where I would ask the user for input on what queries they would like to run, as well as
        * method calls to run those queries. An example is below, top10DeathRates(hiveCtx) 
        */
        //top10DeathRates(hiveCtx)
        sc.stop() // Necessary to close cleanly. Otherwise, spark will continue to run and run into problems.
    }
    // This method checks to see if a user-inputted username/password combo is part of a mySQL table.
    // Returns true if admin, false if basic user, gets stuckl in a loop until correct combo is inputted (FIX)
    def login(connection: Connection): Boolean = {
        
        while (true) {
            val statement = connection.createStatement()
            val statement2 = connection.createStatement()
            println("Enter username: ")
            var scanner = new Scanner(System.in)
            var username = scanner.nextLine().trim()

            println("Enter password: ")
            var password = scanner.nextLine().trim()
            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM admin_accounts WHERE username='"+username+"' AND password='"+password+"';")
            while ( resultSet.next() ) {
                if (resultSet.getString(1) == "1") {
                    return true;
                }
            }

            val resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM user_accounts WHERE username='"+username+"' AND password='"+password+"';")
            while ( resultSet2.next() ) {
                if (resultSet2.getString(1) == "1") {
                    return false;
                }
            }

            println("Username/password combo not found. Try again!")
        }
        return false
    }

    def insertCovidData(hiveCtx:HiveContext): Unit = {
        // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
        // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can 
        // then be 
        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/covid.csv")
        // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries. 
        // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
        // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this 
        // code as well as the creation of output will not be necessary.
        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS data2 (Date STRING, Tcases DOUBLE, Newcases DOUBLE, ActiveCases DOUBLE, Tdeaths DOUBLE, dDeaths DOUBLE) PARTITIONED BY (country STRING) clustered by (Date) INTO 20 BUCKETS")
        hiveCtx.sql("INSERT INTO data2 SELECT * FROM temp_data")
        // To query the data1 table. When we make a query, the result set ius stored using a dataframe. In order to print to the console, 
        // we can use the .show() method.
    }
}
