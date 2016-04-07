package net.catten.db.importer.ne;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Created by Catten on 2016/4/6.
 */
public class DBImporter{

    private File[] files;
    private Queue<File> fileQueue;
    private DBTargetInfo targetDBInfo;

    private boolean noRepeat;

    public DBImporter(File[] fileList, DBTargetInfo dbTargetInfo) throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        this.files = fileList;
        fileQueue = new LinkedList<>();
        Collections.addAll(fileQueue,files);
        this.targetDBInfo = dbTargetInfo;
    }

    //File filter for DBImporter
    private static FilenameFilter fileFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".txt");
        }
    };
    public static FilenameFilter getFileFilter(){
        return fileFilter;
    }

    public void startHandleFileList(DBTargetInfo dbTargetInfo) throws Throwable{
        startHandleFileList(null,dbTargetInfo);
    }

    //Import files to database, if connection is null, it will create a default connection.
    public void startHandleFileList(Connection connection, DBTargetInfo dbTargetInfo) throws Throwable{
        Connection currentConnection = connection;
        if(currentConnection == null) currentConnection = connectionFactory(targetDBInfo);
        long startTime = System.currentTimeMillis();
        for(int i = 0; i < files.length; i++){
            System.out.println(String.format("[%d/%d]Current File: %s",i + 1,files.length,files[i].getName()));
            if(noRepeat){
                importFileToDBUseMap(files[i],currentConnection, dbTargetInfo.getTableName());
            }else{
                importFileToDB(files[i],currentConnection, dbTargetInfo.getTableName());
            }
            System.out.println();
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("\n\nFinished all task under %ds.\n\n",(endTime - startTime) / 1000);
        if(connection == null) currentConnection.close();
    }

    //Single file procedure
    private void importFileToDB(File file, Connection dbConnection, String table) throws SQLException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

        String line;
        String sql = "insert into " + table +" values (?,?)";
        PreparedStatement psql = dbConnection.prepareStatement(sql);
        RawDataReader rawDataReader = new RawDataReader();

        int count = 0;
        int errors = 0;
        long startTime = System.currentTimeMillis();
        System.out.print("Lines: ");
        do {
            line = reader.readLine();
            try {
                if(line == null) continue;
                System.out.print("\r");
                System.out.printf("%d\t",++count);
                System.out.print("                         ");
                String[] fields = rawDataReader.sortOut(line);
                psql.setString(1,fields[0]);
                psql.setString(2,fields[1]);
                psql.execute();
            }catch (Throwable e){
                System.out.printf("error at %d, skip : %s , cause %s\n",count,line,e.toString());
                errors++;
            }
        }while (line != null);
        long endTime = System.currentTimeMillis();

        psql.close();
        System.out.println("\nFinished.(" + (endTime - startTime) / 1000 +"s used)");
        System.out.println("[!] With " + errors + " warning(s), please check the output.");

    }

    //Single file procedure with a Map which use for pre-read file.
    private void importFileToDBUseMap(File file, Connection dbConnection, String table) throws SQLException, IOException {
        String line = null;
        long startTime;
        long endTime;
        int count = 0;
        int errors = 0;

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        RawDataReader rawDataReader = new RawDataReader();
        rawDataReader.setSqlExp("insert into " + table +" values (?,?)");
        PreparedStatement ps = dbConnection.prepareStatement(rawDataReader.getSqlExp());
        HashMap<String,String[]> dataPool = new HashMap<>();

        //Start preparing file.
        System.out.println("Preparing file...");
        startTime = System.currentTimeMillis();
        do{
            try {
                count++;
                System.out.printf("\r%d\t",count);
                line = reader.readLine();
                if(line == null) continue;
                String[] strings = rawDataReader.sortOut(line);
                String pKey = strings[0];
                dataPool.put(pKey,Arrays.copyOfRange(strings,1,strings.length));
            }catch (Exception e){
                System.out.printf("error at %d, skip : %s , cause %s\n",count,line,e.toString());
                errors++;
            }
        }while (line != null);
        endTime = System.currentTimeMillis();
        System.out.println("\nFinished at " + String.valueOf((endTime - startTime)/1000) + "s.");
        System.out.println("[!] With " + errors + " warning(s), please check the output.");
        System.out.println("Starting import data to database. Total " + String.valueOf(count) + " data.");
        System.out.print("Processing: ");

        //Start insert data to database
        count = dataPool.size();
        startTime = System.currentTimeMillis();
        errors = 0;
        int currentCount = 0;
        for (String s : dataPool.keySet()){
            try {
                System.out.print("\r");
                System.out.printf("%d%%\t%s",(currentCount * 100 / count),s);
                System.out.print("                         ");
                currentCount++;
                String[] data = dataPool.get(s);
                ps.setString(1,s);
                for (int i = 0; i < data.length; i++) {
                    ps.setString(i + 2, data[i]);
                }
                ps.execute();
            }catch (Throwable e){
                System.out.printf("error at %d, skip : %s , cause %s\n",count,s,e.toString());
                errors++;
            }
        }
        endTime = System.currentTimeMillis();
        ps.close();
        System.out.println("\nFinished.(" + (endTime - startTime) / 1000 +"s used)");
        System.out.println("[!] With " + errors + " warning(s), please check the output.");
    }

    //Create connection
    public static Connection connectionFactory(DBTargetInfo dbTargetInfo) throws SQLException {
        Connection connection = DriverManager.getConnection(
                String.format(
                        "jdbc:mysql://%s/%s",
                        dbTargetInfo.getServerName(),
                        dbTargetInfo.getDbName()
                ),
                dbTargetInfo.getUsername(),
                dbTargetInfo.getPassword()
        );
        System.out.println("Get connection successful.");
        return connection;
    }

    public boolean isNoRepeat() {
        return noRepeat;
    }

    public void setNoRepeat(boolean noRepeat) {
        this.noRepeat = noRepeat;
    }
}