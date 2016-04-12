package net.catten.db.importer.line.core;

import net.catten.db.importer.line.model.TargetInfo;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Created by Catten on 2016/4/6.
 */
public class Importer {

    private File[] files;
    private TargetInfo targetDBInfo;
    private RawDataReader rawDataReader;

    private boolean noRepeat;
    private boolean preRead;

    public Importer(File[] fileList, Properties properties) throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        this.files = fileList;
        this.targetDBInfo = readInfoFormProperties(properties);
        rawDataReader = new RawDataReader(properties);
    }

    private static TargetInfo readInfoFormProperties(Properties properties){
        TargetInfo targetInfo = new TargetInfo();
        targetInfo.setServerName(properties.getProperty("server.name"));
        targetInfo.setDbName(properties.getProperty("server.db"));
        targetInfo.setUsername(properties.getProperty("server.username"));
        targetInfo.setPassword(properties.getProperty("server.password"));
        return targetInfo;
    }

    //File filter for Importer
    private static FilenameFilter fileFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".txt");
        }
    };
    public static FilenameFilter getFileFilter(){
        return fileFilter;
    }

    public void startHandleFileList() throws Throwable{
        startHandleFileList(null);
    }

    //Import files to database, if connection is null, it will create a default connection.
    public void startHandleFileList(Connection connection) throws Throwable{
        Connection currentConnection = connection;
        if(currentConnection == null) currentConnection = connectionFactory(targetDBInfo);
        long startTime = System.currentTimeMillis();
        for(int i = 0; i < files.length; i++){
            System.out.println(String.format("[%d/%d]Current File: %s",i + 1,files.length,files[i].getName()));
            if(noRepeat){
                importFileToDBUseMap(files[i],currentConnection);
            }else{
                importFileToDB(files[i],currentConnection);
            }
            System.out.println();
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("\n\nFinished all task under %ds.\n\n",(endTime - startTime) / 1000);
        if(connection == null) currentConnection.close();
    }

    //Single file procedure
    private void importFileToDB(File file, Connection dbConnection) throws SQLException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        PreparedStatement psql = dbConnection.prepareStatement(rawDataReader.getSqlExp());

        String line;
        int count = 0;
        int currentCount = 0;
        int errors = 0;
        long startTime = 0;
        long endTime = 0;

        if(preRead){
            System.out.println("Pre-read mode on, it may take a lot of memory.");
            System.out.println("Preparing file...");
            Queue<String[]> itemQueue = new LinkedList<>();
            startTime = System.currentTimeMillis();
            do{
                line = reader.readLine();
                try {
                    if (line == null) continue;
                    System.out.printf("\r%d\t",++count);
                    itemQueue.add(rawDataReader.sortOut(line));
                }catch (Throwable e){
                    System.out.printf("error at %d, skip : %s, cause %s\n",count,line,e.toString());
                    errors++;
                }
            }while (line != null);
            endTime = System.currentTimeMillis();
            System.out.println("\nFinished.(" + (endTime - startTime) / 1000 +"s used).");
            if (errors > 0) System.out.println("[!] With " + errors + " warning(s), please check the output.");

            errors = 0;
            System.out.println("Processing...");
            count = itemQueue.size();
            startTime = System.currentTimeMillis();
            while (!itemQueue.isEmpty()){
                System.out.printf("\r%d%%\t%d\t",(++currentCount * 100 / count),currentCount);
                try{
                    String[] aLine = itemQueue.poll();
                    for (int i = 0; i < aLine.length; i++) {
                        psql.setString(i+1,aLine[i]);
                    }
                    psql.execute();
                }catch (Throwable e){
                    System.out.printf("error at %d, skip, cause %s\n",count,e.toString());
                    errors++;
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("\nFinished.(" + (endTime - startTime) / 1000 +"s used)");
        }else{
            System.out.println("Lines: ");
            do {
                line = reader.readLine();
                try {
                    if(line == null) continue;
                    System.out.printf("\r%d\t",++count);
                    rawDataReader.sortOut(psql,line).execute();
                }catch (Throwable e){
                    System.out.printf("error at %d, skip : %s , cause %s\n",count,line,e.toString());
                    errors++;
                }
            }while (line != null);
        }
        psql.close();
        if (errors > 0) System.out.println("[!] With " + errors + " warning(s), please check the output.");
    }

    //Single file procedure with a Map which use for pre-read file.
    private void importFileToDBUseMap(File file, Connection dbConnection) throws SQLException, IOException {
        String line = null;
        long startTime;
        long endTime;
        int count = 0;
        int errors = 0;

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        PreparedStatement ps = dbConnection.prepareStatement(rawDataReader.getSqlExp());
        HashMap<String,String[]> dataPool = new HashMap<>();

        //Start preparing file.
        System.out.println("No-repeat mode on, it may take a lot of memory.");
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
        if (errors > 0) System.out.println("[!] With " + errors + " warning(s), please check the output.");
        System.out.println("Starting import data to database. Total " + String.valueOf(count) + " data.");
        System.out.println("Processing: ");

        //Start insert data to database
        count = dataPool.size();
        startTime = System.currentTimeMillis();
        errors = 0;
        int currentCount = 0;
        for (String s : dataPool.keySet()){
            try {
                System.out.print("\r");
                System.out.printf("%d%%\t%s",(++currentCount * 100 / count),s);
                System.out.print("                         ");
                String[] data = dataPool.get(s);
                ps.setString(1,s);
                for (int i = 0; i < data.length; i++) {
                    ps.setString(i + 2, data[i]);
                }
                ps.execute();
                dataPool.remove(s);
            }catch (Throwable e){
                System.out.printf("error at %d, skip : %s , cause %s\n",count,s,e.toString());
                errors++;
            }
        }
        endTime = System.currentTimeMillis();
        ps.close();
        System.out.println("\nFinished.(" + (endTime - startTime) / 1000 +"s used)");
        if (errors > 0) System.out.println("[!] With " + errors + " warning(s), please check the output.");
    }

    //Create connection
    public Connection connectionFactory(TargetInfo targetInfo) throws SQLException {
        Connection connection = DriverManager.getConnection(
                targetInfo.getDBLink(),
                targetInfo.getUsername(),
                targetInfo.getPassword()
        );
        System.out.println("Get connection successful.");
        return connection;
    }

    public Connection connectionFactory() throws SQLException{
        Connection connection = DriverManager.getConnection(
                targetDBInfo.getDBLink(),
                targetDBInfo.getUsername(),
                targetDBInfo.getPassword()
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

    public boolean isPreRead() {
        return preRead;
    }

    public void setPreRead(boolean preRead) {
        this.preRead = preRead;
    }
}