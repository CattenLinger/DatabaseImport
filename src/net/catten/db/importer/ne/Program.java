package net.catten.db.importer.ne;

import java.io.*;
import java.sql.Connection;
import java.util.Properties;

/**
 * Created by Catten on 2016/4/6.
 */
public class Program {
    public static void main(String[] args) throws Throwable {
        File[] files = null;
        Connection connection = null;
        DBImporter dbImporter;
        //DBTargetInfo dbTargetInfo = new DBTargetInfo();
        Properties properties = new Properties();

        try{
            if(args.length <= 0) {
                printHelp();
                System.exit(0);
            }else {
                properties = handleArgs(args);
            }

            files = checkFilePath(properties.getProperty("target.path"),DBImporter.getFileFilter());

            if (files != null) {
                dbImporter = new DBImporter(files,properties);
                dbImporter.setNoRepeat("f".equals(properties.getProperty("importer.repeatable")));
                dbImporter.setPreRead("t".equals(properties.getProperty("importer.pre-read")));
                connection = dbImporter.connectionFactory();
                dbImporter.startHandleFileList(connection);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if(connection != null && !connection.isClosed()) connection.close();
        }
    }

    private static Properties handleArgs(String[] args) throws IOException {
        Properties properties = new Properties();

        for(int i = 0; i < args.length; i++){
            switch (args[i].toLowerCase()){
                case "-c":
                case "--config":
                    properties = new Properties();
                    properties.load(new InputStreamReader(new FileInputStream(args[++i])));
                    return properties;

                default:
                    printHelp();
                    System.exit(0);
                    break;
            }
        }

        return properties;
    }

    //Get a list of file that going to handling form console
    private static File[] checkFilePath(String arg, FilenameFilter filenameFilter){
        File file = new File(arg);
        if(!file.exists() || !file.isDirectory()) {
            System.out.println("Path not available");
            return null;
        }

        File[] files = file.listFiles(filenameFilter);
        if(files == null || files.length <= 0){
            System.out.println("No file found.");
            return null;
        }

        System.out.printf("%d available file(s) found.\n",files.length);
        return files;
    }

    //Print help information.
    private static void printHelp(){
        System.out.println("Database Import Helper 2.0 by CattenLinger");
        System.out.println("DBImporter.jar -c [config file]");
        System.out.println("-c  | --config\t\tConfig file.");
        System.out.println("-h  | --help\t\tThis info");
        System.out.println("Fields in config file:");
        System.out.println("target.path=path/to/directory\tDirectory for importing");
        System.out.println("server.name=server:port\t\t\tServer name with port");
        System.out.println("server.db=targetDatabase\t\tTarget Database");
        System.out.println("server.username=username\t\tDatabase user");
        System.out.println("server.password=password\t\tDatabase password");
        System.out.println("importer.repeatable=[t/f]\t\tAllow repeated primary key or not(according to first field)");
        System.out.println("reader.pre-read=[t/f]\t\t\tRead file to RAM before import to database.");
        System.out.println("reader.split=char\t\t\t\tSplit char for lines");
        System.out.println("reader.sql=sqlExp\t\t\t\tSql expression, use '?' instead field data.");
        System.out.println("\t* This feature will force enable pre-reading for preparing files, ensure you have enough physical memory.");
        System.out.println("\nBase on Mysql JDBC Driver. Only for Mysql and mariaDB.");
        System.out.println("\nCurrent OS information:");
        System.out.println("\tSystem\t: "+System.getProperty("os.name"));
        System.out.println("\tVersion\t: "+System.getProperty("os.version"));
        System.out.println("\tJVM\t: "+System.getProperty("java.version"));
        System.out.println("");
    }
}
