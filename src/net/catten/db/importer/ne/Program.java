package net.catten.db.importer.ne;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

/**
 * Created by Catten on 2016/4/6.
 */
public class Program {
    public static void main(String[] args) throws Throwable {
        File[] files = null;
        Connection connection = null;
        DBImporter dbImporter;
        DBTargetInfo dbTargetInfo = new DBTargetInfo();
        boolean noRepeat = false;

        try{
            if(args.length <= 0) {
                printHelp();
                System.exit(0);
            }

            //Handle args.
            for(int i = 0; i < args.length; i++){
                switch (args[i].toLowerCase()){
                    case "-d":
                    case "--directory":
                        i++;
                        files = checkFilePath(args[i],DBImporter.getFileFilter());
                        break;

                    case "-s":
                    case "--server":
                        i++;
                        dbTargetInfo.setServerName(args[i]);
                        break;

                    case "-db":
                    case "--database":
                        i++;
                        dbTargetInfo.setDbName(args[i]);
                        break;

                    case "-u":
                    case "--username":
                        i++;
                        dbTargetInfo.setUsername(args[i]);
                        break;

                    case "-p":
                    case "--password":
                        i++;
                        dbTargetInfo.setPassword(args[i]);
                        break;

                    case "-t":
                    case "--table":
                        i++;
                        dbTargetInfo.setTableName(args[i]);
                        break;

                    case "-nr":
                    case "--no-repeat":
                        i++;
                        if (args[i].toLowerCase().equals("t")) noRepeat = true;
                        System.out.printf("You enabled no-repeat primary key function, it will take a lot of physical memory.");
                        break;

                    default:
                        printHelp();
                        System.exit(0);
                        break;
                }
            }

            if (files != null) {
                dbImporter = new DBImporter(files,dbTargetInfo);
                dbImporter.setNoRepeat(noRepeat);
                connection = DBImporter.connectionFactory(dbTargetInfo);
                dbImporter.startHandleFileList(connection, dbTargetInfo);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if(connection != null && !connection.isClosed()) connection.close();
        }
    }

    //Get a list of file that going to handling form console
    private static File[] checkFilePath(String arg, FilenameFilter filenameFilter){
        File file = new File(arg);
        if(!file.exists() || !file.isDirectory()) {
            System.out.println("Path not available");
            return null;
        }

        File[] files = file.listFiles(filenameFilter);
        if(files == null || file.length() <= 0){
            System.out.println("No file found.");
            return null;
        }

        System.out.printf("%d available file(s) found.\n",files.length);
        return files;
    }

    //Print help information.
    private static void printHelp(){
        System.out.println("Database Import Helper 1.0 by CattenLinger");
        System.out.println("DBImporter.jar -d [directory] -s [server:port] -db [database] -u [username] -p [password] -t [table] -nr [t/f]");
        System.out.println("-d  | --directory\tDirectory which contains files for import");
        System.out.println("-s  | --server\t\tServer address with port");
        System.out.println("-db | --database\tTarget Database");
        System.out.println("-u  | --username\tUsername");
        System.out.println("-p  | --password\tPassword (Very sorry for acquire u input it without mask)");
        System.out.println("-t  | --table\t\tTarget table");
        System.out.println("-nr | --no-repeat\tNo repeated primary key(according to first field)");
        System.out.println("\t* This feature will use Hashmap for preparing files, ensure you have enough physical memory.");
        System.out.println("-h  | --help\t\tThis info");
        System.out.println("\nBase on Mysql JDBC Driver. Only for Mysql and mariaDB.");
        System.out.println("\nCurrent OS information:");
        System.out.println("\tSystem\t: "+System.getProperty("os.name"));
        System.out.println("\tVersion\t: "+System.getProperty("os.version"));
        System.out.println("\tJVM\t: "+System.getProperty("java.version"));
        System.out.println("");
    }
}
