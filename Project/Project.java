import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Scanner;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import static org.apache.commons.collections4.MapUtils.multiValueMap;


class SharedMemory {
    Queue<TransactionData> streamBuffer;
    int streamSize;
    public volatile boolean stopThread = true;
    public  SharedMemory(int streamSize)
    {
        streamBuffer = new LinkedList<>();
        this.streamSize= streamSize;
    }


    public synchronized void putInStream(TransactionData obj) throws InterruptedException {
        while (streamBuffer.size() == streamSize) {
            wait();
        }
        streamBuffer.add(obj);
        notifyAll();
    }

    public synchronized TransactionData getFromStream() throws InterruptedException {
        while (streamBuffer.isEmpty()) {
            wait();
        }
        TransactionData obj = streamBuffer.remove();
        notifyAll();
        return obj;
    }

    public synchronized int getBufferSize()
    {
        return streamBuffer.size();
    }
}


public class Project {
    public static void main(String[] args) throws SQLException {
        Scanner scanner = new Scanner(System.in);
        int capcityStream = 1000;
        int sleepTime = 2;
        SharedMemory buffer = new SharedMemory(capcityStream);

        System.out.print("Enter streaming database name: ");
        String streamDatabase = scanner.nextLine();
        System.out.print("Enter streaming table name: ");
        String streamTable = scanner.nextLine();
        System.out.print("Enter streaming username: ");
        String streamUser = scanner.nextLine();
        System.out.print("Enter streaming password: ");
        String streamPassword = scanner.nextLine();
        System.out.print("Enter streaming port number: ");
        String streamPort = scanner.nextLine();


        System.out.print("Enter MasterData database name: ");
        String masterDatabase = scanner.nextLine();
        System.out.print("Enter MasterData table name: ");
        String masterTable = scanner.nextLine();
        System.out.print("Enter MasterData username: ");
        String masterUser = scanner.nextLine();
        System.out.print("Enter MasterData password: ");
        String masterPassword = scanner.nextLine();
        System.out.print("Enter MasterData port number: ");
        String masterPort = scanner.nextLine();

        System.out.print("Enter DataWareHouse Database name: ");
        String dwDatabase = scanner.nextLine();
        System.out.print("Enter DataWareHouse username: ");
        String dwUser = scanner.nextLine();
        System.out.print("Enter DataWareHouse password: ");
        String dwPassword = scanner.nextLine();
        System.out.print("Enter DataWareHouse port number: ");
        String dwPort = scanner.nextLine();

        StreamData stream = new StreamData(buffer, sleepTime, streamUser, streamPassword,streamPort,streamDatabase,streamTable);
        HybridJoin hybrid = new HybridJoin(buffer,masterUser,masterPassword,masterPort,masterDatabase,masterTable);
        hybrid.loadDataWareHouse(dwDatabase,dwUser,dwPassword,dwPort);

//        StreamData stream = new StreamData(buffer, sleepTime, "root", "root","3306","transaction","transactions");
//        HybridJoin hybrid = new HybridJoin(buffer,"root","root","3306","masterdata","master_data");
        Controller control = new Controller(buffer, stream);
//        hybrid.loadDataWareHouse("ELECTRONICA-DW","root","root","3306");

        Thread streamThread = new Thread(stream);
        Thread hybridThread = new Thread(hybrid);
        Thread controllerThread = new Thread(control);


        streamThread.start();
        hybridThread.start();
        controllerThread.start();



    }
}


