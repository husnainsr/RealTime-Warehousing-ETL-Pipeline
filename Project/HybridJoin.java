import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.Collection;

public class HybridJoin implements Runnable {
    SharedMemory obj;
    MultiValuedMap<String, TransactionData> multiValueMap;
    DoublyQueue hybridList;
    int hybridListSize;
    boolean loadFirst;
    int printRows;
    private Connection conDataWareHouse;
    private Connection conMasterData;
    private Statement stmtMasterData;

    private ResultSet rsMasterData;
    String user;
    String password;
    String portNumber;
    String databaseName;
    String tableName;
    public static volatile boolean stopper = false;
    int rowsStop;
    public HybridJoin(SharedMemory obj,String user,String password,String portNumber,String databaseName,String tableName) {
        this.obj = obj;
        this.user=user;
        this.password=password;
        this.portNumber=portNumber;
        this.tableName=tableName;
        hybridListSize = 1000;
        hybridList = new DoublyQueue(hybridListSize);
        multiValueMap = new ArrayListValuedHashMap<>(1000);
        loadFirst = true;
        printRows = 0;
        rowsStop=0;
        stopper=false;
        try {
            this.conMasterData = DriverManager.getConnection("jdbc:mysql://localhost:"+portNumber+"/"+databaseName, user,password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void loadDataWareHouse(String databaseName,String user,String password,String portNumber)
    {
        try {
            this.conDataWareHouse = DriverManager.getConnection("jdbc:mysql://localhost:"+portNumber+"/"+databaseName, user,password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    void pushToDataWareHouse(TransactionData tempObject) {
        //DateSetup
        String getDate = tempObject.orderDate;
        int year,month,weekOfYear,dayOfMonth, quarter ;

        int productID;
        double productPrice;
        String productName;
        String dayName,dateID;

        int customerID;
        String customerName,gender;

        int suplierID;
        String supplierName;

        int storeID;
        String storeName;

        try {
            //---------------------------------DATE TRANSFORMATION----------------------------------------//
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm");
            LocalDateTime dateTime = LocalDateTime.parse(getDate, formatter);
            LocalDateTime updatedateTime = dateTime.withYear(2019);
            year = updatedateTime.getYear();
            month = updatedateTime.getMonthValue();
            weekOfYear = updatedateTime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            dayName = updatedateTime.getDayOfWeek().name();
            dayOfMonth = updatedateTime.getDayOfMonth();
            quarter = (updatedateTime.getMonthValue() - 1) / 3 + 1;
            dateID= month+"/"+dayOfMonth+"/"+year;
            //-------------------------------------------------------------------------------------------//



        } catch (DateTimeParseException e) {
//            System.out.println("Invalid date format or incorrect date: " + getDate);
            // Handle the incorrect date case here
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm");
            LocalDateTime dateTime = LocalDateTime.parse(getDate, formatter);
            year = dateTime.getYear();
            month = dateTime.getMonthValue();
            weekOfYear = dateTime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            dayName = dateTime.getDayOfWeek().name();
            dayOfMonth = dateTime.getDayOfMonth();
            quarter = (dateTime.getMonthValue() - 1) / 3 + 1;
            dateID= month+"/"+dayOfMonth+"/"+year;
        }

        productID= tempObject.productID;
        productPrice=Double.parseDouble((tempObject.productPrice).replace("$", ""));
        productName= tempObject.productName;

        customerID= tempObject.customerID;
        customerName= tempObject.customerName;
        gender= tempObject.gender;

        supplierName= tempObject.supplierName;

        storeID=tempObject.storeID;
        storeName= tempObject.storeName;

        //-----------------------------------Insertion--------------------------------------------------
        Statement stmtDataWare;

        ResultSet rsDataWare;

        String insertsql;
        String checksql;
        boolean checkExist;

        try {
            //insertion into Supplier Tabl
            String sqlSupplier = "insert ignore into suppliers values(?)";
            PreparedStatement insertSupplier = this.conDataWareHouse.prepareStatement(sqlSupplier);
            insertSupplier.setString(1, supplierName);
            insertSupplier.executeUpdate();

           //insertion into customerTable
            String sqlCustomer = "insert ignore into customers values(?, ?, ?)";
            PreparedStatement insertCustomer = this.conDataWareHouse.prepareStatement(sqlCustomer);
            insertCustomer.setInt(1, customerID);
            insertCustomer.setString(2, customerName);
            insertCustomer.setString(3, gender);
            insertCustomer.executeUpdate();

            //insertion into products
            String sqlProducts = "insert ignore into products values(?, ?, ?)";
            PreparedStatement insertProduct = this.conDataWareHouse.prepareStatement(sqlProducts);
            insertProduct.setInt(1, productID);
            insertProduct.setString(2, productName);
            insertProduct.setDouble(3, productPrice);
            insertProduct.executeUpdate();

            //insertion into store
            String sqlStore = "insert ignore into stores values(?, ?)";
            PreparedStatement insertStore = this.conDataWareHouse.prepareStatement(sqlStore);
            insertStore.setInt(1, storeID);
            insertStore.setString(2, storeName);
            insertStore.executeUpdate();


            //insertion into Date TableS
            String sqlDate = "insert ignore into dates values(?, ?, ?, ?, ?, ?,?)";
            PreparedStatement insertDate = this.conDataWareHouse.prepareStatement(sqlDate);
            insertDate.setString(1, dateID);
            insertDate.setInt(2, year);
            insertDate.setInt(3, month);
            insertDate.setInt(4, weekOfYear);
            insertDate.setInt(5, dayOfMonth);
            insertDate.setString(6, dayName);
            insertDate.setInt(7,quarter);
            insertDate.executeUpdate();

            //insertion into sales Table
            String sqlSales = "insert into sales values(?, ?, ?, ?, ?, ?)";
            PreparedStatement insertSales = this.conDataWareHouse.prepareStatement(sqlSales);
            insertSales.setInt(1, productID);
            insertSales.setInt(2, customerID);
            insertSales.setString(3, dateID);
            insertSales.setInt(4, storeID);
            insertSales.setString(5, supplierName);
            insertSales.setInt(6, tempObject.quantityOrdered);
            insertSales.executeUpdate();

        }

        catch (Exception e) {
            System.out.println(e);
        }






    }

    public ArrayList<MasterData> load(String productIdToMatch) throws SQLException {
//        System.out.println(productIdToMatch);
        ArrayList<MasterData> diskBuffer = new ArrayList<>();
        this.stmtMasterData = this.conMasterData.createStatement();
//        this.rsMasterData=this.stmtMasterData.executeQuery("SELECT * FROM products LIMIT " + startRow + ", 9");
        this.rsMasterData = this.stmtMasterData.executeQuery("SELECT * FROM "+tableName+" WHERE productID >= " + productIdToMatch + " LIMIT 10;");

        while (this.rsMasterData.next()) {
            MasterData temp = new MasterData(this.rsMasterData.getInt("productID"), this.rsMasterData.getString("productName"), this.rsMasterData.getString("productPrice"),
                    this.rsMasterData.getInt("supplierID"), this.rsMasterData.getString("supplierName"), this.rsMasterData.getInt("storeID"), this.rsMasterData.getString("storeName"));
            diskBuffer.add(temp);
        }


        return diskBuffer;
    }

    @Override
    public void run() {
            try {

                while (hybridList.currentSize < hybridListSize && !stopper) {
                    TransactionData temp = obj.getFromStream();
//                temp.displayTransactionData();
                    hybridList.enque(Integer.toString(temp.productID)); //storing productIDS into Queue
                    String key = Integer.toString(temp.productID);
                    multiValueMap.put(key, temp);  //putting data into hashtable and tuples
//                    System.out.println(multiValueMap.size());

                    if (multiValueMap.size() == 1000 || loadFirst == false) {
                        loadFirst = false   ;

                        ArrayList<MasterData> currentDiskBuffer = load(hybridList.getFront());
//                    System.out.println(hybridList.getFront());
                        for (MasterData m : currentDiskBuffer) {
//                        System.out.println("INININNIN");
                            String keyToMatch = Integer.toString(m.productID);
                            Collection<TransactionData> valuesMatched = multiValueMap.get(keyToMatch);
                            if (valuesMatched != null) {
                                for (TransactionData temporaryTuple : valuesMatched) {
                                    temporaryTuple.joinMaster(m.productName, m.productPrice, m.supplierID, m.supplierName, m.storeID, m.storeName);
                                    if (printRows != 50) {
                                        printRows++;
                                        temporaryTuple.displayJoinedData();

                                    }
                                    pushToDataWareHouse(temporaryTuple);
                                    rowsStop++;
                                }
                                multiValueMap.remove(keyToMatch);
                                hybridList.delete(keyToMatch);
//                                System.out.println(rowsStop);
//                                if(rowsStop>30248)
//                                {
////                                    stopper=true;
//                                    System.out.println("Thread Ended");
////                                    break;
//
//                                }

                            }
                        }
                    }
                    if(rowsStop>30247)
                    {
                        System.out.println("Thread Ended");
                    }

                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
}
