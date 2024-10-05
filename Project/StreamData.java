import java.sql.*;

public class StreamData implements Runnable {
    SharedMemory tempBuffer; //temporary object stored before putting this into main memory
    volatile int sleepTime;//this time is intially dedicated for the wait to look like streaming
    private Connection conTransaction;
    private Statement stmtTransaction;
    private ResultSet rsTransaction;
    String tableName;

    public StreamData(SharedMemory tempBuffer, int sleepTime, String user, String password,String portNumber,String databaseName,String tableName) throws SQLException {
        this.tempBuffer = tempBuffer;
        this.sleepTime = sleepTime;
        this.tableName=tableName;
        try {
            this.conTransaction = DriverManager.getConnection("jdbc:mysql://localhost:"+portNumber+"/"+databaseName, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.stmtTransaction = this.conTransaction.createStatement();
        this.rsTransaction = this.stmtTransaction.executeQuery("SELECT *FROM "+tableName+";");
        System.out.println(conTransaction);

    }


    @Override
    public void run() {
            try {
                int orderID;
                int productID;
                int customerID;
                String customerName;
                String gender;
                int quantityOrdered;
//            System.out.println("Outside");
                while (!HybridJoin.stopper) {
                    if (this.rsTransaction.next()) {
                        customerName = this.rsTransaction.getString("CustomerName");
                        gender = this.rsTransaction.getString("Gender");
                        orderID = this.rsTransaction.getInt("Order ID");
                        productID = this.rsTransaction.getInt("ProductID");
                        customerID = this.rsTransaction.getInt("CustomerID");
                        quantityOrdered = this.rsTransaction.getInt("Quantity Ordered");
                        String orderTimestamp = this.rsTransaction.getString("Order Date");
                        TransactionData obj = new TransactionData(orderID, orderTimestamp, productID, customerID, customerName, gender, quantityOrdered);

                        tempBuffer.putInStream(obj);
//                obj.displayTransactionData();
                        if (HybridJoin.stopper) {
                            break;
                        }
                        Thread.sleep(sleepTime);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    if (rsTransaction != null) rsTransaction.close();
                    if (stmtTransaction != null) stmtTransaction.close();
                    if (conTransaction != null) conTransaction.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

    }

    public void adjustRate(int newSleepTime) {
        this.sleepTime = newSleepTime;
    }
}
