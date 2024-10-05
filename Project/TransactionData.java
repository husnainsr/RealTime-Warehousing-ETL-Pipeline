import java.util.Date;

public class TransactionData {
    int orderID;
    String orderDate;
    int productID;
    int customerID;
    String customerName;
    String gender;
    int  quantityOrdered;
    //This data will come after the merge happens in hybridJoin
//    int productID;
    String productName;
    String productPrice;
    int supplierID;
    String supplierName;
    int storeID;
    String storeName;

    public void joinMaster(String productName,String productPrice,int supplierID,String supplierName,int storeID,String storeName)
    {
        this.productName=productName;
        this.productPrice=productPrice;
        this.supplierID=supplierID;
        this.supplierName=supplierName;
        this.storeID=storeID;
        this.storeName=storeName;
    }

        public TransactionData(int orderID,String orderDate,int productID,int customerID,String customerName,String gender ,int quantityOrdered)
    {
        this.orderID=orderID;
        this.orderDate=orderDate;
        this.productID=productID;
        this.customerID=customerID;
        this.customerName=customerName;
        this.gender=gender;
        this.quantityOrdered=quantityOrdered;
    }

    void displayTransactionData()
    {
        System.out.println("Order ID: " + orderID + ", Order Date: " + orderDate + ", Product ID: " + productID + ", Customer ID: " + customerID + ", Customer Name: " + customerName + ", Gender: " + gender + ", Quantity Ordered: " + quantityOrdered);
    }

    public void displayMasterData() {
        System.out.println("Product Name: " + productName + ", " +"Product Price: " + productPrice + ", " +"Supplier ID: " + supplierID + ", " + "Supplier Name: " + supplierName + ", " + "Store ID: " + storeID + ", " + "Store Name: " + storeName);
    }


    public void displayJoinedData() {
        System.out.println("Order ID: " + orderID + ", Order Date: " + orderDate + ", Product ID: " + productID + ", Customer ID: " + customerID + ", Customer Name: " + customerName + ", Gender: " + gender + ", Quantity Ordered: " + quantityOrdered +
                ", Product Name: " + productName + ", Product Price: " + productPrice + ", Supplier ID: " + supplierID + ", Supplier Name: " + supplierName + ", Store ID: " + storeID + ", Store Name: " + storeName);
    }

}
