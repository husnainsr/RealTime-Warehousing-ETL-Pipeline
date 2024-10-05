public class MasterData {
    int productID;
    String productName;
    String productPrice;
    int supplierID;
    String supplierName;
    int storeID;
    String storeName;

    public MasterData(int productID,String productName,String productPrice,int supplierID,String supplierName,int storeID,String storeName)
    {
        this.productID=productID;
        this.productName=productName;
        this.productPrice=productPrice;
        this.supplierID=supplierID;
        this.supplierName=supplierName;
        this.storeID=storeID;
        this.storeName=storeName;
    }

    public void displayMasterData() {
        System.out.println("Product ID: " + productID + ", "+"Product Name: " + productName + ", " +"Product Price: " + productPrice + ", " +"Supplier ID: " + supplierID + ", " + "Supplier Name: " + supplierName + ", " + "Store ID: " + storeID + ", " + "Store Name: " + storeName);
    }

}
















