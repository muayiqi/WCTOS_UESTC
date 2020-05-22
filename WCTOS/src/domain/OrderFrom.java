package domain;

public class OrderFrom {
    private String orderFormID;
    private String dishID;
    private int number;
    private double price;
    private String userID;
    private String storeID;
    private String status;
    private String date;
    private String fromLocation;
    private String toLocation;
    private String content;

    public String getOrderFormID() {
        return orderFormID;
    }

    public void setOrderFormID(String orderFormID) {
        this.orderFormID = orderFormID;
    }

    public String getDishID() {
        return dishID;
    }

    public void setDishID(String dishID) {
        this.dishID = dishID;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getStoreID() {
        return storeID;
    }

    public void setStoreID(String storeID) {
        this.storeID = storeID;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getFromLocation() {
        return fromLocation;
    }

    public void setFromLocation(String fromLocation) {
        this.fromLocation = fromLocation;
    }

    public String getToLocation() {
        return toLocation;
    }

    public void setToLocation(String toLocation) {
        this.toLocation = toLocation;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
