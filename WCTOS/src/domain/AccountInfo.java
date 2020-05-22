package domain;

public class AccountInfo {
    private String userID;
    private String password;
    private String tel;
    private String type;
    private String location;

    public String getUserID() {
        return userID;
    }

    public String getPassword() {
        return password;
    }

    public String getTel() {
        return tel;
    }

    public String getType() {
        return type;
    }

    public String getLocation() {
        return location;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
