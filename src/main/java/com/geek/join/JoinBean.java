package com.geek.join;

import com.geek.order.topn.OrderBean;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 19:15 2019/3/18
 */
public class JoinBean implements WritableComparable<JoinBean> {

    private String orderId;
    private String userId;
    private String userName;
    private int userAge;
    private String userFriend;
    private String tableName;


    public void set(String orderId, String userId, String userName, int userAge, String userFriend, String tableName) {
        this.orderId = orderId;
        this.userId = userId;
        this.userName = userName;
        this.userAge = userAge;
        this.userFriend = userFriend;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return this.orderId + "," + this.userId + "," + this.userAge + "," + this.userName + "," + this.userFriend;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getUserAge() {
        return userAge;
    }

    public void setUserAge(int userAge) {
        this.userAge = userAge;
    }

    public String getUserFriend() {
        return userFriend;
    }

    public void setUserFriend(String userFriend) {
        this.userFriend = userFriend;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.userId);
        dataOutput.writeUTF(this.userName);
        dataOutput.writeInt(this.userAge);
        dataOutput.writeUTF(this.userFriend);
        dataOutput.writeUTF(this.tableName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.userName = dataInput.readUTF();
        this.userAge = dataInput.readInt();
        this.userFriend = dataInput.readUTF();
        this.tableName = dataInput.readUTF();
    }

    @Override
    public int compareTo(JoinBean o) {
        return this.userId.compareTo(o.getUserId()) == 0 ? o.getTableName().compareTo(this.tableName) : this.userId.compareTo(o.getUserId());

    }

}
