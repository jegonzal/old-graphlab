package org.graphlab.net;

import java.net.InetAddress;

/**
 * Created with IntelliJ IDEA.
 * User: akyrola
 * Date: 11/7/12
 * Time: 4:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class GraphLabNodeInfo {

    private int id;
    private InetAddress address;
    private int port;


    public GraphLabNodeInfo(int id, InetAddress address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public InetAddress getAddress() {
        return address;
    }

    public void setAddress(InetAddress address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String toString() {
        return "Node id=" + id + ", address=" + address + ", port=" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraphLabNodeInfo that = (GraphLabNodeInfo) o;

        if (id != that.id) return false;
        if (port != that.port) return false;
        if (address != null ? !address.equals(that.address) : that.address != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (address != null ? address.hashCode() : 0);
        result = 31 * result + port;
        return result;
    }
}
