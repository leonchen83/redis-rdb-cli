package com.moilioncircle.redis.rdb.cli.metric;

import com.moilioncircle.redis.rdb.cli.glossary.Gateway;

import java.net.URI;

/**
 * @author Baoyi Chen
 */
public class MetricConfigure {
    private URI uri;
    private String user;
    private String pass;
    private Gateway gateway;

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public Gateway getGateway() {
        return gateway;
    }

    public void setGateway(Gateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public String toString() {
        return "MetricConfigure{" +
                "uri=" + uri +
                ", user='" + user + '\'' +
                ", pass='" + pass + '\'' +
                ", gateway=" + gateway +
                '}';
    }
}
