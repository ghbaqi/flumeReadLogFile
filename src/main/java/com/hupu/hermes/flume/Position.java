package com.hupu.hermes.flume;

import java.util.Date;

public class Position {
    private String id;
    private Date gmt_created;
    private Date gmt_modified;
    private String host;
    private String path;
    private Long inode;
    private Long pos;
    private Long line_number;
    private String unique_flag;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getGmt_created() {
        return gmt_created;
    }

    public void setGmt_created(Date gmt_created) {
        this.gmt_created = gmt_created;
    }

    public Date getGmt_modified() {
        return gmt_modified;
    }

    public void setGmt_modified(Date gmt_modified) {
        this.gmt_modified = gmt_modified;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getInode() {
        return inode;
    }

    public void setInode(Long inode) {
        this.inode = inode;
    }

    public Long getPos() {
        return pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }

    public Long getLine_number() {
        return line_number;
    }

    public void setLine_number(Long line_number) {
        this.line_number = line_number;
    }

    public String getUnique_flag() {
        return unique_flag;
    }

    public void setUnique_flag(String unique_flag) {
        this.unique_flag = unique_flag;
    }
}
