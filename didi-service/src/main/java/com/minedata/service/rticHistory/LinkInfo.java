package com.minedata.service.rticHistory;

/**
 * Created by panzongqi on 2017/4/5.
 */
public class LinkInfo {
    /* 当前nilink距离起点的距离 */
    private int slen = 0;
    /* 当前nilink的长度 */
    private int len = 0;

    /* NILINK ID */
    private long linkId;

    private long meshId;

    public LinkInfo(int slen, int len, long linkId, long meshId) {
        this.slen = slen;
        this.len = len;
        this.linkId = linkId;
        this.meshId = meshId;
    }


    public int getSlen() {
        return slen;
    }

    public long getMeshId() {
        return meshId;
    }


    public int getLen() {
        return len;
    }

    public long getLinkId() {
        return linkId;
    }
}
