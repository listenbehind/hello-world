package com.minedata.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class LogEntity {
    private String table;
    private JSONObject content;


    public static void main(String[] args) {
        LogEntity log = new LogEntity();
        log.setContent();
        log.setTable("datalist");
        System.out.println(JSON.toJSONString(log));
    }



    public JSON getContent() {
        return content;
    }



    public void setContent(JSONObject content) {
        this.content = content;
    }



    public void setContent() {
        JsonStr jsonStr = new JsonStr();
        jsonStr.setCoord("gcj02");
        jsonStr.setPath("/data/hushi/didi/20170508.gz");
        jsonStr.setData_type("didiTrajectory");
        jsonStr.setDesc("didi");
        jsonStr.setSize("181351239");
        jsonStr.setSplits(",");
        jsonStr.setAdmincode("110000");
        // this.content = new String(JSON.toJSONString(jsonStr).getBytes(), "UTF-8");
        this.content = (JSONObject) JSON.toJSON(jsonStr);
    }



    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }



    public class JsonStr {

        private String path;
        private String version;
        private String coord;
        private String desc;
        private String data_type;
        private String size;
        private String splits;
        private String admincode;

        public String getAdmincode() {
            return admincode;
        }

        public void setAdmincode(String admincode) {
            this.admincode = admincode;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getCoord() {
            return coord;
        }

        public void setCoord(String coord) {
            this.coord = coord;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }

        public String getData_type() {
            return data_type;
        }

        public void setData_type(String data_type) {
            this.data_type = data_type;
        }

        public String getSize() {
            return size;
        }

        public void setSize(String size) {
            this.size = size;
        }

        public String getSplits() {
            return splits;
        }

        public void setSplits(String splits) {
            this.splits = splits;
        }
    }
}
