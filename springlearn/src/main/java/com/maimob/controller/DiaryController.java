package com.maimob.controller;

import com.alibaba.fastjson.JSONObject;
import com.maimob.utils.HbaseUntil;
import com.maimob.utils.TimeUtil;
import org.apache.catalina.servlet4preview.http.HttpServletRequest;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class DiaryController extends BaseController{

    @CrossOrigin
    @PostMapping("/detail")
    public String getDetai(HttpServletRequest request){

        String str = this.checkPara(request);
        JSONObject json = JSONObject.parseObject(str);
        return getDiaryDetail(json);
    }

    public static String getDiaryDetail(JSONObject json){

        String connector = json.getString("connector");
        String minDate = json.getString("minDate");
        String maxDate = json.getString("maxDate");
        String mobile = json.getString("mobile");
        String uuid = json.getString("uuid");

// 多条件过滤
        FilterList filterList=new FilterList();
        if(connector != null && !"".equals(connector)){
//            SubstringComparator connectorComp = new SubstringComparator(connector);
            SingleColumnValueFilter connectorFilter = new SingleColumnValueFilter(Bytes.toBytes("args"),
                    Bytes.toBytes("connector"),CompareFilter.CompareOp.EQUAL,Bytes.toBytes(connector));
            // 该列没值时直接跳过
            connectorFilter.setFilterIfMissing(true);
            filterList.addFilter(connectorFilter);
        }
        if(uuid != null && !"".equals(uuid)){
//            SubstringComparator uuidComp = new SubstringComparator(uuid);
            SingleColumnValueFilter uuidFilter = new SingleColumnValueFilter(Bytes.toBytes("args"),
                    Bytes.toBytes("uuid"),CompareFilter.CompareOp.EQUAL,Bytes.toBytes(uuid));
            uuidFilter.setFilterIfMissing(true);
            filterList.addFilter(uuidFilter);
        }
        if(mobile != null && !"".equals(mobile)){
//            SubstringComparator mobileComp = new SubstringComparator(mobile);
            SingleColumnValueFilter mobileFilter = new SingleColumnValueFilter(Bytes.toBytes("args"),
                    Bytes.toBytes("mobile"),CompareFilter.CompareOp.EQUAL,Bytes.toBytes(mobile));
            mobileFilter.setFilterIfMissing(true);
            filterList.addFilter(mobileFilter);
        }

        if(minDate == null || "".equals(minDate)){
            minDate = TimeUtil.transformat();
        }

        SingleColumnValueFilter minFilter = new SingleColumnValueFilter(Bytes.toBytes("args"),
                    Bytes.toBytes("dateTime"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(minDate));
        minFilter.setFilterIfMissing(true);
            filterList.addFilter(minFilter);


        if(maxDate != null && !"".equals(maxDate)){
            SingleColumnValueFilter maxFilter = new SingleColumnValueFilter(Bytes.toBytes("args"),
                    Bytes.toBytes("dateTime"),CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(maxDate));
            maxFilter.setFilterIfMissing(true);
            filterList.addFilter(maxFilter);
        }

        List<Map<String,Map<String,String>>> list = new ArrayList<Map<String,Map<String,String>>>();
        try {
            Table table =  HbaseUntil.getTable("diary");
            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result: resultScanner){
                Cell[] cells = result.rawCells();
                Map<String,Map<String,String>> cellmap = new HashMap<String,Map<String,String>>();
                Map<String,String> reqmap = new HashMap<String,String>();
                Map<String,String> repmap = new HashMap<String,String>();
                for(Cell cell:cells){
                    if("args".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                        System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + ":");
                        System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                        reqmap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                    }else{
                        repmap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
                cellmap.put("args",reqmap);
                cellmap.put("resp",repmap);
                list.add(cellmap);
            }
            table.close();
            HbaseUntil.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        baseResponse.setResponse(list);
        return baseResponse.toString();
    }
}
