package com.harvicom.kafkastreams.fiveminwindow.processor;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.databind.MappingIterator;

import java.io.*;
import java.util.*;

public class LookupTable2 {

    public List<Map<String, String>> readCSV() {
        Reader reader=null;
        List<Map<String, String>> lookupList = null;

        File file = new File("/Users/ottalk/Github/kafka-streams-processing/five-min-window/src/main/resources/LookupTable.csv");
        try {
            reader = new FileReader(file);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        }

        MappingIterator<Map<String, String>> iterator=null;
        try {
            iterator = new CsvMapper().readerFor(Map.class).with(CsvSchema.emptySchema().withHeader()).readValues(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        /* 
        while (iterator.hasNext()) {
            Map<String, String> keyVals = iterator.next();
            System.out.println(keyVals);
        }
        */

        try {
            if( iterator!=null) {
                lookupList = iterator.readAll();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lookupList;
    }

    public static void main(String[] args) {
        Map<String,String> foundMap=null;
        //List<Map<String,String>> foundMapList=null;

        LookupTable2 lookupTable2 = new LookupTable2();
        List<Map<String, String>> lookupList = lookupTable2.readCSV();

        //foundMap = lookupList.stream().filter(Id -> "123000020001".equals(foundMap.get("Id"))).findAny().orElse(null);
        foundMap = lookupList.stream().filter(Id -> Id.get("Id").equals("123000020001")).findFirst().orElse(null);
        System.out.println(foundMap);
    
    }
}
