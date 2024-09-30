package com.socialgist.debugtool;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.socialgist.debugtool.model.Cell;
import com.socialgist.debugtool.model.Column;
import com.socialgist.debugtool.model.JsonData;
import com.socialgist.debugtool.model.Row;
import com.socialgist.debugtool.utils.HBaseContainer;
import com.socialgist.debugtool.utils.items.GvpVideo;
import com.socialgist.debugtool.utils.items.StatsTimePeriod;


@Controller
public class RedirectionController {

	@Autowired
	HBaseUtilityContainer hbaseUtilityContainer;

    @GetMapping("/")
    public String redirectToIndex() {
        return "index";
    }

    @GetMapping("/youtube_index")
    public String youtube_index() {
        return "youtube_index.html";
    }

    @GetMapping("/vk_index")
    public String vk_index() {
        return "vk_index.html";
    }
    
    
    @GetMapping("/youtube_stats")
    public String youtube_stats() {
        return "youtube_stats.html";
    }	

    @GetMapping("/vk_stats")
    public String vk_stats() {
        return "vk_stats.html";
    }	
    
    
    
    @GetMapping("/charts")
    public String charts() {
        return "chart.html";
    }	

    @GetMapping("/tables")
    public String tables() {
        return "table.html";
    }	
    
    @GetMapping("/tokensView")
    public String tokensView() {
        return "tokensView.html";
    }	

    @GetMapping("/dbView")
    public String dbView() {
        return "dbView.html";
    }	
    
    @GetMapping("/hbaseView")
    public String hbaseView() {
        return "hbaseView.html";
    }	
    
    /*    
    
    @GetMapping("/index2")
    public String index2() {
        return "index2.html";
    }	

    @GetMapping("/index3")
    public String index3() {
        return "index3.html";
    }	

    @GetMapping("/index4")
    public String index4() {
        return "index4.html";
    }	
    
    @GetMapping("/index5")
    public String index5() {
        return "index5.html";
    }

    @GetMapping("/chart1")
    public String chart1() {
        return "chart1.html";
    }
    
    @GetMapping("/test1")
    public String test1(Model model) throws JsonProcessingException {
    	  // Create a JsonData instance
        JsonData jsonData = new JsonData();

        // Create and populate the "cols" list
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("year", "Year", "string"));
        columns.add(new Column("sales", "Sales", "number"));
        jsonData.setCols(columns);

        // Create and populate the "rows" list
        List<Row> rows = new ArrayList<>();

        // Create the first row
        Row row1 = new Row(2010, "aaa");
        List<Cell> cells1 = new ArrayList<>();
        cells1.add(new Cell("2010"));
        cells1.add(new Cell(2));
        row1.setCells(cells1);
        rows.add(row1);

        // Create the second row
        Row row2 = new Row(2011, "bbb");
        List<Cell> cells2 = new ArrayList<>();
        cells2.add(new Cell("2011"));
        cells2.add(new Cell(1));
        row2.setCells(cells2);
        rows.add(row2);

        // Create the third row
        Row row3 = new Row(2015, "ccc");
        List<Cell> cells3 = new ArrayList<>();
        cells3.add(new Cell("2012"));
        cells3.add(new Cell(3));
        row3.setCells(cells3);
        rows.add(row3);
        
        jsonData.setRows(rows);

        // Now jsonData represents the JSON structure you provided

        // You can use a JSON library like Jackson to serialize it to a JSON string
        // For example, using Jackson's ObjectMapper:
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonMessage = objectMapper.writeValueAsString(jsonData);
            System.out.println(jsonMessage);
            // Set the JSON string as the value of the message variable
            model.addAttribute("message", jsonMessage);        
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }    	
//        model.addAttribute("message", message);
        return "index5";
    }
    
    @GetMapping("/display")
    public String displayString(Model model) {
        String message = "bbb";
        model.addAttribute("message", message);
        return "display";
    }

    @GetMapping("/show_tokens")
    public String tokens(Model model) {
        String message = "Hello, Thymeleaf!";
        model.addAttribute("message", message);
        return "tokens";
    }
    
    @GetMapping("/chart111")
    public String chart(Model model) {
    	String message = "Hello, Thymeleaf!";
        model.addAttribute("message", message);
        return "tokens";
    }

    @GetMapping("/charts")
    public String charts(Model model, @RequestParam(name = "stats_id") String stats_id, @RequestParam(name = "period") String period) throws IOException {

    	StatsTimePeriod stp = StatsTimePeriod.valueOf(period);
    	
        JsonData jsonData = new JsonData();
//        jsonData.buildTimeCollection(StatsTimePeriod.LAST_HOUR);
        jsonData.buildTimeCollection(stp);

        
        String[] charts = stats_id.split(",");
        
        for (String s : charts) {
//          Map<String, Long> map = hbaseUtilityContainer.getStatsMap(s, StatsTimePeriod.LAST_HOUR);
          Map<String, Long> map = hbaseUtilityContainer.getStatsMap(s, stp);
          jsonData.addMap(s, map);
          System.out.println(s);
        }
        
        
//        Map<String, Long> map1 = hbaseUtilityContainer.getStatsMap("test111", StatsTimePeriod.LAST_HOUR);
//        jsonData.addMap("test111", map1);
//        Map<String, Long> map2 = hbaseUtilityContainer.getStatsMap("test222", StatsTimePeriod.LAST_HOUR);
//        jsonData.addMap("test222", map2);
//        Map<String, Long> map3 = hbaseUtilityContainer.getStatsMap("test333", StatsTimePeriod.LAST_HOUR);
//        jsonData.addMap("test333", map3);
        
        
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonMessage = objectMapper.writeValueAsString(jsonData);
            System.out.println(jsonMessage);
            model.addAttribute("message", jsonMessage);        
            model.addAttribute("title", StatsTimePeriod.LAST_HOUR.getTitle());        
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }    	
//        model.addAttribute("message", message);
        return "index5";
    }
    
    @GetMapping("/table")
    public String table(Model model, @RequestParam(name = "stats_id") String stats_id) throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();
//        model.addAttribute("message", jsonMessage);        
        model.addAttribute("title", StatsTimePeriod.LAST_HOUR.getTitle());        
        return "table";
    }
*/
    
    
    
}
