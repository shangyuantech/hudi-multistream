package org.apache.hudi.multistream.rest;

import org.apache.hudi.multistream.common.TaskInfo;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController
@RequestMapping("hudi")
public class HudiMultistreamController {

    @Autowired
    private HudiMultistreamService service;

    @RequestMapping(path = "/getServiceState", method = GET)
    public HashMap<String, Object> getServiceState() {
        HashMap<String, Object> result = new HashMap<>();
        try{
            TaskInfo taskInfo = new TaskInfo(service.getCurrentTopic(), service.getSuccessTasks(), service.getFailedTasks());

            result.put("data", taskInfo);
            result.put("code", "200");
            result.put("msg", "success");
        } catch (Exception e){
            e.printStackTrace();
            result.put("code", "500");
            result.put("msg", "failed");
        }

        return result;
    }
}
