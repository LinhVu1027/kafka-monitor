package vn.cloud.springkafkamonitor.topic;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/topics")
@RequiredArgsConstructor
public class TopicController {

    private final TopicService topicService;

    @GetMapping("/{topic}")
    public TopicDto getTopicInfo(@PathVariable String topic) {
        return this.topicService.getTopicInfo(topic);
    }

    @GetMapping("/{topic}/records")
    public Map<String, List<ConsumerRecordDto<Object, Object>>> getLatestRecords(@PathVariable String topic) {
        return this.topicService.getLatestRecords(topic);
    }

    @GetMapping("/{topic}/partitions/{partition}/records")
    public List<ConsumerRecordDto<Object, Object>> getNLatestRecords(@PathVariable String topic,
                                                       @PathVariable Integer partition,
                                                       @RequestParam Integer n) {
        return this.topicService.getNLatestRecords(topic, partition, n);
    }

    @GetMapping("/{topic}/partitions/{partition}/offsets/{offset}/records")
    public List<ConsumerRecordDto<Object, Object>> getNRecordsFromOffset(@PathVariable String topic,
                                                                         @PathVariable Integer partition,
                                                                         @PathVariable Long offset,
                                                                         @RequestParam Integer n) {
        return this.topicService.getNRecordsFromOffset(topic, partition, offset, n);
    }

}
