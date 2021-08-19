package nl.erasmusmc.mi.biosemantics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RxNormalizer {

    private static final String RXNAV_URL = "https://rxnav.nlm.nih.gov/REST/rxcui.json?search=2&name=";
    private static final Logger log = LogManager.getLogger();
    private final HttpClient client = HttpClient.newBuilder()
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    public Map<String, List<Integer>> callRxNav(List<String> drugs) {
        if (drugs.isEmpty()) {
            return Collections.emptyMap();
        }
        long start = System.currentTimeMillis();
        var fullList = new ArrayList<>(drugs);
        var size = fullList.size();
        log.info("calling rxnormalizer for {} drugs will take approx {} mins", size, size / 600.0);
        var maps = ListUtils.partition(fullList, size / 3).parallelStream().map(this::callRxNavSplit).collect(Collectors.toList());
        long diff = System.currentTimeMillis() - start;
        var results = Stream.concat(maps.get(0).entrySet().stream(), maps.get(1).entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info("matched {} out of {} drugnames in {} seconds", results.size(), size, diff / 1000);
        return results;
    }


    public Map<String, List<Integer>> callRxNavSplit(List<String> drugs) {
        int total = drugs.size();
        Map<String, List<Integer>> results = new HashMap<>(total / 10);
        drugs.forEach(drug -> {
            List<String> ids = makeCall(drug);
            if (ids != null && !ids.isEmpty()) {
                List<Integer> idsInt = ids.stream().map(Integer::parseInt).collect(Collectors.toList());
                results.put(drug, idsInt);
            }
        });
        return results;
    }


    public List<String> makeCall(String drug) {
        String drugNameParam = URLEncoder.encode(drug, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(RXNAV_URL + drugNameParam))
                .GET()
                .build();
        try {
            return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApply(HttpResponse::body)
                    .thenApply(this::parseBodyToIds)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> parseBodyToIds(String body) {
        try {
            return mapper.readValue(body, RxNavResponse.class).getIdGroup().getRxnormId();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}

@Data
class RxNavResponse {
    IdGroup idGroup;
}

@Data
class IdGroup {
    String name;
    List<String> rxnormId;
}
