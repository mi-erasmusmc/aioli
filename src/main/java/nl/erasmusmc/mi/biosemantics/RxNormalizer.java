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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.SECONDS;

public class RxNormalizer {

    private int failureCounter = 0;
    private static final String RXNAV_URL = "https://rxnav.nlm.nih.gov/REST/rxcui.json?search=2&name=";
    private static final Logger log = LogManager.getLogger();
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.of(10, SECONDS))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    public Map<String, List<Integer>> callRxNav(List<String> drugs) {
        if (drugs.isEmpty()) {
            return Collections.emptyMap();
        }
        long start = System.currentTimeMillis();
        var fullList = new ArrayList<>(drugs);
        var size = fullList.size();
        log.info("Calling RxNormalizer for {} drugs will take approx {} mins", size, size / 600.0);
        var maps = ListUtils.partition(fullList, size / 3).parallelStream().map(this::callRxNavSplit).collect(Collectors.toList());
        long diff = System.currentTimeMillis() - start;
        var results = Stream.concat(maps.get(0).entrySet().stream(), maps.get(1).entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info("matched {} out of {} drugnames in {} seconds", results.size(), size, diff / 1000);
        return results;
    }


    public Map<String, List<Integer>> callRxNavSplit(List<String> drugs) {
        int total = drugs.size();
        Map<String, List<Integer>> results = new HashMap<>(total / 7);
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
        } catch (Exception e) {
            // If for some reason RxNorm API is failing us we will just pause a couple of seconds and resume, for a max of 25 tims.
            log.warn(e.getMessage());
            log.warn("RxNav request failed for {}", request.uri());
            try {
                Thread.sleep(5000);
                failureCounter++;
                if (failureCounter > 25) {
                    log.error("RxNav has failed over 25 requests, killing the process");
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException ignored) {
                failureCounter++;
            }
            return Collections.emptyList();
        }
    }

    private List<String> parseBodyToIds(String body) {
        try {
            return mapper.readValue(body, RxNavResponse.class).getIdGroup().getRxnormId();
        } catch (JsonProcessingException e) {
            log.error("Failed to parse body: {} to ids", body);
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
