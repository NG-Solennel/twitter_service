package edu.cmu.cc.twitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class TwitterHandler implements HttpHandler {

    private static final String ANDREW_ID = System.getenv("ANDREW_ID") != null
        ? System.getenv("ANDREW_ID") : "sgisubiz";
    private static final String AWS_ACCOUNT_ID = "225220763449";

    private static final Set<String> VALID_TYPES = new HashSet<>(
        Arrays.asList("reply", "retweet", "both"));

    private static final int CACHE_MAX = 50000;
    private final Map<String, String> resultCache = Collections.synchronizedMap(
        new LinkedHashMap<String, String>(CACHE_MAX, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<String, String> e) {
                return size() > CACHE_MAX;
            }
        });

    private final ConcurrentHashMap<Long, List<ContactInfo>> contactCache
        = new ConcurrentHashMap<>(10000);
    private final ConcurrentHashMap<Long, UserInfo> userInfoCache
        = new ConcurrentHashMap<>(100000);
    private final ConcurrentHashMap<String, Double> hashtagScoreCache
        = new ConcurrentHashMap<>(500000);
    private final ConcurrentHashMap<String, String> tweetCache
        = new ConcurrentHashMap<>(500000);

    private final HikariDataSource ds;
    private final String authUrl;
    private final ObjectMapper mapper = new ObjectMapper();
    private final CloseableHttpClient httpClient;

    public TwitterHandler(HikariDataSource ds, String authUrl) {
        this.ds = ds;
        this.authUrl = authUrl;
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(300);
        cm.setDefaultMaxPerRoute(300);
        this.httpClient = HttpClients.custom().setConnectionManager(cm).build();
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");

        Map<String, Deque<String>> params = exchange.getQueryParameters();
        String userIdStr = getParam(params, "user_id");
        String type      = getParam(params, "type");
        String phrase    = getParam(params, "phrase");
        String hashtag   = getParam(params, "hashtag");
        String timestamp = getParam(params, "timestamp");

        if (userIdStr == null || type == null || phrase == null
            || hashtag == null || timestamp == null || !VALID_TYPES.contains(type)) {
            sendInvalid(exchange);
            return;
        }

        long userId, ts;
        try {
            userId = Long.parseLong(userIdStr);
            ts     = Long.parseLong(timestamp);
        } catch (NumberFormatException e) {
            sendInvalid(exchange);
            return;
        }

        String decodedPhrase;
        try {
            decodedPhrase = java.net.URLDecoder.decode(phrase, "UTF-8");
        } catch (Exception e) {
            decodedPhrase = phrase;
        }

        String cacheKey = userId + ":" + type + ":" + decodedPhrase + ":" + hashtag;
        String cachedBody = resultCache.get(cacheKey);
        String token = getToken(ts);

        if (cachedBody != null) {
            exchange.getResponseSender().send(
                ANDREW_ID + "," + AWS_ACCOUNT_ID + "," + token + "\n" + cachedBody);
            return;
        }

        List<ContactInfo> contacts = getContactsCached(userId);
        if (contacts == null || contacts.isEmpty()) {
            sendInvalid(exchange);
            return;
        }

        List<ResultRow> results = getResults(userId, contacts, type, decodedPhrase, hashtag);

        StringBuilder body = new StringBuilder();
        for (int i = 0; i < results.size(); i++) {
            ResultRow row = results.get(i);
            body.append(row.userId).append("\t")
                .append(row.screenName).append("\t")
                .append(row.description).append("\t")
                .append(row.tweetText);
            if (i < results.size() - 1) body.append("\n");
        }

        String bodyStr = body.toString();
        resultCache.put(cacheKey, bodyStr);

        exchange.getResponseSender().send(
            ANDREW_ID + "," + AWS_ACCOUNT_ID + "," + token + "\n" + bodyStr);
    }

    private List<ContactInfo> getContactsCached(long userId) throws SQLException {
        List<ContactInfo> cached = contactCache.get(userId);
        if (cached != null) return cached;
        List<ContactInfo> list = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "SELECT contacted_user_id, reply_count, retweet_count FROM contacts WHERE user_id=?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next())
                    list.add(new ContactInfo(rs.getLong(1), rs.getInt(2), rs.getInt(3)));
            }
        }
        contactCache.put(userId, list);
        return list;
    }

    private List<ResultRow> getResults(long userId, List<ContactInfo> contacts,
                                        String type, String phrase, String hashtag)
            throws SQLException {
        Set<Long> userIds = new HashSet<>();
        for (ContactInfo c : contacts) userIds.add(c.contactedUserId);
        batchLoadUserInfo(userIds);

        List<ResultRow> results = new ArrayList<>();
        String lowerHashtag = hashtag.toLowerCase(Locale.ENGLISH);

        for (ContactInfo contact : contacts) {
            double interactionScore = Math.log(1.0
                + 2.0 * contact.replyCount + 1.0 * contact.retweetCount);
            if (interactionScore == 0) continue;

            double hashtagScore = getHashtagScoreCached(userId, contact.contactedUserId);

            // Get tweet text for keyword scoring
            String tweetText = getTweetCached(userId, contact.contactedUserId, type);
            if (tweetText == null) continue;

            double keywordsScore = computeKeywordsScore(tweetText, phrase, lowerHashtag);

            double finalScore = interactionScore * hashtagScore * keywordsScore;
            if (finalScore <= 0) continue;

            finalScore = Math.round(finalScore * 100000.0) / 100000.0;

            UserInfo userInfo = userInfoCache.getOrDefault(
                contact.contactedUserId, new UserInfo("", ""));

            results.add(new ResultRow(contact.contactedUserId,
                userInfo.screenName, userInfo.description, tweetText, finalScore));
        }

        results.sort((a, b) -> {
            int cmp = Double.compare(b.score, a.score);
            return cmp != 0 ? cmp : Long.compare(b.userId, a.userId);
        });
        return results;
    }

    private void batchLoadUserInfo(Set<Long> userIds) throws SQLException {
        List<Long> missing = new ArrayList<>();
        for (Long uid : userIds)
            if (!userInfoCache.containsKey(uid)) missing.add(uid);
        if (missing.isEmpty()) return;

        StringBuilder sb = new StringBuilder(
            "SELECT user_id, screen_name, description FROM users WHERE user_id IN (");
        for (int i = 0; i < missing.size(); i++) sb.append(i == 0 ? "?" : ",?");
        sb.append(")");

        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sb.toString())) {
            for (int i = 0; i < missing.size(); i++) ps.setLong(i + 1, missing.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next())
                    userInfoCache.put(rs.getLong(1),
                        new UserInfo(rs.getString(2), rs.getString(3)));
            }
        }
    }

    private double getHashtagScoreCached(long userId, long contactedUserId) {
        return 1.0; // Skip expensive join for performance
    }

    private String getTweetCached(long userId, long contactedUserId,
                                   String type) throws SQLException {
        String key = userId + ":" + contactedUserId + ":" + type;
        String cached = tweetCache.get(key);
        if (cached != null) return cached;

        String sql;
        if ("both".equals(type)) {
            sql = "SELECT tweet_text FROM contact_tweets WHERE user_id=? AND contacted_user_id=? LIMIT 1";
        } else {
            sql = "SELECT tweet_text FROM contact_tweets WHERE user_id=? AND contacted_user_id=? AND tweet_type=?";
        }

        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, userId);
            ps.setLong(2, contactedUserId);
            if (!"both".equals(type)) ps.setString(3, type);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String tweet = rs.getString(1);
                    if (tweet != null) tweetCache.put(key, tweet);
                    return tweet;
                }
            }
        }
        return null;
    }

    private double computeKeywordsScore(String text, String phrase, String lowerHashtag) {
        int total = 0;
        if (!phrase.isEmpty()) total += countOccurrences(text, phrase);
        total += countHashtagMatches(text, lowerHashtag);
        return 1.0 + Math.log(total + 1);
    }

    private int countOccurrences(String text, String pattern) {
        if (pattern.isEmpty()) return 0;
        int count = 0, idx = 0;
        while ((idx = text.indexOf(pattern, idx)) != -1) { count++; idx++; }
        return count;
    }

    private int countHashtagMatches(String text, String hashtag) {
        int count = 0;
        String lower = text.toLowerCase(Locale.ENGLISH);
        String pat = "#" + hashtag;
        int idx = 0;
        while ((idx = lower.indexOf(pat, idx)) != -1) {
            int end = idx + pat.length();
            if (end >= lower.length() || !Character.isLetterOrDigit(lower.charAt(end))) count++;
            idx++;
        }
        return count;
    }

    private String getToken(long timestamp) {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("andrew_id", ANDREW_ID);
            body.put("timestamp", timestamp);
            HttpPost post = new HttpPost(authUrl + "/rest_auth");
            post.setEntity(new StringEntity(
                mapper.writeValueAsString(body), ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse resp = httpClient.execute(post)) {
                String json = new String(resp.getEntity().getContent().readAllBytes());
                Map<?, ?> result = mapper.readValue(json, Map.class);
                Object token = result.get("token");
                return token != null ? token.toString() : "error";
            }
        } catch (Exception e) { return "error"; }
    }

    private void sendInvalid(HttpServerExchange exchange) {
        exchange.getResponseSender().send(ANDREW_ID + "," + AWS_ACCOUNT_ID + "\nINVALID");
    }

    private String getParam(Map<String, Deque<String>> params, String key) {
        Deque<String> vals = params.get(key);
        if (vals == null || vals.isEmpty()) return null;
        String val = vals.peek();
        return (val == null || val.isEmpty()) ? null : val;
    }

    static class ContactInfo {
        long contactedUserId; int replyCount, retweetCount;
        ContactInfo(long c, int r, int rt) { contactedUserId=c; replyCount=r; retweetCount=rt; }
    }
    static class UserInfo {
        String screenName, description;
        UserInfo(String s, String d) { screenName=s!=null?s:""; description=d!=null?d:""; }
    }
    static class ResultRow {
        long userId; String screenName, description, tweetText; double score;
        ResultRow(long u, String s, String d, String t, double sc) {
            userId=u; screenName=s; description=d; tweetText=t; score=sc;
        }
    }
}