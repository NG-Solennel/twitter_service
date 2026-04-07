import sys
import json
from pyspark import SparkContext, SparkConf

VALID_LANGS = {"ar", "en", "fr", "in", "pt", "es", "tr", "ja"}

def get_id(obj, id_field="id", id_str_field="id_str"):
    val = obj.get(id_field)
    if val is not None:
        try:
            return str(int(float(str(val))))
        except Exception:
            pass
    val = obj.get(id_str_field)
    if val is not None and str(val).strip():
        try:
            return str(int(float(str(val))))
        except Exception:
            pass
    return None

def parse_tweet(line):
    try:
        t = json.loads(line.strip())
        if not isinstance(t, dict):
            return None
        tweet_id = get_id(t)
        if tweet_id is None:
            return None
        user = t.get("user")
        if not isinstance(user, dict):
            return None
        user_id = get_id(user)
        if user_id is None:
            return None
        if not t.get("created_at"):
            return None
        text = t.get("text")
        if not text or str(text).strip() == "":
            return None
        entities = t.get("entities")
        if not isinstance(entities, dict):
            return None
        hashtags = entities.get("hashtags")
        if not isinstance(hashtags, list) or len(hashtags) == 0:
            return None
        lang = t.get("lang", "")
        if lang not in VALID_LANGS:
            return None
        return (tweet_id, t)
    except Exception:
        return None

def lowercase_tag(tag):
    result = []
    for c in tag:
        cp = ord(c)
        if 65 <= cp <= 90:
            result.append(chr(cp + 32))
        else:
            result.append(c)
    return ''.join(result)

def get_user_info(t):
    user = t.get("user", {})
    user_id = get_id(user)
    if user_id is None:
        return None
    screen_name = user.get("screen_name") or ""
    description = user.get("description") or ""
    created_at = t.get("created_at", "")
    tweet_id = get_id(t) or "0"
    return (user_id, created_at, screen_name, description, tweet_id)

def extract_user_infos(t):
    infos = []
    info = get_user_info(t)
    if info:
        infos.append(info)
    rt = t.get("retweeted_status")
    if isinstance(rt, dict):
        rt_user = rt.get("user", {})
        rt_user_id = get_id(rt_user)
        if rt_user_id:
            screen_name = rt_user.get("screen_name") or ""
            description = rt_user.get("description") or ""
            created_at = t.get("created_at", "")
            tweet_id = get_id(t) or "0"
            infos.append((rt_user_id, created_at, screen_name, description, tweet_id))
    return infos

def get_contact_relationships(t):
    results = []
    sender_id = get_id(t.get("user", {}))
    if sender_id is None:
        return results

    tweet_id = get_id(t) or "0"
    text = t.get("text", "")
    created_at = t.get("created_at", "")

    hashtags = []
    entities = t.get("entities", {})
    if isinstance(entities, dict):
        for h in entities.get("hashtags", []):
            if isinstance(h, dict) and h.get("text"):
                hashtags.append(lowercase_tag(h["text"]))

    reply_to = t.get("in_reply_to_user_id")
    if reply_to is None:
        reply_to = t.get("in_reply_to_user_id_str")
    if reply_to is not None:
        try:
            contacted_id = str(int(float(str(reply_to))))
            results.append({
                "contacted_user_id": contacted_id,
                "sender_user_id": sender_id,
                "tweet_type": "reply",
                "tweet_id": tweet_id,
                "text": text,
                "created_at": created_at,
                "hashtags": hashtags
            })
        except Exception:
            pass

    rt = t.get("retweeted_status")
    if isinstance(rt, dict):
        rt_user = rt.get("user", {})
        rt_user_id = get_id(rt_user)
        if rt_user_id is not None:
            results.append({
                "contacted_user_id": rt_user_id,
                "sender_user_id": sender_id,
                "tweet_type": "retweet",
                "tweet_id": tweet_id,
                "text": text,
                "created_at": created_at,
                "hashtags": hashtags
            })

    return results

def keep_latest_user(a, b):
    cmp = (a[0] > b[0]) - (a[0] < b[0])
    if cmp > 0:
        return a
    elif cmp < 0:
        return b
    else:
        return a if int(a[3]) >= int(b[3]) else b

def keep_latest_tweet(a, b):
    # a, b = (created_at, tweet_id, text)
    cmp = (a[0] > b[0]) - (a[0] < b[0])
    if cmp > 0:
        return a
    elif cmp < 0:
        return b
    else:
        return a if int(a[1]) >= int(b[1]) else b

def escape_tsv(s):
    if s is None:
        return ""
    return str(s).replace("\t", " ").replace("\n", " ").replace("\r", " ")

def main():
    input_path             = sys.argv[1]
    output_path            = sys.argv[2]
    excluded_hashtags_path = sys.argv[3]

    conf = SparkConf().setAppName("TwitterETL")
    sc = SparkContext(conf=conf)

    # Load excluded hashtags
    excluded_raw = sc.textFile(excluded_hashtags_path).collect()
    excluded_hashtags = set(lowercase_tag(h.strip()) for h in excluded_raw if h.strip())
    excluded_bc = sc.broadcast(excluded_hashtags)

    # Parse + filter + deduplicate
    raw = sc.textFile(input_path)
    parsed = raw.map(parse_tweet).filter(lambda x: x is not None)
    deduped = parsed.reduceByKey(lambda a, b: a).values()
    deduped.cache()

    # Latest user info
    user_info_rdd = deduped.flatMap(extract_user_infos)
    latest_users = user_info_rdd \
        .map(lambda x: (x[0], (x[1], x[2], x[3], x[4]))) \
        .reduceByKey(keep_latest_user) \
        .map(lambda x: (x[0], x[1][1], x[1][2]))

    # Contact relationships
    contacts_rdd = deduped.flatMap(get_contact_relationships)
    contacts_rdd.cache()

    # Interaction counts
    interaction_counts = contacts_rdd \
        .map(lambda c: (
            (c["contacted_user_id"], c["sender_user_id"]),
            (1 if c["tweet_type"] == "reply" else 0,
             1 if c["tweet_type"] == "retweet" else 0)
        )) \
        .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

    # ── KEY FIX: Latest contact tweet per (contacted, sender, type) ─────────
    # We store BOTH:
    # 1. latest_tweet_text — for display
    # 2. all_tweet_texts   — concatenated, for keyword scoring
    
    # Latest tweet per pair+type
    latest_contact_tweet = contacts_rdd \
        .map(lambda c: (
            (c["contacted_user_id"], c["sender_user_id"], c["tweet_type"]),
            (c["created_at"], c["tweet_id"], c["text"])
        )) \
        .reduceByKey(keep_latest_tweet) \
        .map(lambda x: ((x[0][0], x[0][1], x[0][2]), x[1][2]))  # key -> latest_text

    # All tweet texts concatenated per pair+type (use \x01 as separator)
    all_contact_tweets = contacts_rdd \
        .map(lambda c: (
            (c["contacted_user_id"], c["sender_user_id"], c["tweet_type"]),
            escape_tsv(c["text"])
        )) \
        .reduceByKey(lambda a, b: a + "\x01" + b)  # concatenate all texts

    # Join latest + all texts
    contact_tweets_combined = latest_contact_tweet.join(all_contact_tweets)

    # User hashtag counts (only t.user, exclude popular hashtags)
    def extract_user_hashtags(t):
        user_id = get_id(t.get("user", {}))
        if user_id is None:
            return []
        entities = t.get("entities", {})
        tags = []
        for h in entities.get("hashtags", []):
            if isinstance(h, dict) and h.get("text"):
                tag = lowercase_tag(h["text"])
                tags.append((user_id, tag))
        return tags

    user_hashtags_rdd = deduped \
        .flatMap(extract_user_hashtags) \
        .filter(lambda x: x[1] not in excluded_bc.value) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0][0], x[0][1], x[1]))

    # Write outputs
    # users: user_id \t screen_name \t description
    latest_users \
        .map(lambda x: f"{x[0]}\t{escape_tsv(x[1])}\t{escape_tsv(x[2])}") \
        .saveAsTextFile(f"{output_path}/users")

    # contacts: contacted_user_id \t sender_user_id \t reply_count \t retweet_count
    interaction_counts \
        .map(lambda x: f"{x[0][0]}\t{x[0][1]}\t{x[1][0]}\t{x[1][1]}") \
        .saveAsTextFile(f"{output_path}/contacts")

    # contact_tweets: contacted_uid \t sender_uid \t type \t latest_text \t all_texts
    contact_tweets_combined \
        .map(lambda x: f"{x[0][0]}\t{x[0][1]}\t{x[0][2]}\t{escape_tsv(x[1][0])}\t{x[1][1]}") \
        .saveAsTextFile(f"{output_path}/contact_tweets")

    # user_hashtags: user_id \t hashtag \t count
    user_hashtags_rdd \
        .map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}") \
        .saveAsTextFile(f"{output_path}/user_hashtags")

    sc.stop()

if __name__ == "__main__":
    main()
