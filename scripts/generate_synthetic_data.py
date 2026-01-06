#!/usr/bin/env python3
"""
Synthetic Data Generator for Postgres vs Elasticsearch Benchmarks
Generates documents using English dictionary words to construct sentences
"""

import sys
import json
import random
import string
import os
import urllib.request
import gzip
import tempfile
import multiprocessing
import uuid
import argparse

def download_english_words():
    """Download a comprehensive English word list"""
    # Use a reliable source for English words
    word_urls = [
        "https://raw.githubusercontent.com/dwyl/english-words/master/words_dictionary.json",
        "https://www.mit.edu/~ecprice/wordlist.10000",  # Fallback
    ]

    words = []

    for url in word_urls:
        try:
            print(f"Downloading word list from {url}...", file=sys.stderr)
            if url.endswith('.json'):
                with urllib.request.urlopen(url) as response:
                    data = json.loads(response.read().decode('utf-8'))
                    words = list(data.keys())
            else:
                with urllib.request.urlopen(url) as response:
                    content = response.read().decode('utf-8')
                    words = [word.strip() for word in content.split('\n') if word.strip()]

            if len(words) > 1000:  # Ensure we have a good word list
                print(f"Downloaded {len(words)} words", file=sys.stderr)
                return words
        except Exception as e:
            print(f"Failed to download from {url}: {e}")
            continue

    # Fallback: generate basic words
    print("Using fallback word generation...", file=sys.stderr)
    words = []
    # Common English words
    common_words = [
        "the", "be", "to", "of", "and", "a", "in", "that", "have", "I", "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
        "this", "but", "his", "by", "from", "they", "we", "say", "her", "she", "or", "an", "will", "my", "one", "all", "would", "there", "their",
        "what", "so", "up", "out", "if", "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time", "no", "just", "him",
        "know", "take", "people", "into", "year", "your", "good", "some", "could", "them", "see", "other", "than", "then", "now", "look", "only",
        "come", "its", "over", "think", "also", "back", "after", "use", "two", "how", "our", "work", "first", "well", "way", "even", "new", "want",
        "because", "any", "these", "give", "day", "most", "us", "find", "here", "thing", "give", "many", "well", "only", "those", "tell", "one",
        "very", "her", "even", "back", "there", "should", "through", "before", "two", "too", "old", "think", "after", "work", "down", "still",
        "being", "our", "home", "where", "call", "world", "school", "why", "try", "ask", "need", "feel", "three", "when", "state", "never",
        "become", "between", "high", "really", "something", "most", "another", "much", "family", "own", "out", "leave", "put", "old", "while",
        "mean", "on", "keep", "student", "why", "let", "great", "same", "big", "group", "begin", "seem", "country", "help", "talk", "where",
        "turn", "problem", "every", "start", "hand", "might", "American", "show", "part", "about", "against", "place", "over", "such", "again",
        "few", "case", "most", "week", "company", "where", "system", "each", "right", "program", "hear", "so", "question", "during", "work",
        "play", "government", "run", "small", "number", "off", "always", "move", "like", "night", "live", "Mr", "point", "believe", "hold",
        "today", "bring", "happen", "next", "without", "before", "large", "all", "million", "must", "home", "under", "water", "room", "write",
        "mother", "area", "national", "money", "story", "young", "fact", "month", "different", "lot", "right", "study", "book", "eye", "job",
        "word", "though", "business", "issue", "side", "kind", "four", "head", "far", "black", "long", "both", "little", "house", "yes", "after",
        "since", "long", "provide", "service", "around", "friend", "important", "father", "sit", "away", "until", "power", "hour", "game", "often",
        "yet", "line", "political", "end", "among", "ever", "stand", "bad", "lose", "however", "member", "pay", "law", "meet", "car", "city",
        "almost", "include", "continue", "set", "later", "community", "much", "name", "five", "once", "white", "least", "president", "learn",
        "real", "change", "team", "minute", "best", "several", "idea", "kid", "body", "information", "nothing", "ago", "right", "lead", "social",
        "understand", "whether", "back", "watch", "together", "follow", "around", "parent", "only", "stop", "face", "anything", "create", "public",
        "already", "speak", "others", "read", "level", "allow", "add", "office", "spend", "door", "health", "person", "art", "sure", "such", "war",
        "history", "party", "within", "grow", "result", "open", "change", "morning", "walk", "reason", "low", "win", "research", "girl", "guy",
        "early", "food", "before", "moment", "himself", "air", "teacher", "force", "offer", "enough", "both", "education", "across", "although",
        "remember", "foot", "second", "boy", "maybe", "toward", "able", "age", "off", "policy", "everything", "love", "process", "music", "including",
        "consider", "appear", "actually", "buy", "probably", "human", "wait", "serve", "market", "die", "send", "expect", "home", "sense", "build",
        "stay", "fall", "oh", "nation", "plan", "cut", "college", "interest", "death", "course", "someone", "experience", "behind", "reach", "local",
        "kill", "six", "remain", "effect", "use", "yeah", "suggest", "class", "control", "raise", "care", "perhaps", "little", "late", "hard",
        "field", "else", "pass", "former", "sell", "major", "sometimes", "require", "along", "development", "themselves", "report", "role", "better",
        "economic", "effort", "up", "decide", "rate", "strong", "possible", "heart", "drug", "show", "leader", "light", "voice", "wife", "whole",
        "police", "mind", "finally", "pull", "return", "free", "military", "price", "report", "less", "according", "decision", "explain", "son",
        "hope", "even", "develop", "view", "relationship", "carry", "town", "road", "drive", "arm", "true", "federal", "break", "better", "difference",
        "thank", "receive", "value", "international", "building", "action", "full", "model", "join", "season", "society", "because", "tax", "director",
        "early", "position", "player", "agree", "especially", "record", "pick", "wear", "paper", "special", "space", "ground", "form", "support",
        "event", "official", "whose", "matter", "everyone", "center", "couple", "site", "end", "project", "hit", "base", "activity", "star", "table",
        "need", "court", "produce", "eat", "American", "teach", "oil", "half", "situation", "easy", "cost", "industry", "figure", "face", "street",
        "image", "itself", "phone", "either", "data", "cover", "quite", "picture", "clear", "practice", "piece", "land", "recent", "describe", "product",
        "doctor", "wall", "patient", "worker", "news", "test", "movie", "certain", "north", "love", "personal", "open", "support", "simply", "third",
        "technology", "catch", "step", "baby", "computer", "type", "attention", "draw", "film", "Republican", "tree", "source", "red", "nearly", "organization",
        "choose", "cause", "hair", "look", "point", "century", "evidence", "window", "difficult", "listen", "soon", "culture", "billion", "chance", "brother",
        "energy", "period", "course", "summer", "less", "realize", "hundred", "available", "plant", "likely", "opportunity", "term", "short", "letter",
        "condition", "choice", "place", "single", "rule", "daughter", "administration", "south", "husband", "Congress", "floor", "campaign", "material",
        "population", "well", "call", "economy", "medical", "hospital", "church", "close", "thousand", "risk", "current", "fire", "future", "wrong",
        "involve", "defense", "anyone", "increase", "security", "bank", "myself", "certainly", "west", "sport", "board", "seek", "per", "subject", "officer",
        "private", "rest", "behavior", "deal", "performance", "fight", "throw", "top", "quickly", "past", "goal", "second", "bed", "order", "author",
        "fill", "represent", "focus", "foreign", "drop", "plan", "blood", "upon", "agency", "push", "nature", "color", "no", "recently", "store", "reduce",
        "sound", "note", "fine", "before", "near", "movement", "page", "enter", "share", "than", "common", "poor", "other", "natural", "race", "concern",
        "series", "significant", "similar", "hot", "language", "each", "usually", "response", "dead", "rise", "animal", "factor", "decade", "article",
        "shoot", "east", "save", "seven", "artist", "away", "scene", "stock", "career", "despite", "central", "eight", "thus", "treatment", "beyond",
        "happy", "exactly", "protect", "approach", "lie", "size", "dog", "fund", "serious", "occur", "media", "ready", "sign", "thought", "list", "individual",
        "simple", "quality", "pressure", "accept", "answer", "hard", "resource", "identify", "left", "meeting", "determine", "prepare", "disease", "whatever",
        "success", "argue", "cup", "particularly", "amount", "ability", "staff", "recognize", "indicate", "character", "growth", "loss", "degree", "wonder",
        "attack", "herself", "region", "television", "box", "TV", "training", "pretty", "trade", "deal", "election", "everybody", "physical", "lay", "general",
        "feeling", "standard", "bill", "message", "fail", "outside", "arrive", "analysis", "benefit", "name", "sex", "forward", "lawyer", "present", "section",
        "environmental", "glass", "answer", "skill", "sister", "PM", "professor", "operation", "financial", "crime", "stage", "ok", "compare", "authority",
        "miss", "design", "sort", "one", "act", "ten", "knowledge", "gun", "station", "blue", "state", "strategy", "little", "clearly", "discuss", "indeed",
        "force", "truth", "song", "example", "democratic", "check", "environment", "leg", "dark", "public", "various", "rather", "laugh", "guess", "executive",
        "set", "study", "prove", "hang", "entire", "rock", "design", "enough", "forget", "since", "claim", "note", "remove", "manager", "help", "close",
        "sound", "enjoy", "network", "legal", "religious", "cold", "form", "final", "main", "science", "green", "memory", "card", "above", "seat", "cell",
        "establish", "nice", "trial", "expert", "that", "spring", "firm", "Democrat", "radio", "visit", "management", "care", "avoid", "imagine", "tonight",
        "huge", "ball", "no", "close", "finish", "yourself", "talk", "theory", "impact", "respond", "statement", "maintain", "charge", "popular", "traditional",
        "onto", "reveal", "direction", "weapon", "employee", "cultural", "contain", "peace", "head", "control", "base", "pain", "apply", "play", "measure",
        "wide", "shake", "fly", "interview", "manage", "chair", "fish", "particular", "camera", "structure", "politics", "perform", "bit", "weight", "suddenly",
        "discover", "candidate", "top", "production", "treat", "trip", "evening", "affect", "inside", "conference", "unit", "best", "style", "adult", "worry",
        "range", "mention", "rather", "far", "deep", "past", "edge", "individual", "specific", "writer", "trouble", "necessary", "throughout", "challenge",
        "fear", "shoulder", "institution", "middle", "sea", "dream", "bar", "beautiful", "property", "instead", "improve", "stuff", "detail", "method", "sign",
        "somebody", "magazine", "hotel", "soldier", "reflect", "heavy", "sexual", "cause", "bag", "heat", "fall", "marriage", "tough", "sing", "surface",
        "purpose", "exist", "pattern", "whom", "skin", "agent", "owner", "machine", "gas", "down", "ahead", "generation", "commercial", "address", "cancer",
        "test", "item", "reality", "coach", "step", "Mrs", "yard", "beat", "violence", "total", "tend", "investment", "discussion", "finger", "garden", "notice",
        "collection", "modern", "task", "partner", "positive", "civil", "kitchen", "consumer", "shot", "budget", "wish", "painting", "scientist", "safe", "agreement",
        "capital", "mouth", "nor", "victim", "newspaper", "instead", "threat", "responsibility", "smile", "attorney", "score", "account", "interesting", "break",
        "audience", "rich", "dinner", "figure", "vote", "western", "relate", "travel", "debate", "prevent", "citizen", "majority", "none", "front", "born",
        "admit", "senior", "assume", "wind", "key", "professional", "mission", "fast", "alone", "customer", "suffer", "speech", "successful", "option", "participant",
        "southern", "fresh", "eventually", "no", "forest", "video", "global", "Senate", "reform", "access", "restaurant", "judge", "publish", "cost", "relation",
        "like", "release", "own", "bird", "opinion", "credit", "critical", "corner", "concerned", "recall", "version", "stare", "safety", "effective", "neighborhood",
        "original", "troop", "income", "directly", "hurt", "species", "immediately", "track", "basic", "strike", "hope", "sky", "freedom", "absolutely", "plane",
        "nobody", "achieve", "object", "attitude", "labor", "refer", "concept", "client", "powerful", "perfect", "nine", "therefore", "conduct", "announce",
        "conversation", "examine", "touch", "please", "attend", "completely", "vote", "variety", "sleep", "turn", "involved", "investigation", "nuclear", "researcher",
        "press", "conflict", "spirit", "experience", "replace", "British", "encourage", "argument", "by", "once", "camp", "brain", "feature", "afternoon", "AM",
        "weekend", "dozen", "possibility", "along", "insurance", "department", "battle", "beginning", "date", "generally", "African", "very", "sorry", "crisis",
        "complete", "fan", "stick", "define", "easily", "through", "hole", "element", "vision", "status", "normal", "Chinese", "ship", "solution", "stone", "slowly",
        "scale", "bit", "university", "introduce", "driver", "attempt", "park", "spot", "lack", "ice", "boat", "drink", "sun", "front", "distance", "wood", "handle",
        "truck", "return", "mountain", "survey", "supposed", "tradition", "winter", "village", "Soviet", "refuse", "sales", "roll", "communication", "run", "screen",
        "gain", "resident", "hide", "gold", "club", "future", "farm", "potential", "increase", "middle", "European", "presence", "independent", "district", "shape",
        "reader", "Ms", "contract", "crowd", "Christian", "express", "apartment", "willing", "strength", "previous", "band", "obviously", "horse", "interested", "target",
        "prison", "ride", "guard", "terms", "demand", "reporter", "deliver", "text", "share", "tool", "wild", "vehicle", "observe", "flight", "inside", "facility",
        "understanding", "average", "emerge", "advantage", "quick", "light", "leadership", "earn", "pound", "basis", "bright", "operate", "guest", "sample", "contribute",
        "tiny", "block", "protection", "settle", "feed", "collect", "additional", "while", "highly", "identity", "title", "mostly", "lesson", "faith", "river", "promote",
        "living", "present", "count", "unless", "marry", "tomorrow", "technique", "path", "ear", "shop", "folk", "order", "principle", "survive", "lift", "border",
        "competition", "jump", "gather", "limit", "fit", "claim", "cry", "equipment", "worth", "associate", "critic", "warm", "aspect", "result", "insist", "failure",
        "annual", "French", "Christmas", "comment", "responsible", "affair", "approach", "until", "procedure", "regular", "spread", "chairman", "baseball", "soft",
        "ignore", "egg", "measure", "belief", "demonstrate", "anybody", "murder", "gift", "religion", "review", "editor", "past", "engage", "coffee", "document", "speed",
        "cross", "influence", "anyway", "threaten", "commit", "female", "youth", "wave", "move", "afraid", "quarter", "background", "native", "broad", "wonderful",
        "deny", "apparently", "slightly", "reaction", "twice", "suit", "perspective", "growing", "blow", "construction", "kind", "intelligence", "destroy", "cook",
        "connection", "charge", "burn", "shoe", "view", "grade", "context", "committee", "hey", "mistake", "focus", "smile", "location", "clothes", "Indian", "quiet",
        "dress", "promise", "aware", "neighbor", "complete", "drive", "function", "bone", "active", "extend", "chief", "average", "combine", "wine", "below", "cool",
        "voter", "mean", "demand", "learning", "bus", "hell", "dangerous", "remind", "moral", "United", "category", "relatively", "victory", "key", "academic", "visit",
        "Internet", "healthy", "fire", "negative", "following", "historical", "medicine", "tour", "depend", "photo", "finding", "grab", "direct", "classroom", "contact",
        "justice", "participate", "daily", "fair", "pair", "famous", "exercise", "knee", "flower", "tape", "hire", "familiar", "appropriate", "supply", "fully", "cut",
        "will", "actor", "birth", "search", "tie", "democracy", "Eastern", "primary", "yesterday", "circle", "device", "progress", "next", "front", "bottom", "island",
        "exchange", "clean", "studio", "train", "lady", "colleague", "application", "neck", "lean", "damage", "plastic", "tall", "plate", "hate", "otherwise", "writing",
        "press", "male", "start", "alive", "expression", "football", "intend", "attack", "chicken", "army", "abuse", "theater", "shutdown", "map", "extra", "session",
        "danger", "welcome", "domestic", "lots", "literature", "rain", "desire", "assessment", "injury", "respect", "northern", "nod", "paint", "fuel", "leaf", "dry",
        "Russian", "instruction", "fight", "pool", "climb", "sweet", "lead", "engine", "fourth", "salt", "expand", "importance", "metal", "fat", "ticket", "software",
        "disappear", "corporate", "strange", "lip", "reading", "urban", "mental", "increasingly", "lunch", "educational", "somewhere", "farmer", "above", "sugar", "planet",
        "favorite", "explore", "obtain", "enemy", "greatest", "complex", "surround", "athlete", "invite", "repeat", "carefully", "soul", "scientific", "impossible", "panel",
        "meaning", "mom", "married", "instrument", "predict", "weather", "presidential", "emotional", "commitment", "Supreme", "bear", "pocket", "thin", "temperature",
        "surprise", "poll", "proposal", "consequence", "half", "breath", "sight", "cover", "balance", "adopt", "minority", "straight", "attempt", "connect", "works",
        "teaching", "belong", "aid", "advice", "okay", "photography", "empty", "regional", "trail", "novel", "code", "somehow", "organize", "jury", "breast", "Iraqi",
        "human", "acknowledge", "theme", "storm", "union", "record", "desk", "fear", "thanks", "fruit", "under", "expensive", "yellow", "conclusion", "prime", "shadow",
        "struggle", "conclude", "analyst", "dance", "limit", "like", "regulatory", "being", "last", "ring", "largely", "shift", "revenue", "mark", "locate", "county",
        "appearance", "package", "difficulty", "bridge", "recommend", "obvious", "train", "basically", "e-mail", "generate", "anymore", "propose", "thinking", "possibly",
        "trend", "visitor", "loan", "currently", "comfortable", "investor", "but", "profit", "angry", "crew", "deep", "accident", "male", "meal", "hearing", "traffic",
        "muscle", "notion", "capture", "prefer", "truly", "earth", "Japanese", "chest", "search", "thick", "cash", "museum", "beauty", "emergency", "unique", "feature",
        "internal", "ethnic", "link", "stress", "content", "select", "root", "nose", "declare", "outside", "appreciate", "actual", "bottle", "hardly", "setting", "launch",
        "dress", "file", "sick", "outcome", "ad", "defend", "matter", "judge", "duty", "sheet", "ought", "ensure", "Catholic", "extremely", "extent", "component", "mix",
        "long-term", "slow", "contrast", "zone", "wake", "challenge", "airport", "chief", "brown", "standard", "shirt", "pilot", "warn", "ultimately", "cat", "contribution",
        "capacity", "ourselves", "estate", "guide", "circumstance", "snow", "English", "politician", "steal", "pursue", "slip", "percentage", "meat", "funny", "neither",
        "soil", "influence", "surgery", "correct", "Jewish", "blame", "estimate", "due", "basketball", "late", "golf", "investigate", "crazy", "significantly", "chain",
        "address", "branch", "combination", "just", "frequently", "governor", "relief", "user", "dad", "kick", "part", "manner", "ancient", "silence", "rating", "golden",
        "motion", "German", "gender", "solve", "fee", "landscape", "used", "bowl", "equal", "long", "official", "forth", "frame", "typical", "except", "conservative",
        "eliminate", "host", "hall", "trust", "ocean", "score", "row", "producer", "afford", "meanwhile", "regime", "division", "confirm", "fix", "appeal", "mirror",
        "tooth", "smart", "length", "entirely", "rely", "topic", "complain", "issue", "variable", "back", "range", "telephone", "perception", "attract", "confidence",
        "bedroom", "secret", "debt", "rare", "his", "tank", "nurse", "coverage", "opposition", "aside", "anywhere", "bond", "file", "pleasure", "master", "era", "require",
        "arrangement", "check", "stand", "grateful", "weapon", "painting", "bottom", "below", "liberal", "busy", "escape", "award", "therapy", "stimulus", "bag", "historically",
        "firm", "weather", "swim", "bet", "crash", "craft", "theoretical", "replace", "guy", "behind", "count", "earlier", "panel", "pine", "face", "faculty", "guest",
        "yours", "prize", "stream", "significance", "capture", "firm", "preference", "earn", "restaurant", "race", "adopt", "content", "confident", "fun", "gene", "extend",
        "alternative", "cross", "wake", "retire", "assumption", "shift", "somebody", "touch", "substantial", "head", "beyond", "channel", "extreme", "distance", "storage",
        "italics", "boot", "monitor", "minimum", "natural", "funeral", "round", "implication", "maximum", "vision", "actually", "phrase", "clerk", "scared", "tracker",
        "adjustment", "applicable", "practically", "consumption", "kick", "regard", "unlike", "telescope", "row", "stay", "permit", "afternoon", "doubt", "constituent",
        "naked", "plenty", "virtue", "competitor", "powder", "bare", "minimum", "so", "dinner", "mortgage", "tension", "influential", "beast", "fundamental", "lane",
        "hunter", "chess", "discipline", "instrument", "locate", "pack", "equity", "arm", "apartment", "bore", "engage", "eager", "mud", "alter", "consume", "kingdom",
        "sheet", "rage", "profession", "inquiry", "pile", "delay", "fantasy", "originate", "criticism", "automobile", "confession", "bet", "drag", "distinct", "verify",
        "adapt", "provoke", "dimension", "widespread", "utility", "loyal", "talent", "condemn", "delicate", "preach", "swallow", "vanish", "literacy", "export", "horrible",
        "electron", "laughter", "nest", "submission", "refusal", "logic", "agenda", "excitement", "bounce", "frighten", "trap", "sufficient", "endure", "radiation", "personnel",
        "substitute", "framework", "descend", "confront", "similarity", "dismiss", "rider", "revolutionary", "romance", "transmission", "developer", "realm", "hatred", "arise",
        "chalk", "spare", "poetry", "abortion", "absorption", "alert", "courtesy", "electronics", "transient", "cousin", "flash", "unity", "corruption", "compromise", "attribute",
        "compassion", "correlate", "electron", "embark", "enforce", "flavor", "fraction", "illusion", "inclusive", "inspire", "legitimate", "literature", "magnitude", "matrix",
        "medieval", "metaphor", "momentum", "monopoly", "narrative", "nominate", "obstacle", "optimum", "paradigm", "particle", "patron", "pharmacy", "pipeline", "platform",
        "plethora", "polarity", "portfolio", "precedent", "premium", "priority", "protocol", "pursuit", "quantum", "radical", "rationale", "recovery", "refugee", "regulate",
        "reliable", "renaissance", "replicate", "resonance", "restore", "retail", "revenue", "ritual", "satellite", "scenario", "spectrum", "spiral", "stability", "stimulate",
        "strategic", "suburban", "symphony", "synthesis", "tangible", "terminal", "texture", "theology", "therapy", "threshold", "timber", "tolerance", "turbine", "ultimate",
        "umbrella", "uniform", "universe", "upgrade", "utility", "validity", "venture", "vertical", "veteran", "vibrant", "virtual", "visible", "volunteer", "vulnerable",
        "warehouse", "warranty", "wealthy", "wholesale", "witness", "workshop", "worship", "zealous", "zodiac"
    ]
    words = list(set(common_words))  # Remove duplicates
    print(f"Using {len(words)} fallback words", file=sys.stderr)
    return words

def get_deterministic_uuid(int_id):
    """Generate a deterministic UUID from an integer ID"""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(int_id)))

def generate_sentence(words, min_words=5, max_words=20):
    """Generate a random sentence using dictionary words"""
    num_words = random.randint(min_words, max_words)
    sentence_words = random.choices(words, k=num_words)

    # Capitalize first word
    if sentence_words:
        sentence_words[0] = sentence_words[0].capitalize()

    # Add punctuation
    punctuation = random.choice(['.', '!', '?'])
    sentence = ' '.join(sentence_words) + punctuation

    return sentence

def generate_title(words):
    """Generate a random title (shorter sentence)"""
    return generate_sentence(words, min_words=2, max_words=8).rstrip('.!?')

def generate_document(doc_id, words):
    """Generate a single document with title and content"""
    title = generate_title(words)
    # Generate content as multiple sentences
    num_sentences = random.randint(3, 10)
    content_sentences = [generate_sentence(words) for _ in range(num_sentences)]
    content = ' '.join(content_sentences)

    return {
        'id': get_deterministic_uuid(doc_id),
        'title': title,
        'content': content
    }

def generate_child_document(parent_id_range):
    """Generate a child document with a reference to a parent"""
    parent_id = random.randint(1, parent_id_range)
    return {
        'id': str(uuid.uuid4()),
        'parent_id': get_deterministic_uuid(parent_id),
        'data': {
            'status': random.choice(['active', 'inactive', 'pending', 'archived']),
            'priority': random.choice(['high', 'medium', 'low']),
            'score': round(random.random() * 100, 2),
            'tags': random.sample(['urgent', 'review', 'legacy', 'new', 'flagged'], k=random.randint(1, 3)),
            'metadata': {
                'created_at': f"202{random.randint(0, 4)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
                'version': random.randint(1, 10)
            }
        }
    }

def generate_batch(args):
    """Generate a batch of documents in parallel"""
    start, end, words, seed_offset, mode, total_parents = args
    random.seed(42 + seed_offset)  # Different seed per process for reproducibility
    docs = []
    for i in range(start, end):
        if mode == 'child':
            doc = generate_child_document(total_parents)
        else:
            doc = generate_document(i + 1, words)
        docs.append(json.dumps(doc))
    return docs

def generate_dataset(scale, mode='parent', output_file=None, config_file=None):
    """Generate a complete dataset for the given scale"""
    # Load config
    if config_file is None:
        # Try multiple possible config file locations
        possible_configs = [
            '/config/benchmark_config.json',  # Container path
            'config/benchmark_config.json',   # Local path
            '../config/benchmark_config.json' # Relative path
        ]
        for cf in possible_configs:
            if os.path.exists(cf):
                config_file = cf
                break
        else:
            # Fallback: use hardcoded sizes
            size_map = {'small': 100, 'medium': 1000, 'large': 5000}
            expected_size = size_map.get(scale, 100)
            print(f"Using fallback size {expected_size} for {scale} scale")
    else:
        with open(config_file, 'r') as f:
            config = json.load(f)
        expected_size = config['data']['datasets'][scale]['size']
    
    if config_file and os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
        # Map scale to config field
        scale_size_map = {
            'small': 'small_scale',
            'medium': 'medium_scale',
            'large': 'large_scale'
        }
        expected_size = config['data'][scale_size_map[scale]]
    else:
        # Fallback sizes
        size_map = {'small': 100, 'medium': 1000, 'large': 5000}
        expected_size = size_map.get(scale, 100)
        print(f"Config file not found, using fallback size {expected_size} for {scale} scale", file=sys.stderr)

    print(f"Generating {expected_size} synthetic {mode} documents for {scale} scale...", file=sys.stderr)

    # Get English words (only needed for parent documents)
    words = []
    if mode == 'parent':
        words = download_english_words()

    # Generate documents in parallel batches
    batch_size = 10000
    num_processes = min(8, multiprocessing.cpu_count())
    
    with multiprocessing.Pool(processes=num_processes) as pool:
        tasks = []
        seed_offset = 0
        for start in range(0, expected_size, batch_size):
            end = min(start + batch_size, expected_size)
            tasks.append((start, end, words, seed_offset, mode, expected_size))
            seed_offset += 1
        
        for result in pool.imap(generate_batch, tasks):
            for doc_json in result:
                print(doc_json)
            print(f"Generated {len(result)} more documents...", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic data for benchmarks')
    parser.add_argument('scale', choices=['small', 'medium', 'large'], help='Data scale')
    parser.add_argument('--mode', choices=['parent', 'child'], default='parent', help='Type of documents to generate')
    
    args = parser.parse_args()

    # Set random seed for reproducible results
    random.seed(42)

    generate_dataset(args.scale, mode=args.mode)

if __name__ == "__main__":
    main()