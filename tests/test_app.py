import time
from datetime import date

from fastapi.testclient import TestClient

import main
from main import app, classify_sentiment, normalize_stock_code, normalize_user_intent, parse_yahoo_posts, yahoo_symbol

client = TestClient(app)


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_stock_list_is_available():
    response = client.get("/stocks")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_collection_jobs_and_summaries_work(monkeypatch):
    monkeypatch.setattr(
        main,
        "fetch_yahoo_board_posts",
        lambda stock_code, start, end, should_stop=None, on_page=None: [{
            "text": "買い増ししたい",
            "author": "test-user",
            "posted_at": f"{start.year}/{start.month}/{start.day} 10:00",
            "sentiment": "bullish",
            "user_intent": "買いたい",
        }],
    )
    monkeypatch.setattr(
        main,
        "fetch_yahoo_price_history",
        lambda stock_code, start, end: [
            {"date": current.isoformat(), "close": 140.0}
            for current in [date.fromordinal(value) for value in range(start.toordinal(), end.toordinal() + 1)]
        ],
    )
    payload = {
        "stock_code": "6753",
        "start": "2026-09-01",
        "end": "2026-09-05",
    }
    response = client.post("/collections", json=payload)
    assert response.status_code == 202
    body = response.json()
    assert "job_id" in body
    assert body["status"] == "queued"

    job_id = body["job_id"]
    for _ in range(50):
        status_response = client.get(f"/collections/{job_id}")
        if status_response.json().get("status") == "completed":
            break
        time.sleep(0.1)

    final_status = client.get(f"/collections/{job_id}")
    assert final_status.status_code == 200
    assert final_status.json()["status"] == "completed"

    summaries = client.get("/summaries/6753", params={"start": "2026-09-01", "end": "2026-09-05"})
    assert summaries.status_code == 200
    assert len(summaries.json()) >= 1
    assert summaries.json()[0]["buy_intent_count"] == 1
    assert summaries.json()[0]["sell_intent_count"] == 0
    assert summaries.json()[0]["neutral_intent_count"] == 0

    analysis = client.get("/analysis/6753", params={"start": "2026-09-01", "end": "2026-09-05"})
    assert analysis.status_code == 200
    assert len(analysis.json()) >= 1


def test_normalize_stock_code_and_sentiment_rules():
    assert normalize_stock_code(" 6753 ") == "6753"
    assert normalize_stock_code("6753.T") == "6753"
    assert yahoo_symbol("6753") == "6753.T"
    assert normalize_stock_code("285a") == "285A"
    assert classify_sentiment("底打ち期待して買い増ししたい") == "bullish"
    assert classify_sentiment("ここは厳しい、売りたい") == "bearish"
    assert classify_sentiment("まあ普通かな") == "unknown"
    assert normalize_user_intent("強く買いたい") == "買いたい"
    assert normalize_user_intent("売りたい") == "売りたい"
    assert normalize_user_intent("中立") == "中立"
    assert normalize_user_intent(None) == "未設定"


def test_parse_yahoo_posts_handles_html():
    html = """
    <html><body>
      <div class="comment-body">買い増ししたい。底堅いと思う</div>
      <div class="comment-body">今日は売りが多い。警戒が必要</div>
      <div class="comment-body">普通の相場だよ</div>
    </body></html>
    """
    posts = parse_yahoo_posts(html)
    assert len(posts) == 3
    assert posts[0]["sentiment"] == "bullish"
    assert posts[1]["sentiment"] == "bearish"
    assert posts[2]["sentiment"] == "unknown"


def test_parse_yahoo_japan_forum_posts():
        html = """
        <article class="_BbsItem_156cs_10">
            <a class="_BbsItem__userName_156cs_35"><span>user123</span></a>
            <time class="_BbsItem__postDate_156cs_38">2026/9/22 10:30</time>
            <span class="_BbsItem__feelLabel_156cs_93">強く買いたい</span>
            <div class="_BbsItem__body_156cs_90"><p>買い増ししたい</p></div>
        </article>
        """
        posts = parse_yahoo_posts(html)
        assert posts == [{
                "text": "買い増ししたい",
                "author": "user123",
                "posted_at": "2026/9/22 10:30",
                "sentiment": "bullish",
                "user_intent": "買いたい",
        }]
