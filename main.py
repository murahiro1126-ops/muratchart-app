from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
from datetime import date, datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import httpx
import sqlalchemy as sa
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("APP_DATABASE_PATH", BASE_DIR / "data" / "app.db"))
SENTIMENT_WORDS_PATH = Path(
    os.getenv("SENTIMENT_WORDS_PATH", BASE_DIR / "sentiment_words.txt")
)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
DATABASE_URL = f"sqlite:///{DB_PATH}"


class Base(DeclarativeBase):
    pass


class StockSummary(Base):
    __tablename__ = "stock_summaries"

    id: Mapped[int] = mapped_column(primary_key=True)
    stock_code: Mapped[str] = mapped_column(sa.String(16), index=True)
    summary_date: Mapped[date] = mapped_column(sa.Date, index=True)
    close_price: Mapped[float | None] = mapped_column(sa.Float, nullable=True)
    post_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    author_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    bullish_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    bearish_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    unknown_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    bullish_ratio: Mapped[float] = mapped_column(sa.Float, default=0.0)
    bearish_ratio: Mapped[float] = mapped_column(sa.Float, default=0.0)
    unknown_ratio: Mapped[float] = mapped_column(sa.Float, default=0.0)
    sma20: Mapped[float | None] = mapped_column(sa.Float, nullable=True)
    sma60: Mapped[float | None] = mapped_column(sa.Float, nullable=True)
    sma120: Mapped[float | None] = mapped_column(sa.Float, nullable=True)
    sma200: Mapped[float | None] = mapped_column(sa.Float, nullable=True)
    rsi14: Mapped[float | None] = mapped_column(sa.Float, nullable=True)
    atr14: Mapped[float | None] = mapped_column(sa.Float, nullable=True)
    bottom_phase: Mapped[str | None] = mapped_column(sa.String(32), nullable=True)
    trend_status: Mapped[str] = mapped_column(sa.String(32), default="未判定")
    bottom_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    bottom_signal: Mapped[str | None] = mapped_column(sa.String(32), nullable=True)
    buy_intent_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    sell_intent_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    neutral_intent_count: Mapped[int] = mapped_column(sa.Integer, default=0)
    unset_intent_count: Mapped[int] = mapped_column(sa.Integer, default=0)

    __table_args__ = (
        sa.UniqueConstraint(
            "stock_code", "summary_date", name="uq_stock_code_summary_date"
        ),
    )


class BoardPost(Base):
    __tablename__ = "board_posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    stock_code: Mapped[str] = mapped_column(sa.String(16), index=True)
    author: Mapped[str] = mapped_column(sa.String(64), default="unknown")
    posted_at: Mapped[str | None] = mapped_column(sa.String(32), nullable=True)
    content: Mapped[str] = mapped_column(sa.Text)
    sentiment: Mapped[str] = mapped_column(sa.String(16), default="unknown")
    user_intent: Mapped[str] = mapped_column(sa.String(16), default="未設定")
    created_at: Mapped[datetime] = mapped_column(sa.DateTime, default=datetime.utcnow)


class CollectionSchedule(Base):
    __tablename__ = "collection_schedules"

    stock_code: Mapped[str] = mapped_column(sa.String(16), primary_key=True)
    enabled: Mapped[bool] = mapped_column(sa.Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(sa.DateTime, default=datetime.utcnow)


engine = sa.create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

app = FastAPI(title="Stock Board Trend Analysis")
jobs: dict[str, dict[str, Any]] = {}
schedules: dict[str, bool] = {}
job_cancel_events: dict[str, threading.Event] = {}
YAHOO_PAGE_SIZE = int(os.getenv("YAHOO_PAGE_SIZE", "20"))
YAHOO_SCROLL_WAIT_SECONDS = float(os.getenv("YAHOO_SCROLL_WAIT_SECONDS", "1.2"))
YAHOO_MAX_SCROLLS = int(os.getenv("YAHOO_MAX_SCROLLS", "0"))


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    with engine.begin() as connection:
        columns = {
            column[1]
            for column in connection.exec_driver_sql("PRAGMA table_info(board_posts)")
        }
        if "user_intent" not in columns:
            connection.exec_driver_sql(
                "ALTER TABLE board_posts ADD COLUMN user_intent VARCHAR(16) NOT NULL DEFAULT '未設定'"
            )
        summary_columns = {
            column[1]
            for column in connection.exec_driver_sql(
                "PRAGMA table_info(stock_summaries)"
            )
        }
        for column_name in (
            "buy_intent_count",
            "sell_intent_count",
            "neutral_intent_count",
            "unset_intent_count",
        ):
            if column_name not in summary_columns:
                connection.exec_driver_sql(
                    f"ALTER TABLE stock_summaries ADD COLUMN {column_name} INTEGER NOT NULL DEFAULT 0"
                )
        if "neutral_count" in summary_columns:
            connection.exec_driver_sql(
                "ALTER TABLE stock_summaries DROP COLUMN neutral_count"
            )
        if "neutral_ratio" in summary_columns:
            connection.exec_driver_sql(
                "ALTER TABLE stock_summaries DROP COLUMN neutral_ratio"
            )
        summary_info = list(
            connection.exec_driver_sql("PRAGMA table_info(stock_summaries)")
        )
        close_price_info = next(
            column for column in summary_info if column[1] == "close_price"
        )
        if close_price_info[3] == 1:
            old_columns = [column[1] for column in summary_info]
            old_indexes = [
                row[1]
                for row in connection.exec_driver_sql(
                    "PRAGMA index_list(stock_summaries)"
                )
            ]
            for index_name in old_indexes:
                if not index_name.startswith("sqlite_autoindex_"):
                    connection.exec_driver_sql(f'DROP INDEX "{index_name}"')
            connection.exec_driver_sql(
                "ALTER TABLE stock_summaries RENAME TO stock_summaries_old"
            )
            Base.metadata.create_all(bind=connection)
            new_columns = [
                column[1]
                for column in connection.exec_driver_sql(
                    "PRAGMA table_info(stock_summaries)"
                )
            ]
            common_columns = [column for column in new_columns if column in old_columns]
            column_sql = ", ".join(common_columns)
            connection.exec_driver_sql(
                f"INSERT INTO stock_summaries ({column_sql}) SELECT {column_sql} FROM stock_summaries_old"
            )
            connection.exec_driver_sql("DROP TABLE stock_summaries_old")


def normalize_stock_code(value: str) -> str:
    normalized = value.strip().upper().replace(" ", "")
    return normalized.removesuffix(".T")


def yahoo_symbol(stock_code: str) -> str:
    return f"{normalize_stock_code(stock_code)}.T"


def load_sentiment_words() -> tuple[list[str], list[str]]:
    bullish_words: list[str] = []
    bearish_words: list[str] = []
    if SENTIMENT_WORDS_PATH.exists():
        for raw_line in SENTIMENT_WORDS_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, raw_words = line.split("=", 1)
            words = [word.strip() for word in raw_words.split(",") if word.strip()]
            if name.strip() == "BullishWord":
                bullish_words.extend(words)
            elif name.strip() == "BearishWord":
                bearish_words.extend(words)
    return list(dict.fromkeys(bullish_words)), list(dict.fromkeys(bearish_words))


def classify_sentiment(text: str) -> str:
    normalized = re.sub(
        r"[^\w\u3040-\u30ff\u4e00-\u9fffA-Za-z0-9\s]", "", unescape(text)
    ).lower()
    bullish_keywords, bearish_keywords = load_sentiment_words()

    bullish_score = sum(1 for keyword in bullish_keywords if keyword in normalized)
    bearish_score = sum(1 for keyword in bearish_keywords if keyword in normalized)

    if bullish_score > bearish_score:
        return "bullish"
    if bearish_score > bullish_score:
        return "bearish"
    return "unknown"


def normalize_user_intent(value: str | None) -> str:
    label = re.sub(r"\s+", "", unescape(value or ""))
    if label in {"強く買いたい", "買いたい"}:
        return "買いたい"
    if label in {"強く売りたい", "売りたい"}:
        return "売りたい"
    if label == "中立":
        return "中立"
    return "未設定"


def parse_yahoo_posts(html: str) -> list[dict[str, Any]]:
    content = unescape(html)
    yahoo_items = re.findall(
        r'<article class="[^"]*_BbsItem_[^"]*">(.*?)(?=<article class="[^"]*_BbsItem_[^"]*">|</section>|$)',
        content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if yahoo_items:
        posts: list[dict[str, Any]] = []
        for item in yahoo_items:
            author_match = re.search(
                r'<a class="[^"]*_BbsItem__userName_[^"]*"[^>]*>.*?<span>(.*?)</span>',
                item,
                flags=re.IGNORECASE | re.DOTALL,
            )
            posted_match = re.search(
                r'<time class="[^"]*_BbsItem__postDate_[^"]*"[^>]*>(.*?)</time>',
                item,
                flags=re.IGNORECASE | re.DOTALL,
            )
            body_match = re.search(
                r'<div class="[^"]*_BbsItem__body_[^"]*">(.*?)</div>',
                item,
                flags=re.IGNORECASE | re.DOTALL,
            )
            intent_match = re.search(
                r'<[^>]*class="[^"]*_BbsItem__feelLabel[^"]*"[^>]*>(.*?)</[^>]+>',
                item,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not body_match:
                continue
            text = re.sub(r"<[^>]+>", " ", body_match.group(1))
            text = re.sub(r"\s+", " ", unescape(text)).strip()
            if not text:
                continue
            posts.append(
                {
                    "text": text,
                    "author": re.sub(
                        r"<[^>]+>", " ", unescape(author_match.group(1))
                    ).strip()
                    if author_match
                    else "unknown",
                    "posted_at": re.sub(
                        r"\s+", " ", unescape(posted_match.group(1))
                    ).strip()
                    if posted_match
                    else None,
                    "sentiment": classify_sentiment(text),
                    "user_intent": normalize_user_intent(
                        intent_match.group(1) if intent_match else None
                    )
                    if intent_match
                    else "未設定",
                }
            )
        return posts

    candidates = re.findall(
        r"comment-body[^>]*>(.*?)</(?:div|p|li|span)>",
        content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not candidates:
        candidates = re.findall(r">\s*([^<>]{8,})\s*<", content, flags=re.DOTALL)

    posts: list[dict[str, Any]] = []
    for candidate in candidates:
        text = re.sub(r"<[^>]+>", " ", candidate)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        posts.append(
            {
                "text": text,
                "author": "unknown",
                "posted_at": None,
                "sentiment": classify_sentiment(text),
                "user_intent": "未設定",
            }
        )
    return posts


def fetch_yahoo_board_html(stock_code: str) -> str:
    normalized_code = normalize_stock_code(stock_code)
    url = f"https://finance.yahoo.co.jp/quote/{yahoo_symbol(normalized_code)}/forum"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
    }
    try:
        response = httpx.get(url, headers=headers, timeout=20.0)
        response.raise_for_status()
        return response.text
    except httpx.HTTPError:
        return ""


def parse_yahoo_api_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    for item in items:
        text = re.sub(r"<[^>]+>", " ", unescape(str(item.get("body", ""))))
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        posts.append(
            {
                "text": text,
                "author": str(item.get("dispname") or "unknown"),
                "posted_at": str(item.get("postDate") or ""),
                "sentiment": classify_sentiment(text),
                "user_intent": normalize_user_intent(item.get("feelLabel")),
            }
        )
    return posts


def fetch_yahoo_board_posts(
    stock_code: str,
    start: date,
    end: date,
    should_stop: Callable[[], bool] | None = None,
    on_page: Callable[[list[dict[str, Any]], date | None], None] | None = None,
) -> list[dict[str, Any]]:
    normalized_code = normalize_stock_code(stock_code)
    board_url = (
        f"https://finance.yahoo.co.jp/quote/{yahoo_symbol(normalized_code)}/forum"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/127.0.0.1 Safari/537.36",
        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
        "Referer": board_url,
    }
    posts: list[dict[str, Any]] = []
    try:
        with httpx.Client(headers=headers, timeout=20.0) as client:
            html_response = client.get(board_url)
            html_response.raise_for_status()
            html = html_response.text
            initial_posts = parse_yahoo_posts(html)
            posts.extend(initial_posts)
            if on_page:
                initial_oldest = min(
                    (post_date(post) for post in initial_posts if post_date(post)),
                    default=None,
                )
                on_page(initial_posts, initial_oldest)

            token_match = re.search(
                r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", html
            )
            comment_ids = re.findall(
                rf'href="/quote/{re.escape(yahoo_symbol(normalized_code))}/forum/(\d+)"',
                html,
            )
            if not token_match or not comment_ids:
                return [
                    post
                    for post in posts
                    if start <= (post_date(post) or date.min) <= end
                ]

            token = token_match.group(0)
            cursor = min(comment_ids, key=int)
            oldest_loaded = min(
                (post_date(post) for post in posts if post_date(post)), default=None
            )

            scroll_count = 0
            while True:
                if should_stop and should_stop():
                    break
                if oldest_loaded is not None and oldest_loaded <= start:
                    break
                if YAHOO_MAX_SCROLLS > 0 and scroll_count >= YAHOO_MAX_SCROLLS:
                    break
                response = client.get(
                    "https://finance.yahoo.co.jp/bff-quote-stocks/v1/ajax/bbs/comment",
                    params={
                        "code": normalized_code,
                        "size": YAHOO_PAGE_SIZE,
                        "mid": cursor,
                    },
                    headers={"x-jwt-token": token},
                )
                response.raise_for_status()
                payload = response.json()
                response_data = payload.get("response", {})
                items = response_data.get("items", [])
                page_posts = parse_yahoo_api_items(items)
                if not items:
                    break
                posts.extend(page_posts)
                page_oldest = min(
                    (post_date(post) for post in page_posts if post_date(post)),
                    default=None,
                )
                if on_page:
                    on_page(page_posts, page_oldest)
                next_cursor = str(items[-1].get("part") or "")
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
                scroll_count += 1
                if page_oldest is not None and (
                    oldest_loaded is None or page_oldest < oldest_loaded
                ):
                    oldest_loaded = page_oldest
                time.sleep(YAHOO_SCROLL_WAIT_SECONDS)
                if should_stop and should_stop():
                    break
    except (httpx.HTTPError, ValueError, KeyError):
        pass

    return [post for post in posts if start <= (post_date(post) or date.min) <= end]


def fetch_yahoo_price_history(
    stock_code: str, start: date, end: date
) -> list[dict[str, Any]]:
    symbol = yahoo_symbol(stock_code)
    start_timestamp = int(datetime.combine(start, datetime.min.time()).timestamp())
    end_timestamp = int(
        datetime.combine(end + timedelta(days=1), datetime.min.time()).timestamp()
    )
    query = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={start_timestamp}&period2={end_timestamp}&interval=1d"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = httpx.get(query, headers=headers, timeout=20.0)
        response.raise_for_status()
        payload = response.json()
        timestamps = (
            payload.get("chart", {}).get("result", [{}])[0].get("timestamp") or []
        )
        prices = (
            payload.get("chart", {})
            .get("result", [{}])[0]
            .get("indicators", {})
            .get("quote", [{}])[0]
            .get("close")
            or []
        )
        result: list[dict[str, Any]] = []
        for idx, ts in enumerate(timestamps):
            if idx >= len(prices):
                continue
            value = prices[idx]
            if value is None:
                continue
            current = datetime.utcfromtimestamp(ts).date()
            if start <= current <= end:
                result.append({"date": current.isoformat(), "close": value})
        if result:
            return result
    except Exception:
        pass

    return []


def persist_posts(stock_code: str, posts: list[dict[str, Any]]) -> None:
    if not posts:
        return
    with SessionLocal() as session:
        for post in posts:
            session.add(
                BoardPost(
                    stock_code=normalize_stock_code(stock_code),
                    author=str(post.get("author", "unknown")),
                    posted_at=str(post.get("posted_at") or ""),
                    content=str(post.get("text", "")),
                    sentiment=str(post.get("sentiment", "unknown")),
                    user_intent=normalize_user_intent(post.get("user_intent")),
                )
            )
        session.commit()


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def post_date(post: dict[str, Any]) -> date | None:
    posted_at = str(post.get("posted_at") or "")
    match = re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", posted_at)
    if not match:
        return None
    return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))


def build_summary_payload(
    stock_code: str,
    current_day: date,
    close_price: float | None,
    post_count: int,
    sentiment_counts: dict[str, int],
    intent_counts: dict[str, int],
) -> dict[str, Any]:
    total = max(1, post_count)
    bullish_count = sentiment_counts.get("bullish", 0)
    bearish_count = sentiment_counts.get("bearish", 0)
    unknown_count = max(0, post_count - bullish_count - bearish_count)
    return {
        "stock_code": stock_code,
        "summary_date": current_day,
        "close_price": round(close_price, 2) if close_price is not None else None,
        "post_count": post_count,
        "author_count": post_count // 2 if post_count else 0,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "unknown_count": unknown_count,
        "bullish_ratio": bullish_count / total,
        "bearish_ratio": bearish_count / total,
        "unknown_ratio": unknown_count / total,
        "sma20": None,
        "sma60": None,
        "sma120": None,
        "sma200": None,
        "rsi14": None,
        "atr14": None,
        "bottom_phase": None,
        "trend_status": "未判定",
        "bottom_count": 0,
        "bottom_signal": None,
        "buy_intent_count": intent_counts.get("買いたい", 0),
        "sell_intent_count": intent_counts.get("売りたい", 0),
        "neutral_intent_count": intent_counts.get("中立", 0),
        "unset_intent_count": intent_counts.get("未設定", 0),
    }


async def run_collection(job_id: str, stock_code: str, start: date, end: date) -> None:
    job = jobs[job_id]
    cancel_event = job_cancel_events[job_id]
    job["status"] = "running"

    def save_page(page_posts: list[dict[str, Any]], page_oldest: date | None) -> None:
        if page_posts:
            persist_posts(stock_code, page_posts)
        # 取得できた最古の日付があれば「YYYY/MM/DD までスクロール中...」と表示させる
        date_str = page_oldest.strftime("%Y/%m/%d") if page_oldest else "取得中"

        job["progress"] = {
            **job.get("progress", {}),
            "phase": f"{date_str} までスクロール中...",
            "pages_loaded": job.get("progress", {}).get("pages_loaded", 0) + 1,
            "saved_posts": job.get("progress", {}).get("saved_posts", 0)
            + len(page_posts),
            "loaded_posts": job.get("progress", {}).get("loaded_posts", 0)
            + len(page_posts),
            "oldest_posted_at": page_oldest.isoformat()
            if page_oldest
            else job.get("progress", {}).get("oldest_posted_at"),
            "target_start": start.isoformat(),
            "reached_target": page_oldest is not None and page_oldest <= start,
        }

    job["progress"] = {
        **job.get("progress", {}),
        "phase": "掲示板ページを読み込み中",
        "pages_loaded": 0,
        "saved_posts": 0,
        "loaded_posts": 0,
        "oldest_posted_at": None,
        "target_start": start.isoformat(),
        "reached_target": False,
    }
    posts = fetch_yahoo_board_posts(
        stock_code, start, end, should_stop=cancel_event.is_set, on_page=save_page
    )
    if cancel_event.is_set():
        job["status"] = "cancelled"
        return

    job["progress"]["phase"] = "株価を取得中"
    price_history = fetch_yahoo_price_history(stock_code, start, end)
    prices_by_date = {item["date"]: float(item["close"]) for item in price_history}
    saved = 0

    with SessionLocal() as session:
        for current_day in daterange(start, end):
            if cancel_event.is_set() or job.get("status") == "cancelled":
                job["status"] = "cancelled"
                return

            day_posts = [
                post
                for post in posts
                if post["text"]
                and post["sentiment"] in {"bullish", "bearish", "unknown"}
                and post_date(post) == current_day
            ]
            sentiment_counts = {"bullish": 0, "bearish": 0, "unknown": 0}
            intent_counts = {"買いたい": 0, "売りたい": 0, "中立": 0, "未設定": 0}
            for post in day_posts:
                sentiment_counts[post["sentiment"]] = (
                    sentiment_counts.get(post["sentiment"], 0) + 1
                )
                intent = normalize_user_intent(post.get("user_intent"))
                intent_counts[intent] = intent_counts.get(intent, 0) + 1
            close_price = prices_by_date.get(current_day.isoformat())
            payload = build_summary_payload(
                stock_code,
                current_day,
                close_price,
                len(day_posts),
                sentiment_counts,
                intent_counts,
            )
            existing_summary = (
                session.query(StockSummary)
                .filter(StockSummary.stock_code == stock_code)
                .filter(StockSummary.summary_date == current_day)
                .one_or_none()
            )
            if existing_summary is None:
                session.add(StockSummary(**payload))
            else:
                for field, value in payload.items():
                    setattr(existing_summary, field, value)
            saved += 1
            job["progress"] = {
                "phase": "日別サマリーを保存中",
                "processed_days": saved,
                "saved_posts": job["progress"].get("saved_posts", len(posts)),
                "loaded_posts": max(len(posts), saved),
                "pages_loaded": job["progress"].get("pages_loaded", 0),
                "oldest_posted_at": start.isoformat(),
                "target_start": start.isoformat(),
                "reached_target": current_day >= start,
            }
            session.commit()
            await asyncio.sleep(0.03)

    job["status"] = "completed"
    job["result"] = {
        "stock_code": stock_code,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "posts": len(posts),
        "days": saved,
        "status": "completed",
    }


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}


@app.get("/stocks")
def list_stocks() -> list[str]:
    with SessionLocal() as session:
        rows = (
            session.query(StockSummary.stock_code)
            .distinct()
            .order_by(StockSummary.stock_code.asc())
            .all()
        )
    return [row[0] for row in rows]


@app.get("/crawl-status/{stock_code}")
def get_crawl_status(stock_code: str) -> dict[str, Any]:
    normalized = normalize_stock_code(stock_code)
    latest = next(
        (
            job
            for job in jobs.values()
            if job.get("request", {}).get("stock_code") == normalized
        ),
        None,
    )
    if latest is None:
        return {"stock_code": normalized, "status": "idle"}
    return {"stock_code": normalized, **latest}


@app.get("/summary-periods/{stock_code}")
def get_summary_periods(stock_code: str) -> list[str]:
    with SessionLocal() as session:
        rows = (
            session.query(StockSummary.summary_date)
            .filter(StockSummary.stock_code == normalize_stock_code(stock_code))
            .order_by(StockSummary.summary_date.asc())
            .all()
        )
    return [row[0].isoformat() for row in rows]


class CollectionRequest(BaseModel):
    stock_code: str = "6753"
    start: date
    end: date


@app.post("/collections", status_code=202)
async def create_collection(request: CollectionRequest) -> dict[str, Any]:
    if request.start > request.end:
        raise HTTPException(status_code=400, detail="start must be before end")

    stock_code = normalize_stock_code(request.stock_code)
    if not stock_code:
        raise HTTPException(status_code=422, detail="stock_code is required")

    job_id = str(uuid4())
    jobs[job_id] = {
        "status": "queued",
        "request": {
            "stock_code": stock_code,
            "start": request.start.isoformat(),
            "end": request.end.isoformat(),
        },
        "progress": {
            "saved_posts": 0,
            "loaded_posts": 0,
            "oldest_posted_at": None,
            "target_start": request.start.isoformat(),
            "reached_target": False,
        },
    }
    job_cancel_events[job_id] = threading.Event()
    threading.Thread(
        target=lambda: asyncio.run(
            run_collection(job_id, stock_code, request.start, request.end)
        ),
        daemon=True,
    ).start()
    return {"job_id": job_id, "status": "queued"}


@app.get("/collections/{job_id}")
def get_collection(job_id: str) -> dict[str, Any]:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.post("/collections/{job_id}/cancel")
def cancel_collection(job_id: str) -> dict[str, Any]:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    job_cancel_events[job_id].set()
    job["status"] = "cancelling"
    return {"job_id": job_id, "status": "cancelling"}


@app.get("/summaries/{stock_code}")
def get_summaries(
    stock_code: str,
    start: date = Query(..., alias="start"),
    end: date = Query(..., alias="end"),
) -> list[dict[str, Any]]:
    if start > end:
        raise HTTPException(status_code=400, detail="start must be before end")

    normalized_code = normalize_stock_code(stock_code)
    with SessionLocal() as session:
        rows = (
            session.query(StockSummary)
            .filter(StockSummary.stock_code == normalized_code)
            .filter(StockSummary.summary_date >= start)
            .filter(StockSummary.summary_date <= end)
            .order_by(StockSummary.summary_date.asc())
            .all()
        )

    return [
        {
            "summary_date": row.summary_date.isoformat(),
            "close_price": row.close_price,
            "post_count": row.post_count,
            "author_count": row.author_count,
            "bullish_count": row.bullish_count,
            "bearish_count": row.bearish_count,
            "unknown_count": row.unknown_count,
            "bullish_ratio": row.bullish_ratio,
            "bearish_ratio": row.bearish_ratio,
            "unknown_ratio": row.unknown_ratio,
            "sma20": row.sma20,
            "sma60": row.sma60,
            "sma120": row.sma120,
            "sma200": row.sma200,
            "rsi14": row.rsi14,
            "atr14": row.atr14,
            "bottom_phase": row.bottom_phase,
            "trend_status": row.trend_status,
            "bottom_count": row.bottom_count,
            "bottom_signal": row.bottom_signal,
            "buy_intent_count": row.buy_intent_count,
            "sell_intent_count": row.sell_intent_count,
            "neutral_intent_count": row.neutral_intent_count,
            "unset_intent_count": row.unset_intent_count,
        }
        for row in rows
    ]


@app.get("/analysis/{stock_code}")
def get_analysis(
    stock_code: str,
    start: date = Query(..., alias="start"),
    end: date = Query(..., alias="end"),
) -> list[dict[str, Any]]:
    summaries = get_summaries(stock_code, start=start, end=end)
    if not summaries:
        return []

    last = summaries[-1]
    previous = summaries[-2] if len(summaries) > 1 else last
    middle = sum(item["bullish_ratio"] for item in summaries) / max(1, len(summaries))
    rise = (
        ((last["close_price"] - previous["close_price"]) / previous["close_price"])
        * 100
        if previous["close_price"]
        else 0.0
    )
    comment = (
        "強気が支配的で、価格は緩やかに上昇傾向です。"
        if last["bullish_ratio"] > middle and rise >= 0
        else "価格の伸びが弱く、投稿の慎重さが目立っています。"
    )
    return [
        {
            "stock_code": normalize_stock_code(stock_code),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "comment": comment,
            "latest_close": last["close_price"],
            "price_change_pct": round(rise, 2),
        }
    ]


@app.post("/schedules/{stock_code}")
def enable_schedule(stock_code: str) -> dict[str, Any]:
    normalized = normalize_stock_code(stock_code)
    schedules[normalized] = True
    with SessionLocal() as session:
        session.merge(CollectionSchedule(stock_code=normalized, enabled=True))
        session.commit()
    return {"stock_code": normalized, "enabled": True}


@app.get("/schedules/{stock_code}")
def get_schedule(stock_code: str) -> dict[str, Any]:
    normalized = normalize_stock_code(stock_code)
    enabled = schedules.get(normalized, False)
    return {"stock_code": normalized, "enabled": enabled}


@app.delete("/schedules/{stock_code}")
def disable_schedule(stock_code: str) -> dict[str, Any]:
    normalized = normalize_stock_code(stock_code)
    schedules[normalized] = False
    with SessionLocal() as session:
        existing = session.get(CollectionSchedule, normalized)
        if existing:
            existing.enabled = False
            session.commit()
    return {"stock_code": normalized, "enabled": False}


@app.get("/", response_class=HTMLResponse)
def root() -> str:
    return """
    <!doctype html>
    <html lang="ja">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>掲示板トレンド分析</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
                    :root { --ink: #17212b; --muted: #66727b; --line: #dce3e4; --teal: #168b84; --teal-soft: #e7f3f1; --paper: #fbfcfb; --orange: #d88443; }
                    * { box-sizing: border-box; }
                    body { margin: 0; background: #eef3f2; color: var(--ink); font-family: "Yu Mincho", "Hiragino Mincho ProN", serif; }
                    .page { width: min(1180px, calc(100% - 48px)); margin: 0 auto; padding: 44px 0 84px; }
                    .eyebrow { margin: 0 0 8px; color: var(--teal); font: 700 12px/1.2 Arial, sans-serif; letter-spacing: .2em; }
                    h1 { margin: 0; font-size: clamp(42px, 6vw, 72px); line-height: 1.1; letter-spacing: .04em; font-weight: 600; }
                    .top-rule { height: 1px; margin: 46px 0 54px; background: var(--line); }
                    .section { margin: 0 0 34px; padding: 48px 60px 54px; background: var(--paper); border: 1px solid #e0e6e5; box-shadow: 0 12px 30px rgba(36, 63, 62, .05); }
                    .section-head { display: flex; align-items: end; justify-content: space-between; gap: 24px; margin-bottom: 34px; }
                    .section-number { margin: 0 0 14px; color: #956f53; font: 700 13px/1 Arial, sans-serif; letter-spacing: .12em; }
                    h2 { margin: 0; font-size: clamp(30px, 4vw, 46px); font-weight: 600; line-height: 1.2; }
                    .section-note { max-width: 360px; margin: 0; color: var(--muted); font-size: 15px; line-height: 1.8; }
                    .fields { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }
                    .field { display: grid; gap: 10px; min-height: 100px; padding: 18px 22px; border: 1px solid var(--line); background: #fff; }
                    .field label { color: var(--muted); font-size: 13px; }
                    input, select { width: 100%; border: 0; outline: 0; background: transparent; color: var(--ink); font: 600 24px/1.2 "Yu Mincho", serif; }
                    input[type="date"] { font-size: 20px; }
                    .schedule-row { display: flex; align-items: center; justify-content: space-between; gap: 24px; margin-top: 30px; padding: 24px 0; border-top: 1px solid var(--line); border-bottom: 1px solid var(--line); }
                    .schedule-row strong { display: block; margin-bottom: 7px; font-size: 17px; }
                    .schedule-row span { color: var(--muted); font-size: 13px; }
                    .switch { position: relative; display: inline-flex; align-items: center; gap: 10px; color: var(--teal); font-size: 13px; white-space: nowrap; }
                    .switch input { position: absolute; opacity: 0; width: 1px; }
                    .switch-mark { width: 42px; height: 24px; border-radius: 99px; background: #cbd5d4; transition: .2s; }
                    .switch-mark::after { display: block; width: 18px; height: 18px; margin: 3px; border-radius: 50%; background: #fff; content: ""; transition: .2s; }
                    .switch input:checked + .switch-mark { background: var(--teal); }
                    .switch input:checked + .switch-mark::after { transform: translateX(18px); }
                    .actions { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; margin-top: 30px; }
                    button { min-height: 44px; padding: 0 22px; border: 1px solid var(--line); background: #fff; color: var(--ink); font: 600 14px/1 "Yu Mincho", serif; cursor: pointer; }
                    button:hover:not(:disabled) { border-color: var(--teal); color: var(--teal); }
                    button.primary { border-color: var(--teal); background: var(--teal); color: #fff; }
                    button.danger { border-color: #d5a49a; color: #a74736; }
                    button:disabled { cursor: not-allowed; opacity: .38; }
                    .arrow { margin-left: 10px; font-family: Arial, sans-serif; }
                    .progress { display: grid; grid-template-columns: 1fr auto; gap: 26px; align-items: center; margin-top: 30px; padding: 18px 0 0 24px; border-left: 4px solid var(--teal); }
                    .progress-label { color: var(--muted); font-size: 13px; }
                    .progress-value { margin-top: 6px; font-size: 22px; font-weight: 600; }
                    .progress-percent { color: var(--teal); font: 24px/1 Arial, sans-serif; }
                    .progress-meta { grid-column: 1 / -1; display: flex; gap: 26px; color: var(--muted); font: 12px/1.5 Arial, sans-serif; }
                    .chart-controls { display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 14px; align-items: end; }
                    .chart-controls .field { min-height: 78px; }
                    .checks { display: flex; flex-wrap: wrap; gap: 22px; margin: 20px 0 28px; color: var(--muted); font-size: 13px; }
                    .checks label { display: inline-flex; align-items: center; gap: 8px; }
                    .checks input { width: 16px; height: 16px; accent-color: var(--teal); }
                    .chart-box { min-height: 340px; margin-top: 22px; padding: 26px 22px 18px; border: 1px solid var(--line); background: #fff; }
                    .chart-title { margin: 0 0 16px; font-size: 19px; }
                    .chart-wrap { position: relative; height: 300px; }
                    .comment { display: grid; grid-template-columns: 170px 1fr; gap: 30px; align-items: center; min-height: 130px; border-left: 4px solid var(--teal); padding-left: 26px; }
                    .comment-status { font-size: 23px; font-weight: 600; }
                    .comment-confidence { margin-top: 8px; color: var(--muted); font: 12px/1.5 Arial, sans-serif; }
                    .comment-body { color: #334047; font-size: 17px; line-height: 2; }
                    .status-json { display: none; }
                    @media (max-width: 760px) {
                        .page { width: min(100% - 24px, 620px); padding-top: 28px; }
                        .section { padding: 30px 22px 34px; }
                        .section-head, .schedule-row { align-items: start; flex-direction: column; }
                        .fields, .chart-controls { grid-template-columns: 1fr; }
                        .progress { grid-template-columns: 1fr auto; }
                        .comment { grid-template-columns: 1fr; gap: 12px; }
                    }
        </style>
      </head>
      <body>
                <main class="page">
                    <header>
                        <p class="eyebrow">STOCK BOARD INTELLIGENCE</p>
                        <h1>掲示板トレンド分析</h1>
                    </header>
                    <div class="top-rule"></div>

                    <section class="section" aria-labelledby="collection-title">
                        <div class="section-head">
                            <div><p class="section-number">01</p><h2 id="collection-title">集計設定</h2></div>
                            <p class="section-note">銘柄と期間を指定して、掲示板データを収集します。</p>
                        </div>
                        <form id="collection-form">
                            <div class="fields">
                                <div class="field"><label for="stock-code">銘柄コード　例 6753</label><input id="stock-code" name="stock_code" value="6753" /></div>
                                <div class="field"><label for="collection-start">集計開始日</label><input id="collection-start" name="start" type="date" value="2026-09-01" /></div>
                                <div class="field"><label for="collection-end">集計終了日</label><input id="collection-end" name="end" type="date" value="2026-09-18" /></div>
                            </div>
                            <div class="schedule-row">
                                <div><strong>定期集計モード</strong><span>毎日、前日分を自動収集</span></div>
                                <label class="switch"><input id="schedule-toggle" type="checkbox" /><span class="switch-mark"></span><span id="schedule-label">期間指定モード</span></label>
                            </div>
                            <div class="actions">
                                <button class="primary" type="submit">集計を実行 <span class="arrow">→</span></button>
                                <button id="cancel-button" class="danger" type="button" disabled>処理を中止</button>
                                <button id="schedule-button" type="button">定期集計を実行</button>
                                <button id="schedule-cancel-button" type="button" disabled>定期集計を中止</button>
                            </div>
                        </form>
                        <div class="progress">
                            <div><div class="progress-label">処理状況</div><div id="progress-status" class="progress-value">待機中</div></div>
                            <div class="text-right" style="text-align: right;">
                                <div id="progress-percent" class="progress-percent">0%</div>
                                <div id="progress-eta-container" style="font-size: 0.75rem; color: #94a3b8; margin-top: 2px; display: none;">
                                    残り推定: <span id="progress-eta" style="font-weight: 500; color: #475569;">計算中...</span>
                                </div>
                            </div>
                              <div class="progress-meta"><span>処理済み <b id="progress-days">0</b> / <span id="target-days">0</span>日</span><span>保存済み <b id="progress-posts">0</b>件</span><span>取得ページ <b id="progress-pages">0</b></span><span>取得済み <b id="progress-loaded">0</b>件</span><span>最古日 <b id="progress-oldest">--</b></span></div>
                        </div>
                        <pre id="status-json" class="status-json"></pre>
                    </section>

                    <section class="section" aria-labelledby="chart-title">
                        <div class="section-head">
                            <div><p class="section-number">02</p><h2 id="chart-title">グラフ表示設定</h2></div>
                            <p class="section-note">保存済みの日別データから、終値と掲示板の変化を重ねて表示します。</p>
                        </div>
                        <div class="chart-controls">
                            <div class="field"><label for="chart-stock">銘柄</label><select id="chart-stock"><option value="">読み込み中...</option></select></div>
                            <div class="field"><label for="chart-start">表示開始日</label><input id="chart-start" type="date" value="2026-09-01" /></div>
                            <div class="field"><label for="chart-end">表示終了日</label><input id="chart-end" type="date" value="2026-09-18" /></div>
                            <button id="refresh-chart" class="primary" type="button">グラフを更新 <span class="arrow">→</span></button>
                        </div>
                        <div class="checks"><label><input id="show-bottom" type="checkbox" checked />底値候補を表示</label><label><input id="show-reversal" type="checkbox" checked />反転確認を表示</label></div>
                        <div class="chart-box"><h3 class="chart-title">終値と投稿数 <span class="section-note">左軸：終値　右軸：投稿数</span></h3><div class="chart-wrap"><canvas id="price-post-chart"></canvas></div></div>
                        <div class="chart-box"><h3 class="chart-title">終値とセンチメント割合 <span class="section-note">左軸：終値　右軸：割合</span></h3><div class="chart-wrap"><canvas id="sentiment-chart"></canvas></div></div>
                    </section>

                    <section class="section" aria-labelledby="analysis-title">
                        <div class="section-head"><div><p class="section-number">03</p><h2 id="analysis-title">センチメント分析コメント</h2></div><button id="refresh-comment" type="button">コメントを更新 <span class="arrow">→</span></button></div>
                        <div class="comment"><div><div id="comment-status" class="comment-status">未分析</div><div class="comment-confidence">信頼度 --</div></div><div id="comment-body" class="comment-body">表示中のグラフデータをもとに分析します。</div></div>
                    </section>
                </main>

                <script>
                    const $ = (id) => document.getElementById(id);
                    let activeJobId = null;
                    let pricePostChart = null;
                    let sentimentChart = null;
                    let collectionStartTime = null; // 👈 開始時間を記録する変数を追加！
                    const palette = { ink: '#17212b', teal: '#168b84', orange: '#d88443', blue: '#4f7897', gray: '#bfc9c9' };
                    const chartOptions = { responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false }, plugins: { legend: { labels: { usePointStyle: true, font: { family: 'Yu Mincho' } } } }, scales: { x: { grid: { color: '#edf1f0' } }, y: { position: 'left', grid: { color: '#edf1f0' } }, y1: { position: 'right', grid: { drawOnChartArea: false }, min: 0 } } };

                function setProgress(status, progress = {}) {
                    const etaContainer = $('progress-eta-container');
                    const etaText = $('progress-eta');

                    // ★修正1: status が 'running' または '収集中' の時にタイマーを起動！
                    if ((status === 'running' || status === '収集中' || status === '処理中') && !collectionStartTime) {
                        collectionStartTime = Date.now();
                    }

                    if (status === 'completed' || status === 'cancelled' || status === '完了' || status === '中止') {
                        $('progress-status').textContent = status === 'completed' ? '完了' : status === 'cancelled' ? '中止' : status;
                        $('progress-percent').textContent = (status === 'completed' || status === '完了') ? '100%' : '0%';
                        if (etaContainer) etaContainer.style.display = 'none';
                        collectionStartTime = null; // リセット
                    } else {
                        const phaseText = progress.phase || status;
                        $('progress-status').textContent = phaseText;

                        const targetDays = parseInt($('target-days').textContent) || 1;
                        const processedDays = progress.processed_days || 0;

                        const isScrolling = phaseText.includes('スクロール中') || phaseText.includes('読み込み中');

                        if (isScrolling) {
                            let scrolledDays = 0;
                            if (progress.oldest_posted_at) {
                                // ★修正2: 時差ズレを防ぐため年月日を数値でパースして正確に日数計算！
                                const [oY, oM, oD] = progress.oldest_posted_at.split('-').map(Number);
                                const oldestDate = new Date(oY, oM - 1, oD);

                                const endVal = $('collection-end').value;
                                const [eY, eM, eD] = endVal.split('-').map(Number);
                                const endDate = new Date(eY, eM - 1, eD);

                                const diffTime = Math.max(0, endDate - oldestDate);
                                // 終了日当日も含むため +1 日
                                scrolledDays = Math.min(targetDays, Math.floor(diffTime / (1000 * 60 * 60 * 24)) + 1);
                            }

                            // スクロールフェーズは 0% ~ 85% の範囲で進捗を表示
                            const percent = Math.min(85, Math.round((scrolledDays / targetDays) * 85));
                            $('progress-percent').textContent = `${percent}%`;

                            // 残り時間を算出！
                            if (collectionStartTime && scrolledDays > 0) {
                                const elapsedSec = (Date.now() - collectionStartTime) / 1000;
                                const secPerDay = elapsedSec / scrolledDays;
                                const remainingDays = Math.max(0, targetDays - scrolledDays);
                                const remainingSec = Math.round(remainingDays * secPerDay);

                                if (remainingSec > 60) {
                                    const min = Math.floor(remainingSec / 60);
                                    const sec = remainingSec % 60;
                                    etaText.textContent = `約${min}分${sec}秒`;
                                } else if (remainingSec > 0) {
                                    etaText.textContent = `約${remainingSec}秒`;
                                } else {
                                    etaText.textContent = 'まもなく完了';
                                }
                                if (etaContainer) etaContainer.style.display = 'block';
                            } else {
                                if (etaContainer) etaContainer.style.display = 'block';
                                etaText.textContent = '計算中...';
                            }
                        } else if (phaseText.includes('保存中')) {
                            const savePercent = 85 + Math.round((processedDays / targetDays) * 14);
                            $('progress-percent').textContent = `${savePercent}%`;
                            if (etaContainer) etaContainer.style.display = 'none';
                        } else {
                            $('progress-percent').textContent = '0%';
                            if (etaContainer) etaContainer.style.display = 'none';
                        }
                    }

                    // メタ情報の更新
                    $('progress-days').textContent = progress.processed_days || 0;
                    $('progress-posts').textContent = progress.saved_posts || 0;
                    $('progress-oldest').textContent = progress.oldest_posted_at || '--';$('progress-pages').textContent = progress.pages_loaded || 0;
                    $('progress-loaded').textContent = progress.loaded_posts || 0;
                }

                    // ★追加: DBに存在する銘柄一覧（/stocks）を取得してプルダウンを生成する関数
                    async function updateStockDropdown() {
                        try {
                            const response = await fetch('/stocks');
                            const stocks = await response.json();
                            const select = $('chart-stock');
                            if (stocks && stocks.length > 0) {
                                select.innerHTML = stocks.map(code => `<option value="${code}">${code}</option>`).join('');
                            }
                        } catch (e) {
                            console.error('銘柄一覧の取得失敗:', e);
                        }
                    }

                    async function renderCharts() {
                        const stock = $('chart-stock').value;
                        const start = $('chart-start').value;
                        const end = $('chart-end').value;
                        const response = await fetch(`/summaries/${stock}?start=${start}&end=${end}`);
                        const rows = await response.json();
                        const labels = rows.map(row => row.summary_date);
                        const base = { tension: .25, pointRadius: 2, borderWidth: 2 };
                        if (pricePostChart) pricePostChart.destroy();
                        pricePostChart = new Chart($('price-post-chart'), { type: 'line', data: { labels, datasets: [
                            { ...base, label: '終値', data: rows.map(row => row.close_price), borderColor: palette.ink, backgroundColor: palette.ink, yAxisID: 'y', spanGaps: false },
                            { type: 'bar', label: '投稿数', data: rows.map(row => row.post_count), borderColor: palette.teal, backgroundColor: 'rgba(22,139,132,.45)', yAxisID: 'y1' }
                        ] }, options: chartOptions });
                        if (sentimentChart) sentimentChart.destroy();
                        sentimentChart = new Chart($('sentiment-chart'), { type: 'bar', data: { labels, datasets: [
                            { label: '強気', data: rows.map(row => row.bullish_ratio), backgroundColor: 'rgba(216,132,67,.72)', yAxisID: 'y1',order: 2,stack: 'sentiment' },
                            { label: '弱気', data: rows.map(row => row.bearish_ratio), backgroundColor: 'rgba(79,120,151,.72)', yAxisID: 'y1',order: 2,stack: 'sentiment' },
                            { label: '不明', data: rows.map(row => row.unknown_ratio), backgroundColor: 'rgba(191,201,201,.9)', yAxisID: 'y1',order: 2,stack: 'sentiment' },
                            { type: 'line', label: '終値', data: rows.map(row => row.close_price), borderColor: palette.ink, backgroundColor: palette.ink, yAxisID: 'y', tension: .25, pointRadius: 2,order: 1}
                        ] }, options: { ...chartOptions, scales: { ...chartOptions.scales, x: {...chartOptions.scales.x,stacked: true},y1: { ...chartOptions.scales.y1,stacked: true, max: 1, ticks: { callback: value => `${Math.round(value * 100)}%` } } } } });
                        await renderComment(stock, start, end);
                    }

                    async function renderComment(stock, start, end) {
                        const response = await fetch(`/analysis/${stock}?start=${start}&end=${end}`);
                        const data = await response.json();
                        if (!data.length) { $('comment-status').textContent = '未分析'; $('comment-body').textContent = '表示中のグラフデータをもとに分析します。'; return; }
                        $('comment-status').textContent = '分析済み';
                        $('comment-body').textContent = data[0].comment;
                    }

                    async function pollJob(jobId) {
                        activeJobId = jobId; $('cancel-button').disabled = false;
                        while (activeJobId === jobId) {
                            const response = await fetch(`/collections/${jobId}`);
                            const status = await response.json();
                            $('status-json').textContent = JSON.stringify(status, null, 2);
                            const total = Math.max(1, Math.round((new Date(status.request.end) - new Date(status.request.start)) / 86400000) + 1);
                            $('target-days').textContent = total;
                              setProgress(status.status === 'completed' ? '完了' : status.status === 'cancelled' ? '中止' : status.status === 'running' ? '収集中' : '待機中', status.progress);
                            if (status.status === 'completed') { await renderCharts(); break; }
                            if (status.status === 'cancelled') break;
                            await new Promise(resolve => setTimeout(resolve, 500));
                        }
                        activeJobId = null; $('cancel-button').disabled = true;
                    }

                    $('collection-form').addEventListener('submit', async (event) => {
                        event.preventDefault();
                        collectionStartTime = Date.now(); //
                        const response = await fetch('/collections', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(Object.fromEntries(new FormData(event.target).entries())) });
                        const data = await response.json();
                        if (response.ok) await pollJob(data.job_id); else setProgress('エラー', {});
                    });
                    $('cancel-button').addEventListener('click', async () => { if (activeJobId) await fetch(`/collections/${activeJobId}/cancel`, { method: 'POST' }); });
                    $('refresh-chart').addEventListener('click', renderCharts);
                    $('refresh-comment').addEventListener('click', () => renderComment($('chart-stock').value, $('chart-start').value, $('chart-end').value));
                    $('schedule-toggle').addEventListener('change', event => { $('schedule-label').textContent = event.target.checked ? '定期集計モード' : '期間指定モード'; });
                    $('schedule-button').addEventListener('click', async () => { await fetch(`/schedules/${$('stock-code').value}`, { method: 'POST' }); $('schedule-toggle').checked = true; $('schedule-label').textContent = '定期集計モード'; $('schedule-cancel-button').disabled = false; });
                    $('schedule-cancel-button').addEventListener('click', async () => { await fetch(`/schedules/${$('stock-code').value}`, { method: 'DELETE' }); $('schedule-toggle').checked = false; $('schedule-label').textContent = '期間指定モード'; $('schedule-cancel-button').disabled = true; });
                    (async () => {
                        await updateStockDropdown();
                        renderCharts();
                    })();
                </script>
      </body>
    </html>
    """


init_db()
