use anyhow::{Context, Result};
use futures_util::StreamExt; // for sub.next().await
use reqwest::Client;
use std::env;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};

#[tokio::main]
async fn main() -> Result<()> {
    // -------- env --------
    let nats_url   = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let subject    = env::var("NATS_SUBJECT").unwrap_or_else(|_| "WALLET.updates".into());
    let ch_http    = env::var("CH_HTTP").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
    let ch_user    = env::var("CH_USER").unwrap_or_else(|_| "dev".into());
    let ch_pass    = env::var("CH_PASS").unwrap_or_else(|_| "dev".into());
    let ch_db      = env::var("CH_DB").unwrap_or_else(|_| "default".into());
    let ch_table   = env::var("CH_TABLE").unwrap_or_else(|_| "wallet_account_updates".into());
    let batch_size = env::var("BATCH_SIZE").ok().and_then(|s| s.parse().ok()).unwrap_or(200usize);
    let flush_ms   = env::var("FLUSH_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(500u64);

    println!(
        "Ingestor up. NATS={} subject={} → ClickHouse={}/{}.{} (batch={}, flush={}ms)",
        nats_url, subject, ch_http, ch_db, ch_table, batch_size, flush_ms
    );

    // -------- connections --------
    let nc = async_nats::connect(&nats_url).await
        .with_context(|| format!("connect NATS {nats_url}"))?;
    let mut sub = nc.subscribe(subject.clone()).await
        .with_context(|| format!("subscribe {subject}"))?;

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build reqwest client")?;

    let insert_url = format!(
        "{}/?query=INSERT%20INTO%20{}.{}%20FORMAT%20JSONEachRow",
        ch_http, ch_db, ch_table
    );

    // -------- batching --------
    let mut buf: Vec<String> = Vec::with_capacity(batch_size);
    let mut ticker = interval(Duration::from_millis(flush_ms));
    // if your code stalls, prevent “burst” catch-up; or keep default
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_msg = sub.next() => {
                if let Some(msg) = maybe_msg {
                    // payload is UTF-8 JSON from your plugin
                    match String::from_utf8(msg.payload.to_vec()) {
                        Ok(s) => {
                            // Optional: validate JSON is minimally correct
                            // serde_json::from_str::<serde_json::Value>(&s).ok();
                            buf.push(s);
                            if buf.len() >= batch_size {
                                flush(&client, &insert_url, &ch_user, &ch_pass, &mut buf).await?;
                            }
                        }
                        Err(e) => eprintln!("non-utf8 message from NATS: {e:?}"),
                    }
                } else {
                    // stream closed
                    eprintln!("NATS subscription closed; exiting.");
                    break;
                }
            }
            _ = ticker.tick() => {
                if !buf.is_empty() {
                    flush(&client, &insert_url, &ch_user, &ch_pass, &mut buf).await?;
                }
            }
        }
    }

    Ok(())
}

async fn flush(
    client: &reqwest::Client,
    insert_url: &str,
    ch_user: &str,
    ch_pass: &str,
    buf: &mut Vec<String>,
) -> Result<()> {
    // newline-delimited JSON for JSONEachRow
    let body = buf.join("\n") + "\n";

    let resp = client
        .post(insert_url)
        .basic_auth(ch_user, Some(ch_pass))
        .body(body)
        .send()
        .await
        .context("POST to ClickHouse")?;

    // IMPORTANT: Response::text() consumes self, so capture status first.
    let status = resp.status();
    if !status.is_success() {
        let txt = resp.text().await.unwrap_or_default();
        eprintln!("ClickHouse insert failed: {} :: {}", status, txt);
    }

    buf.clear();
    Ok(())
}
