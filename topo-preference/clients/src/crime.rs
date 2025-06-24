use clients::client::*;
use clients::base::*;

use anyhow::{Context, Result};
use anyhow::anyhow;
use dotenvy::dotenv;
use std::env;
use serde::{Deserialize, Serialize};
use rusqlite::params;
use async_trait::async_trait;
use chrono::NaiveDate;
use reqwest::Url;
use std::collections::HashMap;

pub struct CrimeClient {
    base:      ClientBackbone, 
    base_url:  String,
    api_key:   String
}

#[derive(Deserialize, Serialize, Debug)]
struct Agency {
    ori:         String,
    agency_name: String,
    state_abbr:  String,
    #[serde(rename="nibrs_start_date")]
    start_date:  String
}

#[derive(Deserialize, Serialize, Debug)]
struct RateMap {
    rates: HashMap<String, HashMap<String, Option<f32>>>    // Stores rates per 100K individuals 
}

#[derive(Deserialize)]
struct SummaryResponse {
    offenses: RateMap 
}

#[derive(Deserialize, Serialize, Debug)]
struct CrimeRecord {
    data_year: u16,
    ori:       String,
    offense:   String,
    rate:      f32      // per 100K   
}

static FROM:  &str = "01-2010";
static TO:    &str = "12-2023";
static OFFENSES: &[&str] = &[
    "V",            // violent crime 
    "BUR",          // burglary 
    "ASS",          // assault 
    "LAR",          // larceny 
    "MVT",          // motor vehicle theft 
    "HOM",          // homicide
    "RPE",          // rape 
    "ROB",          // robbery 
    "P"             // property crime 
];

impl CrimeClient {
    pub fn new(db_path: &str) -> Result<Self> {
        dotenv().ok();
        let base_url = env::var("GOV_URL")
            .context("must set GOV_URL")?;

        let api_key  = env::var("GOV_API_KEY")
            .context("must set GOV_API_KEY")?;

        let base = ClientBackbone::new(db_path)?;

        // reset and recreate main table 
        base.conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS crime_summary;
            CREATE TABLE IF NOT EXISTS crime_summary (
                data_year   INTEGER,
                state       TEXT, 
                ori         TEXT, 
                offense     TEXT,
                rate        FLOAT,
                PRIMARY KEY (data_year, ori, offense)
            );
            "#
        )?;

        // Create cache table if doesn't exist
        base.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS agency_cache (
                endpoint TEXT PRIMARY KEY,
                payload  BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS summary_cache (
                endpoint TEXT PRIMARY KEY, 
                payload  BLOB NOT NULL
            );
            "#
        )?;

        Ok(CrimeClient { base, base_url, api_key })
    }

    async fn fetch_agencies(&mut self, state: &str) -> Result<Vec<Agency>> {
        let url = format!(
            "{}/agency/byStateAbbr/{state}?API_KEY={key}",
            self.base_url,
            state=state,
            key=self.api_key
        );

        if let Some(cached) = self.base.load_cached("agency_cache", &url)? {
            return Ok(cached);
        }

        let county_map: HashMap<String, Vec<Agency>> = self.base.execute_with_retry(|| {
            self.base.http 
                .get(&url)
                .header("Accept", "application/json")
                .header("x-api-key", &self.api_key)
        })
        .await 
        .context("failed to fetch agencies")?;

        // Collect all agencies 
        let agencies: Vec<Agency> = county_map.into_iter()
            .flat_map(|(_, list)| list)
            .collect();

        self.base.save("agency_cache", &agencies)?;
        Ok(agencies)
    }

    async fn fetch_summary(&mut self, agency: &Agency, offense: &str) -> Result<Vec<CrimeRecord>> {
        let mut url = Url::parse(self.base_url.trim_end_matches('/'))
            .context("invalid base URL")?;

        url.path_segments_mut()
            .expect("base URL is not …/") 
            .extend(&["summarized", "agency", &agency.ori, offense]);

        url.query_pairs_mut()
            .append_pair("from", FROM)
            .append_pair("to", TO)
            .append_pair("API_KEY", &self.api_key);

        if let Some(cached) = self.base.load_cached("summary_cache", url.as_str())? {
            return Ok(cached);
        }

        let response: SummaryResponse = self.base.execute_with_retry(|| {
            self.base.http 
                .get(url.clone())
                .header("x-api-key", &self.api_key)
        })
        .await 
        .context("failed to fetch summary")?;

        let state_name = ABBR_TO_FULL_NAME
            .get(agency.state_abbr.as_str())
            .copied()
            .ok_or_else(|| anyhow!("Unknown state abbr {}", agency.state_abbr))?;

        let state_map = response.offenses 
            .rates
            .get(state_name)
            .ok_or_else(|| anyhow!("No rates for state {}", agency.state_abbr))?;

        let mut records = Vec::with_capacity(state_map.len());
        for (month_year, rate_option) in state_map {
            let rate = if let Some(r) = rate_option {
                *r
            } else {
                continue;
            };
            let year = month_year 
                .split('-')                          // split month, year
                .nth(1) 
                .and_then(|s| s.parse::<u16>().ok()) // get year
                .unwrap_or_default();

            records.push(CrimeRecord {
                data_year: year,
                ori:       agency.ori.clone(),
                offense:   offense.to_string(),
                rate
            })
        }

        self.base.save("summary_cache", &records)?;
        Ok(records)
    }
}

// States is expected to be the full names 
#[async_trait(?Send)]
impl Updater for CrimeClient {
    async fn update(&mut self, states: &'static [&'static str], limit: usize) -> Result<()> {
        for &state in states {
            let mut agencies = self.fetch_agencies(state).await?;
            agencies.sort_by_key(|a| {
                NaiveDate::parse_from_str(&a.start_date, "%Y-%m-%d")
                    .unwrap_or_else(|_| {
                        NaiveDate::from_ymd_opt(9999, 12, 31)
                            .expect("invalid date format")
                    })
            });

            for agency in agencies.into_iter().take(limit) {
                for &offense in OFFENSES {
                    let records = self.fetch_summary(&agency, offense).await?;
                    let transaction = self.base.conn.transaction()?;
                    for record in records {
                        transaction.execute(
                            "REPLACE INTO crime_summary (data_year, state, ori, offense, rate)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![
                                record.data_year as i64, 
                                agency.state_abbr,
                                record.ori,
                                record.offense, 
                                record.rate as f64
                            ],
                        )?;
                    }
                    transaction.commit()?;
                }
            }
        }
        Ok(())
    }    
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[tokio::test]
    async fn test_crime_basic() {
        let mut client: CrimeClient = CrimeClient::new(":memory:")
            .expect("Failed to create CrimeClient");
        {
            let mut statement = client.base.conn 
                .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='crime_summary';")
                .unwrap();
            let exists = statement.exists([]).unwrap();
            
            assert!(exists, "crime_summary table should exist");
        }

        let agencies = client.fetch_agencies("VA")
            .await 
            .expect("failed to fetch_agencies");
        assert!(!agencies.is_empty(), "Expected non-empty agencies result");

        let offense_page = client.fetch_summary(&agencies[0], "BUR")
            .await
            .expect("failed to fetch_offense_page");
        assert!(!offense_page.is_empty(), "Expected non-empty first page");
    }

    #[tokio::test]
    async fn test_noaa_comprehensive() -> Result<()> {
        let mut client = CrimeClient::new(":memory:")
            .expect("Failed to initialize client");

        client.update(&["VA"], 1)
            .await 
            .expect("Update failed");

        let mut statement = client.base.conn.prepare(
            "SELECT COUNT(*) FROM crime_summary"
        )?;
        let count: i64 = statement.query_row([], |row| row.get(0))?;
        assert!(count > 0, "Expected non-zero count in crime_summary");

        let mut statement = client.base.conn.prepare(
            "SELECT data_year, state, ori, offense, rate FROM crime_summary LIMIT 1"
        )?;
        let sample: (i64, String, String, String, f64) = statement.query_row([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?
            ))
        })?;
        assert!(sample.0 >= 2010 && sample.0 <= 2023, "Unexpected year");
        assert_eq!(sample.1, "VA");
        assert!(!sample.2.is_empty(), "Expected non-empty ori");
        assert!(!sample.3.is_empty(), "Expected non-empty offense");
        assert!(sample.4 >= 0.0, "Expected non negative rate");

        Ok(())
    } 
}
