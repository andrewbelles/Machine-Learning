use clients::client::*;
use clients::base::*;

use anyhow::{Context, Result};
use dotenvy::dotenv;
use std::env;
use serde::{Deserialize, Serialize};
use rusqlite::params;
use chrono::{NaiveDate, Datelike};
use async_trait::async_trait;

// Noaa Weather API
pub struct WeatherClient {
    base:     ClientBackbone,
    base_url: String,
    api_key:  String
}

#[derive(Deserialize)]
pub struct Station {
    pub id:      String,
    pub mindate: String,
    pub maxdate: String 
}

#[derive(Deserialize)]
pub struct StationsResponse {
    pub results: Vec<Station>
}

#[derive(Deserialize, Debug, Serialize)]
pub struct NormalRecord {
    datatype: String,
    date:     String,
    value:    f32
}

#[derive(Debug)]
pub struct Normal {
    month: u8, 
    tmax:  Option<f32>,
    tmin:  Option<f32>,
    prcp:  Option<f32>
}

#[derive(Deserialize)]
pub struct NormalsResponse {
    results: Vec<NormalRecord>
}

impl WeatherClient {
    pub fn new(db_path: &str) -> Result<Self> {
        dotenv().ok();
        // Get environment variables 
        let api_key   = env::var("NOAA_API_KEY")
            .context("Missing NOAA_API_KEY in environment")?;
        println!("key: {}", api_key);

        let base_url  = env::var("NOAA_BASE_URL")
            .context("Missing NOAA_BASE_URL in environment")?;
        println!("base: {}", base_url);

        let base     = ClientBackbone::new(db_path)
            .context("Failed to initialize ClientBackbone")?;

        // Generate schema if required 

        base.conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS normals;
            CREATE TABLE IF NOT EXISTS normals (
                station TEXT,
                month   INTEGER,
                tmax    REAL, 
                tmin    REAL,
                prcp    REAL, 
                fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (station, month)
            );
            "#
        )?;

        // Metric -> max_tmax, min_tmin, sum_prcp
        base.conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS extremes;
            CREATE TABLE IF NOT EXISTS extremes (
                station       TEXT,
                state         TEXT,
                metric        TEXT,
                value         REAL,
                fetched_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(station, state, metric)
            );
            "#
        )?;

        Ok(Self { base, base_url, api_key })
    }
    // Get all stations that match specified criteria 
    async fn fetch_stations(
        &self,
        state: &str,
        dataset:   &str,
        limit: usize
    ) -> Result<Vec<Station>> {
        let info = STATE_INFO
            .get(state)
            .ok_or_else(|| anyhow::anyhow!("Unknown state code: {}", state))?;

        let url = format!("{}/stations", self.base_url);
        let locationid = format!("FIPS:{}", info.fips);
        let query = [
            ("datasetid", dataset),
            ("locationid", &locationid),
            ("datacategoryid", "TEMP"),
            ("sortfield", "datacoverage"),
            ("sortorder", "desc"),
            ("limit", &limit.to_string())
        ];

        let response: StationsResponse = self.base.execute_with_retry(|| {
            self.base.http 
                .get(&url)
                .query(&query)
                .header("token", &self.api_key)
        })
        .await
        .context("fetch_stations: all retries failed")?;

        Ok(response.results)
    }

    pub async fn fetch_normals(&mut self, station: &Station) -> Result<Vec<Normal>> 
    {
        let (startdate, enddate) = (station.mindate.clone(), station.maxdate.clone());
        let station: &str = &station.id;

        let url = format!("{}/data", self.base_url);
        let query = [
            ("datasetid",  "NORMAL_MLY"),
            ("stationid",  station),
            ("datatypeid", "MLY-TMIN-NORMAL,MLY-TMAX-NORMAL,MLY-PRCP-NORMAL"),
            ("startdate",  &startdate),
            ("enddate",    &enddate),
            ("limit",      "1000"),
            ("units", "metric")
        ];

        // Get specialized response from api 
        let response: NormalsResponse = self.base.execute_with_retry(|| {
            self.base.http
                .get(&url)
                .query(&query)
                .header("token", &self.api_key)
            })
            .await
            .context("fetch_normals: all retries failed")?;

        // Get temporary maps for each data point 
        let mut tmin_map = std::collections::BTreeMap::new();
        let mut tmax_map = std::collections::BTreeMap::new();
        let mut prcp_map = std::collections::BTreeMap::new();

        for record in response.results {
            let date = NaiveDate::parse_from_str(&record.date, "%Y-%m-%dT%H:%M:%S")
                .context("fetch_normals: invalid date format")?;
            let month = date.month() as u8;
            match record.datatype.as_str() {
                "MLY-TMIN-NORMAL" => { tmin_map.insert(month, record.value); }
                "MLY-TMAX-NORMAL" => { tmax_map.insert(month, record.value); }
                "MLY-PRCP-NORMAL" => { prcp_map.insert(month, record.value / 10.0); }
                _ => {}
            }
        }

        let mut normals = Vec::with_capacity(12);
        for m in 1u8..=12 {
            normals.push(Normal {
                month: m,
                tmax: tmax_map.get(&m).copied(),
                tmin: tmin_map.get(&m).copied(),
                prcp: prcp_map.get(&m).copied()
            })
        }

        let transaction = self.base.conn.transaction()
            .context("Failed to open transaction")?;

        for normal in &normals { 
            transaction.execute(
                "REPLACE INTO normals (station, month, tmax, tmin, prcp) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    station,
                    normal.month as i64,
                    normal.tmax,
                    normal.tmin,
                    normal.prcp
                ],
            )
            .context("Failed to write record")?;
        }
        transaction.commit()
            .context("Failed to commit...")?;

        Ok(normals)
    }
}

#[async_trait(?Send)]
impl Updater for WeatherClient {
    async fn update(&mut self, states: &'static [&'static str], limit: usize) -> Result<()> {
        for &state in states {
            let stations = self.fetch_stations(state, "NORMAL_MLY", limit)
                .await
                .context("Failed to look up stations")?;

            // For each station pulled get normals and keep track of extremes
            for station in &stations {
                let normals = self.fetch_normals(&station)
                    .await
                    .context("Failed to fetch normals")?;

                // Minmax normals for temp  
                let max_tmax = normals
                    .iter()
                    .filter_map(|n| n.tmax)
                    .fold(f32::MIN, f32::max);
                let min_tmin = normals
                    .iter()
                    .filter_map(|n| n.tmin)
                    .fold(f32::MAX, f32::min);

                // Get total precip. iter 
                let sum_prcp = normals 
                    .iter()
                    .filter_map(|n| n.prcp)
                    .sum::<f32>();

                let transaction = self.base.conn.transaction()
                    .context("Failed to open transaction for extremes")?;

                let metrics = [
                    ("max_tmax", max_tmax),
                    ("min_tmin", min_tmin),
                    ("sum_prcp", sum_prcp)
                ];

                for &(metric, value) in &metrics {
                    transaction.execute(
                        "REPLACE INTO extremes (station, state, metric, value) VALUES (?1, ?2, ?3, ?4)",
                        params![&station.id, &state, &metric, &value]
                    )
                    .context("Failed to write to extremes record")?;
                }

                transaction
                    .commit()
                    .context("Failed to commit extremes transaction")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_STATES: &[&str] = &["DE", "CA"];

    // Tests basic client interfaces 
    #[tokio::test]
    async fn test_noaa_basic() {
        let mut client   = WeatherClient::new(":memory:")
            .expect("Failed to initialize client");
        
        let stations = client.fetch_stations("DE", "NORMAL_MLY", 10)
            .await 
            .expect("Failed to fetch stations");

        assert!(!stations.is_empty(), "Expected some station");

        let normals  = client.fetch_normals(&stations[0])
            .await 
            .expect("Failed to fetch normals");
        assert!(!normals.is_empty(), "Expected non-empty normals");
    }
    // Comprehensive test of client 
    #[tokio::test]
    async fn test_noaa_comprehensive() -> Result<()> {
        let mut client = WeatherClient::new(":memory:")
            .expect("Failed to initialize client");

        let c: i64 = client.base.conn 
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM normals", [], |r| r.get(0))
            .expect("normals table should exist");
        assert_eq!(c, 0, "normals table should start empty");

        let c: i64 = client.base.conn 
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM extremes", [], |r| r.get(0))
            .expect("extremes table should exist");
        assert_eq!(c, 0, "extremes table should start empty");

        client.update(TEST_STATES, 10).await?;

        let metrics = ["max_tmax", "min_tmin", "sum_prcp"];
        for &state in TEST_STATES {
            for &metric in &metrics {
                let c: i64 = client.base.conn 
                    .query_row(
                        "SELECT COUNT(*) FROM extremes WHERE state = ?1 AND metric = ?2",
                    params![&state, &metric],
                    |r| r.get(0)
                )
                .expect("query failed");
                assert!(c > 0, "Expected at least one {} row for {}", metric, state);
            }
        }
        
        let max_tmax_val: f32 = client.base.conn 
            .query_row(
                "SELECT MAX(value) FROM extremes WHERE metric = 'max_tmax'",
                [], |r| r.get(0)
            )?;
        assert!(
            (-50.0..=60.0).contains(&max_tmax_val), 
            "max_tmax={} out of expected range", max_tmax_val
        );

        let sum_prcp_val: f32 = client.base.conn 
            .query_row(
                "SELECT MAX(value) FROM extremes WHERE metric = 'sum_prcp'",
                [], |r| r.get(0)
            )?;
        assert!(sum_prcp_val > 0.0, "sum_prcp={} should be greater than 0", sum_prcp_val);

        Ok(())
    }
}
