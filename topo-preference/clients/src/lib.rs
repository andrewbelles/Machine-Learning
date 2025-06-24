pub mod base {

use phf::phf_map;
use async_trait::async_trait;
// use futures::stream::{self, StreamExt};
use anyhow::Result;

// Trait that all clients implement as a main interface to pull state data
#[async_trait(?Send)]
pub trait Updater {
    async fn update(&mut self, states: &'static [&'static str], limit: usize) -> Result<()>;
}
#[derive(Clone, Copy, Debug)]
pub struct StateInfo {
    pub lat: f32, 
    pub lon: f32, 
    pub radius: u32,
    pub fips: &'static str      // needed to keep str in static memory
}

/// Quick lookup table
pub static STATE_INFO: phf::Map<&'static str, StateInfo> = phf_map! {
    "AL" => StateInfo { lat: 32.806671, lon: -86.791130, radius: 300_000, fips: "01" },
    "AK" => StateInfo { lat: 61.370716, lon: -152.404419, radius: 800_000, fips: "02" },
    "AZ" => StateInfo { lat: 33.729759, lon: -111.431221, radius: 500_000, fips: "04" },
    "AR" => StateInfo { lat: 34.969704, lon: -92.373123,  radius: 300_000, fips: "05" },
    "CA" => StateInfo { lat: 36.116203, lon: -119.681564, radius: 600_000, fips: "06" },
    "CO" => StateInfo { lat: 39.059811, lon: -105.311104, radius: 400_000, fips: "08" },
    "CT" => StateInfo { lat: 41.597782, lon: -72.755371,  radius: 150_000, fips: "09" },
    "DE" => StateInfo { lat: 39.318523, lon: -75.507141,  radius: 100_000, fips: "10" },
    "FL" => StateInfo { lat: 27.766279, lon: -81.686783,  radius: 500_000, fips: "12" },
    "GA" => StateInfo { lat: 33.040619, lon: -83.643074,  radius: 400_000, fips: "13" },
    "HI" => StateInfo { lat: 21.094318, lon: -157.498337, radius: 200_000, fips: "15" },
    "ID" => StateInfo { lat: 44.240459, lon: -114.478828, radius: 400_000, fips: "16" },
    "IL" => StateInfo { lat: 40.349457, lon: -88.986137,  radius: 350_000, fips: "17" },
    "IN" => StateInfo { lat: 39.849426, lon: -86.258278,  radius: 300_000, fips: "18" },
    "IA" => StateInfo { lat: 42.011539, lon: -93.210526,  radius: 350_000, fips: "19" },
    "KS" => StateInfo { lat: 38.526600, lon: -96.726486,  radius: 400_000, fips: "20" },
    "KY" => StateInfo { lat: 37.668140, lon: -84.670067,  radius: 300_000, fips: "21" },
    "LA" => StateInfo { lat: 31.169546, lon: -91.867805,  radius: 300_000, fips: "22" },
    "ME" => StateInfo { lat: 44.693947, lon: -69.381927,  radius: 300_000, fips: "23" },
    "MD" => StateInfo { lat: 39.063946, lon: -76.802101,  radius: 150_000, fips: "24" },
    "MA" => StateInfo { lat: 42.230171, lon: -71.530106,  radius: 150_000, fips: "25" },
    "MI" => StateInfo { lat: 43.326618, lon: -84.536095,  radius: 400_000, fips: "26" },
    "MN" => StateInfo { lat: 45.694454, lon: -93.900192,  radius: 400_000, fips: "27" },
    "MS" => StateInfo { lat: 32.741646, lon: -89.678696,  radius: 300_000, fips: "28" },
    "MO" => StateInfo { lat: 38.456085, lon: -92.288368,  radius: 350_000, fips: "29" },
    "MT" => StateInfo { lat: 46.921925, lon: -110.454353, radius: 600_000, fips: "30" },
    "NE" => StateInfo { lat: 41.125370, lon: -98.268082,  radius: 400_000, fips: "31" },
    "NV" => StateInfo { lat: 38.313515, lon: -117.055374, radius: 500_000, fips: "32" },
    "NH" => StateInfo { lat: 43.452492, lon: -71.563896,  radius: 150_000, fips: "33" },
    "NJ" => StateInfo { lat: 40.298904, lon: -74.521011,  radius: 150_000, fips: "34" },
    "NM" => StateInfo { lat: 34.840515, lon: -106.248482, radius: 500_000, fips: "35" },
    "NY" => StateInfo { lat: 42.165726, lon: -74.948051,  radius: 400_000, fips: "36" },
    "NC" => StateInfo { lat: 35.630066, lon: -79.806419,  radius: 300_000, fips: "37" },
    "ND" => StateInfo { lat: 47.528912, lon: -99.784012,  radius: 400_000, fips: "38" },
    "OH" => StateInfo { lat: 40.388783, lon: -82.764915,  radius: 300_000, fips: "39" },
    "OK" => StateInfo { lat: 35.565342, lon: -96.928917,  radius: 400_000, fips: "40" },
    "OR" => StateInfo { lat: 44.572021, lon: -122.070938, radius: 500_000, fips: "41" },
    "PA" => StateInfo { lat: 40.590752, lon: -77.209755,  radius: 300_000, fips: "42" },
    "RI" => StateInfo { lat: 41.680893, lon: -71.511780,  radius: 100_000, fips: "44" },
    "SC" => StateInfo { lat: 33.856892, lon: -80.945007,  radius: 200_000, fips: "45" },
    "SD" => StateInfo { lat: 44.299782, lon: -99.438828,  radius: 400_000, fips: "46" },
    "TN" => StateInfo { lat: 35.747845, lon: -86.692345,  radius: 300_000, fips: "47" },
    "TX" => StateInfo { lat: 31.054487, lon: -97.563461,  radius: 600_000, fips: "48" },
    "UT" => StateInfo { lat: 40.150032, lon: -111.862434, radius: 400_000, fips: "49" },
    "VT" => StateInfo { lat: 44.045876, lon: -72.710686,  radius: 150_000, fips: "50" },
    "VA" => StateInfo { lat: 37.769337, lon: -78.169968,  radius: 300_000, fips: "51" },
    "WA" => StateInfo { lat: 47.400902, lon: -121.490494, radius: 500_000, fips: "53" },
    "WV" => StateInfo { lat: 38.491226, lon: -80.954453,  radius: 200_000, fips: "54" },
    "WI" => StateInfo { lat: 44.268543, lon: -89.616508,  radius: 400_000, fips: "55" },
    "WY" => StateInfo { lat: 42.755966, lon: -107.302490, radius: 500_000, fips: "56" },
};

/// States lookup table 
pub const ALL_STATES_ABBR: [&str; 50] = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
];

pub static ABBR_TO_FULL_NAME: phf::Map<&'static str, &'static str> = phf_map! {
    "AL" => "Alabama",
    "AK" => "Alaska",
    "AZ" => "Arizona",
    "AR" => "Arkansas",
    "CA" => "California",
    "CO" => "Colorado",
    "CT" => "Connecticut",
    "DE" => "Delaware",
    "FL" => "Florida",
    "GA" => "Georgia",
    "HI" => "Hawaii",
    "ID" => "Idaho",
    "IL" => "Illinois",
    "IN" => "Indiana",
    "IA" => "Iowa",
    "KS" => "Kansas",
    "KY" => "Kentucky",
    "LA" => "Louisiana",
    "ME" => "Maine",
    "MD" => "Maryland",
    "MA" => "Massachusetts",
    "MI" => "Michigan",
    "MN" => "Minnesota",
    "MS" => "Mississippi",
    "MO" => "Missouri",
    "MT" => "Montana",
    "NE" => "Nebraska",
    "NV" => "Nevada",
    "NH" => "New Hampshire",
    "NJ" => "New Jersey",
    "NM" => "New Mexico",
    "NY" => "New York",
    "NC" => "North Carolina",
    "ND" => "North Dakota",
    "OH" => "Ohio",
    "OK" => "Oklahoma",
    "OR" => "Oregon",
    "PA" => "Pennsylvania",
    "RI" => "Rhode Island",
    "SC" => "South Carolina",
    "SD" => "South Dakota",
    "TN" => "Tennessee",
    "TX" => "Texas",
    "UT" => "Utah",
    "VT" => "Vermont",
    "VA" => "Virginia",
    "WA" => "Washington",
    "WV" => "West Virginia",
    "WI" => "Wisconsin",
    "WY" => "Wyoming",
};

}

/// Generic Parent Client Class 
pub mod client {

use anyhow::{Context, Result};
use dotenvy::dotenv;
use std::path::Path;
use serde::Serialize;
use serde::de::DeserializeOwned;
use reqwest::{Client, RequestBuilder};
use tokio::time::{sleep, Duration};
use rusqlite::{Connection, params};


pub struct ClientBackbone {
    pub http: reqwest::Client, 
    pub conn: rusqlite::Connection,
    retries: u8
}

impl ClientBackbone {
    // Create new client for an sqlite database at db_path
    pub fn new(db_path: &str) -> Result<Self> {
        dotenv().ok();

        let conn = Connection::open(Path::new(db_path))
            .with_context(|| format!("Failed to open database at {}", db_path))?;

        // Generic table creation 
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS api_cache (
                endpoint TEXT PRIMARY KEY,
                payload BLOB,
                fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );"
        )?;

        let http = Client::builder()
            .user_agent("client-backbone/0.1")
            .build()?;

        // Return Client if exists at path 
        Ok(ClientBackbone { http, conn, retries: 3 })
    }

    // Async to take dynamic request generator to yield json response from api 
    pub async fn execute_with_retry<T, F>(&self, mut request: F) -> Result<T>
    where 
        T: DeserializeOwned + 'static,
        F: FnMut() -> RequestBuilder,
    {
        let mut attempt = 0;
        loop {
            // Get data 
            let response = request()
                .send()
                .await
                .context("HTTP request failed")?; 

            // Retry for server error 
            if response.status().is_server_error() && attempt < self.retries {
                attempt += 1; 
                let wait = Duration::from_secs(2u64.pow(attempt as u32));
                sleep(wait).await;
                continue; 
            }

            let response = response.error_for_status()
                .context("HTTP error status returned")?;

            let data = response.json::<T>()
                .await
                .context("Failed to deserialize JSON response")?;

            return Ok(data);
        }
    }

    // Commit table to database 
    pub fn save<T>(&mut self, table: &str, records: &[T]) -> Result<()>
    where 
        T: Serialize, 
    {
        let transaction = self.conn.transaction()?;
        for (i, record) in records.iter().enumerate() {
            let key  = format!("{}::{}", table, i);
            let blob = serde_json::to_vec(record)
                .context("Failed to serialize record")?;

            transaction.execute(
                &format!("REPLACE INTO {} (endpoint, payload) VALUES (?1, ?2)", table),
                params![key, blob],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn load_cached<T>(&self, table: &str, key: &str) -> Result<Option<T>>
    where 
        T: DeserializeOwned,
    {
        let mut statement = self.conn.prepare(
            &format!("SELECT payload FROM {} WHERE endpoint = ?1", table)
        )?;

        let cache_key = format!("{}::{}", table, key);
        let mut rows = statement.query(params![cache_key])?;
        if let Some(row) = rows.next()? {
            let blob: Vec<u8> = row.get(0)?;
            let obj = serde_json::from_slice(&blob)
                .context("Failed to deserialize cached payload")?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[tokio::test]
    async fn test_client_backbone() {
        let mut client = ClientBackbone::new(":memory:")
            .expect("Backbone instantiation failed");

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Dummy { x: u32 }

        let records = vec![Dummy { x: 42 }];
        client.save("api_cache", &records).expect("Failed to save");
        let cached: Option<Dummy> = client
            .load_cached("api_cache", "0")
            .expect("Failed to load");

        assert_eq!(cached, Some(Dummy { x: 42 }));
    }
}

}
