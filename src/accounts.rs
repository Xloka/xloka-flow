use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::sync::mpsc;
use tracing::warn;

use crate::Settings;
use crate::store::StoreWriter;
use crate::cdc::live::{LiveCdc, LiveCdcConfig};
use crate::cdc::CdcSource;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub name: String,
    pub mysql_url: String,
    pub snapshot_mysql_url: Option<String>,
    pub api_key: String,
    pub status: String,
}

pub struct AccountState {
    pub account: Account,
    pub db_path: String,
    pub writer_conn: Arc<Mutex<duckdb::Connection>>,
    pub reader_conn: Arc<Mutex<duckdb::Connection>>,
    pub cdc_task: Option<JoinHandle<()>>,
    pub writer_task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct AccountManager {
    pub states: Arc<RwLock<HashMap<String, AccountState>>>,
    pub file_path: String,
    pub settings: Arc<Settings>,
}

impl AccountManager {
    pub fn new(file_path: String, settings: Arc<Settings>) -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
            file_path,
            settings,
        }
    }

    pub fn load_persisted(&self) -> Vec<Account> {
        let contents = std::fs::read_to_string(&self.file_path).unwrap_or_else(|_| "[]".to_string());
        serde_json::from_str(&contents).unwrap_or_default()
    }

    pub fn save_persisted(&self, accounts: &[Account]) -> Result<(), anyhow::Error> {
        let json = serde_json::to_string_pretty(accounts)?;
        std::fs::write(&self.file_path, json)?;
        Ok(())
    }

    pub fn get_accounts(&self) -> Vec<Account> {
        let map = self.states.read().unwrap();
        map.values().map(|s| s.account.clone()).collect()
    }

    pub fn add_account_state(&self, state: AccountState) {
        let mut map = self.states.write().unwrap();
        map.insert(state.account.id.clone(), state);
    }
    
    pub fn get_account_conn_by_api_key(&self, api_key: &str) -> Option<Arc<Mutex<duckdb::Connection>>> {
        let map = self.states.read().unwrap();
        for state in map.values() {
            if state.account.api_key == api_key {
                return Some(Arc::clone(&state.reader_conn));
            }
        }
        None
    }

    pub fn get_account_db_path_by_api_key(&self, api_key: &str) -> Option<String> {
        let map = self.states.read().unwrap();
        for state in map.values() {
            if state.account.api_key == api_key {
                return Some(state.db_path.clone());
            }
        }
        None
    }

    pub fn get_account_conn_by_id(&self, id: &str) -> Option<Arc<Mutex<duckdb::Connection>>> {
        let map = self.states.read().unwrap();
        map.get(id).map(|s| Arc::clone(&s.writer_conn))
    }

    pub fn get_account_by_id(&self, id: &str) -> Option<Account> {
        let map = self.states.read().unwrap();
        map.get(id).map(|s| s.account.clone())
    }
    
    pub fn spawn_and_add_account(&self, account: Account) -> Result<(), anyhow::Error> {
        let db_path = format!("data/{}.db", account.id);
        let checkpoint_path = format!("data/{}_checkpoint.json", account.id);
        
        let conn = duckdb::Connection::open(&db_path)?;
        
        // Execute PRAGMAs to secure and optimize the database limit
        conn.execute_batch("PRAGMA memory_limit='2GB';")?;
        
        let reader_conn = conn.try_clone()?;
        
        let shared_writer_conn = Arc::new(Mutex::new(conn));
        let shared_reader_conn = Arc::new(Mutex::new(reader_conn));
        let (tx, rx) = mpsc::channel(1_000);
        
        // Spawn writer
        let writer_settings = Arc::clone(&self.settings);
        let writer_conn = Arc::clone(&shared_writer_conn);
        let writer_task = tokio::spawn(async move {
            let writer = StoreWriter::new(writer_settings, writer_conn);
            let _ = writer.run(rx).await;
        });
        
        // Spawn CDC
        let cdc = LiveCdc::new(LiveCdcConfig {
            mysql_url: account.mysql_url.clone(),
            snapshot_mysql_url: account.snapshot_mysql_url.clone(),
            checkpoint_path,
            filter_databases: vec![],
            filter_tables: vec![],
            server_id: 42 + rand::random::<u32>() % 1000,
        });
        
        let cdc_task = tokio::spawn(async move { 
            if let Err(e) = cdc.run(tx).await {
                warn!("Account CDC task failed: {:?}", e);
            }
        });
        
        let state = AccountState {
            account,
            db_path: db_path.clone(),
            writer_conn: shared_writer_conn,
            reader_conn: shared_reader_conn,
            cdc_task: Some(cdc_task),
            writer_task: Some(writer_task),
        };
        
        self.add_account_state(state);
        Ok(())
    }
    
    pub fn remove_account(&self, id: &str) {
        let mut map = self.states.write().unwrap();
        if let Some(state) = map.remove(id) {
            if let Some(cdc) = state.cdc_task { cdc.abort(); }
            if let Some(writer) = state.writer_task { writer.abort(); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::Settings;

    #[test]
    fn test_account_persistence() {
        // Use a temp file unique to this test to prevent parallel test collision
        let temp_file = format!("test_accounts_{}.json", uuid::Uuid::new_v4());
        let settings = Arc::new(Settings::default());
        let manager = AccountManager::new(temp_file.clone(), settings);

        // Should load empty if file doesn't exist
        let initial = manager.load_persisted();
        assert!(initial.is_empty());

        let test_account = Account {
            id: "123".into(),
            name: "Test Corp".into(),
            mysql_url: "mysql://localhost".into(),
            snapshot_mysql_url: None,
            api_key: "xk_test".into(),
            status: "active".into(),
        };

        // Save account
        manager.save_persisted(std::slice::from_ref(&test_account)).unwrap();

        // Load account
        let loaded = manager.load_persisted();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "123");
        assert_eq!(loaded[0].name, "Test Corp");

        // Clean up temp file
        let _ = std::fs::remove_file(temp_file);
    }
}
