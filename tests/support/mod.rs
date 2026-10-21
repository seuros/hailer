//! Real isolated databases, never a Rails application's database or schema.

use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, PgConnection};

static NEXT: AtomicU64 = AtomicU64::new(1);

pub struct Database {
    options: PgConnectOptions,
    name: String,
    url: String,
}

impl Database {
    pub fn url(&self) -> &str {
        &self.url
    }
}

pub async fn database() -> Database {
    let url = std::env::var("HAILER_TEST_DATABASE_URL")
        .expect("set HAILER_TEST_DATABASE_URL to an isolated test PostgreSQL server");
    let options: PgConnectOptions = url.parse().expect("test database URL");
    let name =
        format!("hailer_test_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let mut connection = PgConnection::connect_with(&options).await.expect("test PostgreSQL");
    // Identifiers cannot be bound. This name contains only a fixed prefix
    // and decimal process/counter values, never caller-controlled SQL.
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name} TEMPLATE template0")))
        .execute(&mut connection)
        .await
        .expect("isolated test database");
    let url = options.clone().database(&name).to_url_lossy().to_string();
    Database { options, name, url }
}

impl Drop for Database {
    fn drop(&mut self) {
        let options = self.options.clone();
        let name = self.name.clone();
        // The test's runtime may be shutting down; a dedicated thread makes
        // cleanup complete before the fixture is released.
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            runtime.block_on(async {
                if let Ok(mut connection) = PgConnection::connect_with(&options).await {
                    // The same internally generated identifier as above.
                    sqlx::query(sqlx::AssertSqlSafe(format!("DROP DATABASE {name} WITH (FORCE)")))
                        .execute(&mut connection)
                        .await
                        .expect("remove test database");
                }
            });
        })
        .join()
        .expect("database cleanup");
    }
}
