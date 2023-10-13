use std::env;

use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

fn get_env() -> &'static str {
    let env_str = std::env::var("ENV");
    match env_str {
        Err(env::VarError::NotPresent) => {
            println!("no ENV in env, assuming dev");
            "dev"
        }
        Ok(str) => match str.as_ref() {
            "dev" => "dev",
            "development" => "dev",
            "stag" => "stag",
            "staging" => "stag",
            "prod" => "prod",
            "production" => "prod",
            _ => {
                panic!("ENV present: {str}, but not one of dev, stag, prod, panicking!")
            }
        },
        err => {
            panic!("error getting ENV from env: {:?}", err)
        }
    }
}

pub fn init() {
    // We want to load log before env, we can't use our env module here.
    let log_perf = std::env::var("LOG_PERF")
        .map(|s| s == "true" || s == "1")
        .unwrap_or(false);

    let span_format = if log_perf {
        FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    if get_env() == "dev" {
        tracing_subscriber::fmt()
            .with_span_events(span_format)
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_span_events(span_format)
            .with_env_filter(EnvFilter::from_default_env())
            .json()
            .init();
    }
}
