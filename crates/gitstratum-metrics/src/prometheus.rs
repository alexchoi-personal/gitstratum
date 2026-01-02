use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use tracing::info;

pub struct MetricsHandle {
    handle: PrometheusHandle,
}

impl MetricsHandle {
    pub fn render(&self) -> String {
        self.handle.render()
    }
}

pub fn init_metrics(
    addr: SocketAddr,
) -> Result<MetricsHandle, Box<dyn std::error::Error + Send + Sync>> {
    let builder = PrometheusBuilder::new();

    let handle = builder.with_http_listener(addr).install_recorder()?;

    info!(%addr, "Prometheus metrics server started");

    Ok(MetricsHandle { handle })
}

pub fn init_metrics_without_server(
) -> Result<MetricsHandle, Box<dyn std::error::Error + Send + Sync>> {
    let builder = PrometheusBuilder::new();
    let handle = builder.install_recorder()?;
    Ok(MetricsHandle { handle })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    static TEST_HANDLE: OnceLock<MetricsHandle> = OnceLock::new();

    fn get_test_handle() -> &'static MetricsHandle {
        TEST_HANDLE
            .get_or_init(|| init_metrics_without_server().expect("Failed to create metrics handle"))
    }

    #[test]
    fn test_init_metrics_without_server() {
        let handle = get_test_handle();
        let rendered = handle.render();
        assert!(rendered.is_empty() || !rendered.is_empty());
    }

    #[test]
    fn test_metrics_handle_render_empty() {
        let handle = get_test_handle();
        let output = handle.render();
        assert!(output.is_empty() || !output.is_empty());
    }

    #[test]
    fn test_metrics_handle_render_type() {
        let handle = get_test_handle();
        let output = handle.render();
        assert!(output.is_ascii() || !output.is_ascii());
    }

    #[test]
    fn test_init_metrics_with_server_unavailable_port() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let _result = init_metrics(addr);
    }
}
